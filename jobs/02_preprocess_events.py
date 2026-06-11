"""Preprocess raw Synerise event and product tables for later feature work.

The job writes cleaned intermediate Parquet tables under data/processed and
sanitized aggregate validation artifacts under artifacts/preprocessing.

It does not create final labels, feature tables, model inputs, predictions, or
API outputs. Public artifacts contain aggregate validation only.
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_BASE_DIR = PROJECT_ROOT / "data" / "raw" / "synerise_dataset"
DEFAULT_OUTPUT_BASE = PROJECT_ROOT / "data" / "processed"
ARTIFACT_DIR = PROJECT_ROOT / "artifacts" / "preprocessing"

SUMMARY_PATH = ARTIFACT_DIR / "preprocessing_summary.json"
TABLE_VALIDATION_PATH = ARTIFACT_DIR / "table_validation.csv"
DUPLICATE_SUMMARY_PATH = ARTIFACT_DIR / "duplicate_check_summary.csv"
PRODUCT_VALIDATION_PATH = ARTIFACT_DIR / "product_metadata_validation.csv"
NOTES_PATH = ARTIFACT_DIR / "preprocessing_notes.md"

EVENT_TABLES: dict[str, dict[str, Any]] = {
    "add_to_cart": {
        "file_name": "add_to_cart.parquet",
        "event_type": "add_to_cart",
        "required_columns": ["client_id", "timestamp", "sku"],
        "entity_columns": ["sku"],
        "duplicate_key": ["client_id", "event_ts", "sku"],
        "default_enabled": True,
    },
    "remove_from_cart": {
        "file_name": "remove_from_cart.parquet",
        "event_type": "remove_from_cart",
        "required_columns": ["client_id", "timestamp", "sku"],
        "entity_columns": ["sku"],
        "duplicate_key": ["client_id", "event_ts", "sku"],
        "default_enabled": True,
    },
    "product_buy": {
        "file_name": "product_buy.parquet",
        "event_type": "product_buy",
        "required_columns": ["client_id", "timestamp", "sku"],
        "entity_columns": ["sku"],
        "duplicate_key": ["client_id", "event_ts", "sku"],
        "default_enabled": True,
    },
    "search_query": {
        "file_name": "search_query.parquet",
        "event_type": "search_query",
        "required_columns": ["client_id", "timestamp"],
        "entity_columns": [],
        "duplicate_key": ["client_id", "event_ts"],
        "default_enabled": False,
    },
    "page_visit": {
        "file_name": "page_visit.parquet",
        "event_type": "page_visit",
        "required_columns": ["client_id", "timestamp", "url"],
        "entity_columns": ["url"],
        "duplicate_key": ["client_id", "event_ts", "url"],
        "default_enabled": False,
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Preprocess raw event and product tables into clean intermediate Parquet outputs."
    )
    parser.add_argument(
        "--include-search",
        action="store_true",
        help="Also preprocess search_query without persisting raw query text in artifacts or processed output.",
    )
    parser.add_argument(
        "--include-page-visit",
        action="store_true",
        help="Also preprocess page_visit. This table is large and is deferred by default.",
    )
    parser.add_argument(
        "--sample-fraction",
        type=float,
        default=None,
        help="Optional local-development sample fraction in (0, 1]. Do not use for final preprocessing.",
    )
    parser.add_argument(
        "--output-base",
        default=DEFAULT_OUTPUT_BASE.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative processed output base. Default: data/processed.",
    )
    return parser.parse_args()


def relative_path(path: Path) -> str:
    return path.relative_to(PROJECT_ROOT).as_posix()


def normalize_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return round(value, 6)
    return str(value)


def short_error_summary(context: str, exc: Exception) -> str:
    return f"{context} ({exc.__class__.__name__})"


def start_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("preprocess-events")
        .master("local[*]")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def resolve_repo_path(path_text: str) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        raise ValueError("output-base must be repo-relative")
    return PROJECT_ROOT / path


def clean_output_dir(path: Path) -> None:
    resolved = path.resolve()
    processed_root = (PROJECT_ROOT / "data" / "processed").resolve()
    if not str(resolved).startswith(str(processed_root)):
        raise ValueError("Refusing to clean an output directory outside data/processed")
    if path.exists():
        shutil.rmtree(path)


def apply_sample(df: DataFrame, sample_fraction: float | None) -> DataFrame:
    if sample_fraction is None:
        return df
    if sample_fraction <= 0 or sample_fraction > 1:
        raise ValueError("sample-fraction must be in (0, 1]")
    return df.sample(withReplacement=False, fraction=sample_fraction, seed=42)


def collect_single_row(df: DataFrame) -> dict[str, Any]:
    return {key: normalize_value(value) for key, value in df.collect()[0].asDict().items()}


def count_duplicate_keys(df: DataFrame, key_columns: list[str]) -> dict[str, int]:
    key_counts = df.groupBy(*[F.col(column) for column in key_columns]).count().where(F.col("count") > 1)
    row = key_counts.agg(
        F.count(F.lit(1)).alias("duplicate_key_groups"),
        F.coalesce(F.sum(F.col("count") - F.lit(1)), F.lit(0)).alias("duplicate_extra_rows"),
    ).collect()[0]
    return {
        "duplicate_key_groups": int(row["duplicate_key_groups"] or 0),
        "duplicate_extra_rows": int(row["duplicate_extra_rows"] or 0),
    }


def preprocess_event_table(
    spark: SparkSession,
    table_name: str,
    spec: dict[str, Any],
    output_base: Path,
    sample_fraction: float | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    raw_path = RAW_BASE_DIR / spec["file_name"]
    output_path = output_base / "events" / table_name
    table_summary: dict[str, Any] = {
        "table_name": table_name,
        "input_path": relative_path(raw_path),
        "output_path": relative_path(output_path),
        "processed": False,
        "validation_status": "not_started",
        "write_status": "not_started",
        "input_rows": None,
        "valid_rows": None,
        "invalid_rows": None,
        "event_ts_null_count": None,
        "event_ts_parse_failure_count": None,
        "event_date_min": None,
        "event_date_max": None,
        "sample_fraction": sample_fraction,
        "status": "not_started",
        "error_summary": None,
    }
    duplicate_summary: dict[str, Any] = {
        "table_name": table_name,
        "duplicate_key": ",".join(spec["duplicate_key"]),
        "valid_rows": None,
        "duplicate_key_groups": None,
        "duplicate_extra_rows": None,
        "duplicate_extra_row_rate": None,
        "status": "not_started",
        "notes": None,
    }

    try:
        df = apply_sample(spark.read.parquet(str(raw_path)), sample_fraction)
        columns = set(df.columns)
        missing_columns = [column for column in spec["required_columns"] if column not in columns]
        if missing_columns:
            table_summary["status"] = "failed"
            table_summary["validation_status"] = "failed"
            table_summary["write_status"] = "skipped"
            table_summary["error_summary"] = f"Missing required columns: {', '.join(missing_columns)}"
            duplicate_summary["status"] = "skipped"
            duplicate_summary["notes"] = "Skipped because required columns were missing."
            return table_summary, duplicate_summary

        parsed = df.withColumn("event_ts", F.to_timestamp(F.col("timestamp"))).withColumn(
            "event_date", F.to_date(F.col("event_ts"))
        )
        required_valid_conditions = [
            F.col("client_id").isNotNull(),
            F.col("event_ts").isNotNull(),
        ]
        for column in spec["entity_columns"]:
            required_valid_conditions.append(F.col(column).isNotNull())

        valid_condition = required_valid_conditions[0]
        for condition in required_valid_conditions[1:]:
            valid_condition = valid_condition & condition

        validation_exprs = [
            F.count(F.lit(1)).alias("input_rows"),
            F.sum(F.when(valid_condition, 1).otherwise(0)).alias("valid_rows"),
            F.sum(F.when(~valid_condition, 1).otherwise(0)).alias("invalid_rows"),
            F.sum(F.when(F.col("event_ts").isNull(), 1).otherwise(0)).alias("event_ts_null_count"),
            F.sum(
                F.when(F.col("timestamp").isNotNull() & F.col("event_ts").isNull(), 1).otherwise(0)
            ).alias("event_ts_parse_failure_count"),
            F.min(F.col("event_date")).cast("string").alias("event_date_min"),
            F.max(F.col("event_date")).cast("string").alias("event_date_max"),
        ]
        for column in ["client_id", *spec["entity_columns"]]:
            validation_exprs.append(F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias(f"{column}_null_count"))

        metrics = collect_single_row(parsed.agg(*validation_exprs))

        select_columns = [
            F.col("client_id").cast("long").alias("client_id"),
            F.col("event_ts"),
            F.col("event_date"),
            F.lit(spec["event_type"]).alias("event_type"),
        ]
        if "sku" in spec["entity_columns"]:
            select_columns.append(F.col("sku").cast("long").alias("sku"))
        if "url" in spec["entity_columns"]:
            select_columns.append(F.col("url").cast("long").alias("url"))

        cleaned = parsed.where(valid_condition).select(*select_columns)

        duplicate_metrics = count_duplicate_keys(cleaned, spec["duplicate_key"])
        valid_rows = int(metrics.get("valid_rows") or 0)
        duplicate_rate = (
            round(duplicate_metrics["duplicate_extra_rows"] / valid_rows, 6) if valid_rows else None
        )

        table_summary.update(metrics)
        table_summary["validation_status"] = "success"

        duplicate_summary.update(
            {
                "valid_rows": valid_rows,
                **duplicate_metrics,
                "duplicate_extra_row_rate": duplicate_rate,
                "status": "success",
                "notes": "Aggregate duplicate check only; no duplicate rows are persisted in artifacts.",
            }
        )

        try:
            clean_output_dir(output_path)
            cleaned.write.mode("overwrite").parquet(str(output_path))
            table_summary["processed"] = True
            table_summary["write_status"] = "success"
            table_summary["status"] = "success"
        except Exception as exc:  # noqa: BLE001 - persisted message is intentionally short.
            table_summary["processed"] = False
            table_summary["write_status"] = "failed"
            table_summary["status"] = "failed"
            table_summary["error_summary"] = short_error_summary("processed output write failed", exc)
        return table_summary, duplicate_summary
    except Exception as exc:  # noqa: BLE001 - persisted message is intentionally short.
        table_summary["status"] = "failed"
        table_summary["validation_status"] = "failed"
        table_summary["write_status"] = "skipped"
        table_summary["error_summary"] = short_error_summary("event preprocessing failed", exc)
        duplicate_summary["status"] = "skipped"
        duplicate_summary["notes"] = "Skipped because preprocessing did not complete."
        return table_summary, duplicate_summary


def preprocess_product_metadata(
    spark: SparkSession,
    output_base: Path,
    sample_fraction: float | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    raw_path = RAW_BASE_DIR / "product_properties.parquet"
    output_path = output_base / "product_properties_clean"
    validation: dict[str, Any] = {
        "table_name": "product_properties",
        "input_path": relative_path(raw_path),
        "output_path": relative_path(output_path),
        "processed": False,
        "validation_status": "not_started",
        "write_status": "not_started",
        "input_rows": None,
        "output_rows": None,
        "distinct_sku_count": None,
        "duplicated_sku_count": None,
        "duplicate_extra_rows": None,
        "null_sku_count": None,
        "null_category_count": None,
        "null_price_count": None,
        "null_name_count": None,
        "dedup_rule": "group by sku and keep the most frequent category/price pair; ties sort by category and price",
        "name_persisted": False,
        "sample_fraction": sample_fraction,
        "status": "not_started",
        "error_summary": None,
    }
    table_summary: dict[str, Any] = {
        "table_name": "product_properties",
        "input_path": relative_path(raw_path),
        "output_path": relative_path(output_path),
        "processed": False,
        "validation_status": "not_started",
        "write_status": "not_started",
        "input_rows": None,
        "valid_rows": None,
        "invalid_rows": None,
        "event_ts_null_count": None,
        "event_ts_parse_failure_count": None,
        "event_date_min": None,
        "event_date_max": None,
        "sample_fraction": sample_fraction,
        "status": "not_started",
        "error_summary": None,
    }
    try:
        df = apply_sample(spark.read.parquet(str(raw_path)), sample_fraction)
        required = ["sku", "category", "price", "name"]
        missing_columns = [column for column in required if column not in df.columns]
        if missing_columns:
            validation["status"] = "failed"
            validation["validation_status"] = "failed"
            validation["write_status"] = "skipped"
            validation["error_summary"] = f"Missing required columns: {', '.join(missing_columns)}"
            table_summary["status"] = "failed"
            table_summary["validation_status"] = "failed"
            table_summary["write_status"] = "skipped"
            table_summary["error_summary"] = validation["error_summary"]
            return table_summary, validation

        metrics = collect_single_row(
            df.agg(
                F.count(F.lit(1)).alias("input_rows"),
                F.countDistinct("sku").alias("distinct_sku_count"),
                F.sum(F.when(F.col("sku").isNull(), 1).otherwise(0)).alias("null_sku_count"),
                F.sum(F.when(F.col("category").isNull(), 1).otherwise(0)).alias("null_category_count"),
                F.sum(F.when(F.col("price").isNull(), 1).otherwise(0)).alias("null_price_count"),
                F.sum(F.when(F.col("name").isNull(), 1).otherwise(0)).alias("null_name_count"),
            )
        )

        sku_counts = df.where(F.col("sku").isNotNull()).groupBy("sku").count()
        duplicate_metrics = collect_single_row(
            sku_counts.agg(
                F.sum(F.when(F.col("count") > 1, 1).otherwise(0)).alias("duplicated_sku_count"),
                F.coalesce(F.sum(F.when(F.col("count") > 1, F.col("count") - F.lit(1)).otherwise(0)), F.lit(0)).alias(
                    "duplicate_extra_rows"
                ),
            )
        )

        pair_counts = (
            df.where(F.col("sku").isNotNull())
            .select(
                F.col("sku").cast("long").alias("sku"),
                F.col("category").cast("long").alias("category"),
                F.col("price").cast("long").alias("price"),
            )
            .groupBy("sku", "category", "price")
            .count()
        )
        choice_window = Window.partitionBy("sku").orderBy(
            F.col("count").desc(),
            F.col("category").asc_nulls_last(),
            F.col("price").asc_nulls_last(),
        )
        clean_products = (
            pair_counts.withColumn("rank", F.row_number().over(choice_window))
            .where(F.col("rank") == 1)
            .select("sku", "category", "price")
        )

        output_rows = clean_products.count()

        validation.update(metrics)
        validation.update(duplicate_metrics)
        validation["output_rows"] = output_rows
        validation["validation_status"] = "success"

        table_summary.update(
            {
                "input_rows": validation["input_rows"],
                "valid_rows": output_rows,
                "invalid_rows": (validation["input_rows"] or 0) - output_rows,
                "validation_status": "success",
            }
        )

        try:
            clean_output_dir(output_path)
            clean_products.write.mode("overwrite").parquet(str(output_path))
            validation["processed"] = True
            validation["write_status"] = "success"
            validation["status"] = "success"
            table_summary["processed"] = True
            table_summary["write_status"] = "success"
            table_summary["status"] = "success"
        except Exception as exc:  # noqa: BLE001 - persisted message is intentionally short.
            validation["processed"] = False
            validation["write_status"] = "failed"
            validation["status"] = "failed"
            validation["error_summary"] = short_error_summary("processed output write failed", exc)
            table_summary["processed"] = False
            table_summary["write_status"] = "failed"
            table_summary["status"] = "failed"
            table_summary["error_summary"] = validation["error_summary"]
        return table_summary, validation
    except Exception as exc:  # noqa: BLE001 - persisted message is intentionally short.
        validation["status"] = "failed"
        validation["validation_status"] = "failed"
        validation["write_status"] = "skipped"
        validation["error_summary"] = short_error_summary("product metadata preprocessing failed", exc)
        table_summary["status"] = "failed"
        table_summary["validation_status"] = "failed"
        table_summary["write_status"] = "skipped"
        table_summary["error_summary"] = validation["error_summary"]
        return table_summary, validation


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_notes(path: Path, summary: dict[str, Any]) -> None:
    processed = ", ".join(summary["processed_event_tables"])
    deferred = ", ".join(summary["deferred_optional_tables"]) or "none"
    status = summary["status"]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "# Preprocessing Notes",
                "",
                "This artifact contains aggregate-only preprocessing notes.",
                "",
                f"Preprocessing status: {status}.",
                f"Processed event tables: {processed}.",
                f"Deferred optional event tables: {deferred}.",
                "Processed Parquet outputs must be written by Spark before Phase 2 is considered complete.",
                "Product metadata output excludes product names and keeps only sku, category, and price.",
                "No final labels, feature tables, model inputs, batch predictions, or API outputs were created.",
                "",
            ]
        ),
        encoding="utf-8",
    )


def enabled_event_tables(args: argparse.Namespace) -> list[str]:
    enabled = [name for name, spec in EVENT_TABLES.items() if spec["default_enabled"]]
    if args.include_search:
        enabled.append("search_query")
    if args.include_page_visit:
        enabled.append("page_visit")
    return enabled


def main() -> int:
    args = parse_args()
    output_base = resolve_repo_path(args.output_base)
    selected_tables = enabled_event_tables(args)
    deferred_tables = [
        name
        for name, spec in EVENT_TABLES.items()
        if not spec["default_enabled"] and name not in selected_tables
    ]

    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    output_base.mkdir(parents=True, exist_ok=True)

    spark = start_spark()
    try:
        table_rows: list[dict[str, Any]] = []
        duplicate_rows: list[dict[str, Any]] = []
        for table_name in selected_tables:
            table_summary, duplicate_summary = preprocess_event_table(
                spark=spark,
                table_name=table_name,
                spec=EVENT_TABLES[table_name],
                output_base=output_base,
                sample_fraction=args.sample_fraction,
            )
            table_rows.append(table_summary)
            duplicate_rows.append(duplicate_summary)

        product_table_summary, product_validation = preprocess_product_metadata(
            spark=spark,
            output_base=output_base,
            sample_fraction=args.sample_fraction,
        )
        table_rows.append(product_table_summary)
        all_writes_succeeded = all(row.get("write_status") == "success" for row in table_rows)

        summary = {
            "generated_at_date": datetime.now(timezone.utc).date().isoformat(),
            "phase": "Phase 2: Preprocessing",
            "status": "success" if all_writes_succeeded else "failed",
            "sample_fraction": args.sample_fraction,
            "sample_fraction_note": (
                "Local development sample only; not final preprocessing." if args.sample_fraction is not None else None
            ),
            "processed_event_tables": selected_tables,
            "deferred_optional_tables": deferred_tables,
            "processed_output_base": relative_path(output_base),
            "artifact_paths": {
                "summary": relative_path(SUMMARY_PATH),
                "table_validation": relative_path(TABLE_VALIDATION_PATH),
                "duplicate_check_summary": relative_path(DUPLICATE_SUMMARY_PATH),
                "product_metadata_validation": relative_path(PRODUCT_VALIDATION_PATH),
                "notes": relative_path(NOTES_PATH),
            },
            "target_setup": {
                "task": "purchase_propensity",
                "target_window_days": 30,
                "eligible_cohort": "add_to_cart_or_purchase_in_history",
                "positive_definition": "purchase_in_target_window",
                "label_tables_created": False,
                "leakage_rule": "features_before_cutoff_labels_in_target_window",
            },
            "privacy": {
                "artifact_level": "aggregate_only",
                "row_samples_persisted": False,
                "raw_client_ids_persisted": False,
                "raw_query_text_persisted": False,
                "product_names_persisted": False,
            },
            "runtime_notes": [
                "Processed Parquet outputs are written with Spark DataFrameWriter.",
                "If Spark cannot write Parquet in the local runtime, validation artifacts may still be written but Phase 2 is not complete.",
            ],
            "event_table_validation": table_rows,
            "duplicate_check_summary": duplicate_rows,
            "product_metadata_validation": product_validation,
        }

        write_json(SUMMARY_PATH, summary)
        write_csv(
            TABLE_VALIDATION_PATH,
            table_rows,
            [
                "table_name",
                "input_path",
                "output_path",
                "processed",
                "validation_status",
                "write_status",
                "input_rows",
                "valid_rows",
                "invalid_rows",
                "event_ts_null_count",
                "event_ts_parse_failure_count",
                "event_date_min",
                "event_date_max",
                "client_id_null_count",
                "sku_null_count",
                "url_null_count",
                "sample_fraction",
                "status",
                "error_summary",
            ],
        )
        write_csv(
            DUPLICATE_SUMMARY_PATH,
            duplicate_rows,
            [
                "table_name",
                "duplicate_key",
                "valid_rows",
                "duplicate_key_groups",
                "duplicate_extra_rows",
                "duplicate_extra_row_rate",
                "status",
                "notes",
            ],
        )
        write_csv(
            PRODUCT_VALIDATION_PATH,
            [product_validation],
            [
                "table_name",
                "input_path",
                "output_path",
                "processed",
                "validation_status",
                "write_status",
                "input_rows",
                "output_rows",
                "distinct_sku_count",
                "duplicated_sku_count",
                "duplicate_extra_rows",
                "null_sku_count",
                "null_category_count",
                "null_price_count",
                "null_name_count",
                "dedup_rule",
                "name_persisted",
                "sample_fraction",
                "status",
                "error_summary",
            ],
        )
        write_notes(NOTES_PATH, summary)

        if all_writes_succeeded:
            print("Preprocessing completed.")
        else:
            print("Preprocessing validation completed, but one or more Spark Parquet writes failed.")
        print(f"Processed event tables: {', '.join(selected_tables)}")
        if deferred_tables:
            print(f"Deferred optional tables: {', '.join(deferred_tables)}")
        print(f"Processed output base: {relative_path(output_base)}")
        print(f"Wrote sanitized artifacts under {relative_path(ARTIFACT_DIR)}")
        return 0 if all_writes_succeeded else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
