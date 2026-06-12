"""Build leakage-safe user-level behavior features.

This Phase 3 job reads processed Parquet tables from data/processed, builds
aggregate client-level features before the configured cutoff date, and writes:

- data/processed/features/user_behavior_features/
- artifacts/features/feature_summary.json
- artifacts/features/feature_catalog.csv
- artifacts/features/feature_validation.csv
- artifacts/features/feature_notes.md

It does not create final labels, train models, create prediction outputs, or
implement API serving.
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

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "pipeline_config.yaml"
DEFAULT_OUTPUT_BASE = PROJECT_ROOT / "data" / "processed"
FEATURE_OUTPUT_DIR = "features/user_behavior_features"
ARTIFACT_DIR = PROJECT_ROOT / "artifacts" / "features"
FEATURE_SUMMARY_PATH = ARTIFACT_DIR / "feature_summary.json"
FEATURE_CATALOG_PATH = ARTIFACT_DIR / "feature_catalog.csv"
FEATURE_VALIDATION_PATH = ARTIFACT_DIR / "feature_validation.csv"
FEATURE_NOTES_PATH = ARTIFACT_DIR / "feature_notes.md"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build user-level aggregate behavior features.")
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative pipeline config path. Default: configs/pipeline_config.yaml.",
    )
    parser.add_argument(
        "--output-base",
        default=DEFAULT_OUTPUT_BASE.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative processed output base. Default: data/processed.",
    )
    parser.add_argument(
        "--include-search",
        action="store_true",
        help="Include search_query features. If omitted, search is included automatically when the processed table exists.",
    )
    parser.add_argument(
        "--feature-window-days",
        type=int,
        action="append",
        default=None,
        help="Lookback window in days. Can be passed multiple times. Default: 30, 60, 90.",
    )
    return parser.parse_args()


def relative_path(path: Path) -> str:
    return path.relative_to(PROJECT_ROOT).as_posix()


def resolve_repo_path(path_text: str) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        raise ValueError("path arguments must be repo-relative")
    return PROJECT_ROOT / path


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


def read_simple_yaml(path: Path) -> dict[str, Any]:
    config: dict[str, Any] = {}
    section: str | None = None
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line:
            continue
        if not line.startswith(" ") and line.endswith(":"):
            section = line[:-1]
            config[section] = {}
            continue
        if section and line.startswith("  ") and ":" in line:
            key, value = line.strip().split(":", 1)
            value = value.strip()
            if value.startswith('"') and value.endswith('"'):
                value = value[1:-1]
            elif value.isdigit():
                value = int(value)
            config[section][key] = value
    return config


def start_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("build-user-features")
        .master("local[*]")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def clean_output_dir(path: Path) -> None:
    resolved = path.resolve()
    processed_root = (PROJECT_ROOT / "data" / "processed").resolve()
    if not str(resolved).startswith(str(processed_root)):
        raise ValueError("Refusing to clean an output directory outside data/processed")
    if path.exists():
        shutil.rmtree(path)


def read_event(spark: SparkSession, output_base: Path, table_name: str) -> DataFrame:
    return spark.read.parquet(str(output_base / "events" / table_name))


def filter_history(df: DataFrame, cutoff_date: str, window_days: int | None = None) -> DataFrame:
    condition = F.col("event_date") < F.to_date(F.lit(cutoff_date))
    if window_days is not None:
        condition = condition & (F.col("event_date") >= F.date_sub(F.to_date(F.lit(cutoff_date)), window_days))
    return df.where(condition)


def event_count_features(
    df: DataFrame,
    event_name: str,
    cutoff_date: str,
    windows: list[int],
    distinct_column: str | None = None,
) -> DataFrame:
    history = filter_history(df, cutoff_date).cache()
    aggregations = [
        F.count(F.lit(1)).alias(f"{event_name}_count"),
        F.max("event_date").alias(f"last_{event_name}_date"),
    ]
    if distinct_column:
        aggregations.append(F.countDistinct(distinct_column).alias(f"distinct_{event_name}_sku_count"))
    if event_name == "search_query":
        aggregations.append(F.countDistinct("event_date").alias("distinct_search_days"))
    base = history.groupBy("client_id").agg(*aggregations)
    base = base.withColumn(
        f"days_since_last_{event_name}",
        F.datediff(F.to_date(F.lit(cutoff_date)), F.col(f"last_{event_name}_date")),
    ).drop(f"last_{event_name}_date")

    for window_days in windows:
        window_counts = (
            filter_history(df, cutoff_date, window_days)
            .groupBy("client_id")
            .agg(F.count(F.lit(1)).alias(f"{event_name}_count_{window_days}d"))
        )
        base = base.join(window_counts, "client_id", "left")
    history.unpersist()
    return base


def active_days_feature(event_frames: list[DataFrame], cutoff_date: str) -> DataFrame:
    date_frames = [filter_history(df, cutoff_date).select("client_id", "event_date") for df in event_frames]
    combined = date_frames[0]
    for frame in date_frames[1:]:
        combined = combined.unionByName(frame)
    return combined.distinct().groupBy("client_id").agg(F.count(F.lit(1)).alias("active_days_count"))


def product_metadata_features(
    events: DataFrame,
    products: DataFrame,
    event_prefix: str,
    cutoff_date: str,
) -> DataFrame:
    joined = (
        filter_history(events, cutoff_date)
        .join(products, "sku", "left")
        .groupBy("client_id")
        .agg(
            F.countDistinct("category").alias(f"distinct_{event_prefix}_category_count"),
            F.avg("price").alias(f"avg_{event_prefix}_price"),
            F.max("price").alias(f"max_{event_prefix}_price"),
        )
    )
    return joined


def safe_ratio(numerator: F.Column, denominator: F.Column) -> F.Column:
    return F.when(denominator > 0, numerator / denominator).otherwise(F.lit(None).cast("double"))


def fill_count_columns(df: DataFrame) -> DataFrame:
    count_columns = [
        field.name
        for field in df.schema.fields
        if field.name.endswith("_count")
        or "_count_" in field.name
        or field.name.endswith("_days")
        or field.name.endswith("_days_count")
    ]
    fill_values = {column: 0 for column in count_columns if column != "client_id"}
    return df.fillna(fill_values)


def add_ratio_features(df: DataFrame, include_search: bool) -> DataFrame:
    result = (
        df.withColumn("buy_to_cart_ratio", safe_ratio(F.col("product_buy_count"), F.col("add_to_cart_count")))
        .withColumn("remove_to_cart_ratio", safe_ratio(F.col("remove_from_cart_count"), F.col("add_to_cart_count")))
        .withColumn("cart_minus_remove_count", F.col("add_to_cart_count") - F.col("remove_from_cart_count"))
    )
    if include_search and "search_query_count" in result.columns:
        result = result.withColumn(
            "search_to_cart_ratio", safe_ratio(F.col("search_query_count"), F.col("add_to_cart_count"))
        )
    return result


def build_feature_catalog(feature_df: DataFrame) -> list[dict[str, Any]]:
    descriptions = {
        "is_eligible_purchase_propensity": "Cohort indicator: client had add_to_cart or product_buy activity before cutoff.",
        "active_days_count": "Number of distinct active event dates before cutoff.",
        "buy_to_cart_ratio": "product_buy_count divided by add_to_cart_count when cart count is positive.",
        "remove_to_cart_ratio": "remove_from_cart_count divided by add_to_cart_count when cart count is positive.",
        "cart_minus_remove_count": "add_to_cart_count minus remove_from_cart_count.",
        "search_to_cart_ratio": "search_query_count divided by add_to_cart_count when cart count is positive.",
    }
    rows = []
    for field in feature_df.schema.fields:
        name = field.name
        if name == "client_id":
            continue
        if "days_since" in name:
            group = "recency"
        elif "ratio" in name or name == "cart_minus_remove_count":
            group = "ratio"
        elif "price" in name or "category" in name:
            group = "product_metadata"
        elif name.endswith("_count") or "_count_" in name or name == "active_days_count":
            group = "activity_count"
        elif name.startswith("is_eligible"):
            group = "cohort_indicator"
        else:
            group = "other"
        rows.append(
            {
                "feature_name": name,
                "data_type": field.dataType.simpleString(),
                "feature_group": group,
                "description": descriptions.get(name, f"Aggregate {group} feature computed before cutoff."),
            }
        )
    return rows


def collect_feature_validation(feature_df: DataFrame) -> list[dict[str, Any]]:
    rows = []
    total_rows = feature_df.count()
    for field in feature_df.schema.fields:
        name = field.name
        if name == "client_id":
            continue
        data_type = field.dataType.simpleString()
        exprs = [F.sum(F.when(F.col(name).isNull(), 1).otherwise(0)).alias("null_count")]
        if data_type in {"int", "bigint", "double", "float", "long", "decimal"} or data_type.startswith("decimal"):
            exprs.extend(
                [
                    F.min(F.col(name)).alias("min_value"),
                    F.max(F.col(name)).alias("max_value"),
                    F.avg(F.col(name)).alias("avg_value"),
                ]
            )
        metrics = feature_df.agg(*exprs).collect()[0].asDict()
        rows.append(
            {
                "feature_name": name,
                "data_type": data_type,
                "total_rows": total_rows,
                "null_count": normalize_value(metrics.get("null_count")),
                "null_rate": round((metrics.get("null_count") or 0) / total_rows, 6) if total_rows else None,
                "min_value": normalize_value(metrics.get("min_value")),
                "max_value": normalize_value(metrics.get("max_value")),
                "avg_value": normalize_value(metrics.get("avg_value")),
            }
        )
    return rows


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
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "# Feature Engineering Notes",
                "",
                "This artifact contains aggregate-only feature engineering notes.",
                "",
                f"Feature table status: {summary['status']}.",
                f"Cutoff date: {summary['cutoff_date']}.",
                f"Feature windows: {', '.join(str(value) for value in summary['feature_window_days'])} days.",
                f"Search features included: {summary['search_features_included']}.",
                "Counts are computed from processed rows as-is; final duplicate policy is deferred for review.",
                "Missing count-style features are filled with 0; missing recency and ratio values remain null.",
                "No final label, model, prediction, or API output was created.",
                "",
            ]
        ),
        encoding="utf-8",
    )


def main() -> int:
    args = parse_args()
    config_path = resolve_repo_path(args.config)
    config = read_simple_yaml(config_path)
    output_base = resolve_repo_path(args.output_base)
    feature_output_path = output_base / FEATURE_OUTPUT_DIR
    windows = args.feature_window_days or [30, 60, 90]
    target_config = config.get("target", {})
    cutoff_date = str(target_config.get("cutoff_date"))
    target_end = str(target_config.get("target_end"))
    if not cutoff_date or cutoff_date == "None":
        raise ValueError("configs/pipeline_config.yaml must define target.cutoff_date")

    search_path = output_base / "events" / "search_query"
    include_search = args.include_search or search_path.exists()

    spark = start_spark()
    try:
        add_to_cart = read_event(spark, output_base, "add_to_cart")
        remove_from_cart = read_event(spark, output_base, "remove_from_cart")
        product_buy = read_event(spark, output_base, "product_buy")
        products = spark.read.parquet(str(output_base / "product_properties_clean"))
        search_query = read_event(spark, output_base, "search_query") if include_search else None

        feature_frames = [
            event_count_features(add_to_cart, "add_to_cart", cutoff_date, windows, "sku"),
            event_count_features(remove_from_cart, "remove_from_cart", cutoff_date, windows, "sku"),
            event_count_features(product_buy, "product_buy", cutoff_date, windows, "sku"),
            product_metadata_features(add_to_cart, products, "cart", cutoff_date),
            product_metadata_features(product_buy, products, "bought", cutoff_date),
        ]
        event_frames = [add_to_cart, remove_from_cart, product_buy]
        if search_query is not None:
            feature_frames.append(event_count_features(search_query, "search_query", cutoff_date, windows))
            event_frames.append(search_query)
        feature_frames.append(active_days_feature(event_frames, cutoff_date))

        all_clients = feature_frames[0].select("client_id")
        for frame in feature_frames[1:]:
            all_clients = all_clients.unionByName(frame.select("client_id"))
        features = all_clients.distinct()
        for frame in feature_frames:
            features = features.join(frame, "client_id", "left")

        eligible_clients = (
            filter_history(add_to_cart, cutoff_date)
            .select("client_id")
            .unionByName(filter_history(product_buy, cutoff_date).select("client_id"))
            .distinct()
            .withColumn("is_eligible_purchase_propensity", F.lit(1))
        )
        features = features.join(eligible_clients, "client_id", "left")
        features = features.withColumn(
            "is_eligible_purchase_propensity",
            F.coalesce(F.col("is_eligible_purchase_propensity"), F.lit(0)).cast("int"),
        )
        features = fill_count_columns(features)
        features = add_ratio_features(features, include_search)
        features = features.cache()

        total_rows = features.count()
        eligible_count = features.agg(F.sum("is_eligible_purchase_propensity").alias("eligible_count")).collect()[0][
            "eligible_count"
        ]
        eligible_count = int(eligible_count or 0)
        eligible_rate = round(eligible_count / total_rows, 6) if total_rows else None
        feature_catalog = build_feature_catalog(features)
        feature_validation = collect_feature_validation(features)
        feature_column_names = [field.name for field in features.schema.fields]
        label_like_columns = [
            column
            for column in feature_column_names
            if column.lower() in {"label", "target", "y", "purchase_in_target"}
            or column.lower().startswith("label_")
            or column.lower().startswith("target_")
        ]

        clean_output_dir(feature_output_path)
        features.write.mode("overwrite").parquet(str(feature_output_path))

        summary = {
            "generated_at_date": datetime.now(timezone.utc).date().isoformat(),
            "phase": "Phase 3: Feature Engineering",
            "status": "success",
            "feature_output_path": relative_path(feature_output_path),
            "artifact_paths": {
                "summary": relative_path(FEATURE_SUMMARY_PATH),
                "catalog": relative_path(FEATURE_CATALOG_PATH),
                "validation": relative_path(FEATURE_VALIDATION_PATH),
                "notes": relative_path(FEATURE_NOTES_PATH),
            },
            "target_task": target_config.get("task"),
            "target_window_days": target_config.get("target_window_days"),
            "cutoff_date": cutoff_date,
            "target_end": target_end,
            "feature_window_days": windows,
            "search_features_included": bool(include_search),
            "page_visit_included": False,
            "total_feature_rows": total_rows,
            "eligible_cohort_count": eligible_count,
            "eligible_cohort_rate": eligible_rate,
            "feature_count": len(feature_catalog),
            "feature_groups": sorted({row["feature_group"] for row in feature_catalog}),
            "null_fill_strategy": {
                "count_features": "filled_with_0",
                "recency_features": "left_null_when_client_has_no_event_type",
                "ratio_features": "left_null_when_denominator_is_0",
            },
            "duplicate_handling_assumption": (
                "Features are computed from processed rows as-is; final duplicate policy is deferred for review."
            ),
            "leakage_validation": {
                "features_use_events_before_cutoff": True,
                "target_window_events_used": False,
                "final_label_created": False,
                "label_like_columns": label_like_columns,
            },
            "privacy": {
                "artifact_level": "aggregate_only",
                "raw_client_ids_persisted": False,
                "raw_query_text_persisted": False,
                "product_names_persisted": False,
                "row_samples_persisted": False,
            },
        }

        write_json(FEATURE_SUMMARY_PATH, summary)
        write_csv(
            FEATURE_CATALOG_PATH,
            feature_catalog,
            ["feature_name", "data_type", "feature_group", "description"],
        )
        write_csv(
            FEATURE_VALIDATION_PATH,
            feature_validation,
            ["feature_name", "data_type", "total_rows", "null_count", "null_rate", "min_value", "max_value", "avg_value"],
        )
        write_notes(FEATURE_NOTES_PATH, summary)

        print("Feature engineering completed.")
        print(f"Feature rows: {total_rows}")
        print(f"Eligible cohort count: {eligible_count}")
        print(f"Search features included: {include_search}")
        print(f"Wrote feature table to {relative_path(feature_output_path)}")
        print(f"Wrote sanitized artifacts under {relative_path(ARTIFACT_DIR)}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
