"""Run sanitized Phase 1 EDA for the Synerise raw Parquet tables.

The job writes table-level artifacts only:
- artifacts/eda/eda_summary.json
- artifacts/eda/table_overview.csv
- artifacts/eda/event_table_overview.csv
- artifacts/eda/product_table_overview.csv
- artifacts/eda/column_overview.csv

It does not write raw row samples or raw identifier/query/name values.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_BASE_DIR = PROJECT_ROOT / "data" / "raw" / "synerise_dataset"
EDA_OUTPUT_DIR = PROJECT_ROOT / "artifacts" / "eda"
EDA_SUMMARY_PATH = EDA_OUTPUT_DIR / "eda_summary.json"
TABLE_OVERVIEW_PATH = EDA_OUTPUT_DIR / "table_overview.csv"
EVENT_TABLE_OVERVIEW_PATH = EDA_OUTPUT_DIR / "event_table_overview.csv"
PRODUCT_TABLE_OVERVIEW_PATH = EDA_OUTPUT_DIR / "product_table_overview.csv"
COLUMN_OVERVIEW_PATH = EDA_OUTPUT_DIR / "column_overview.csv"
SAFE_COUNT_MAX_BYTES = 250 * 1024 * 1024

TABLES = {
    "add_to_cart": RAW_BASE_DIR / "add_to_cart.parquet",
    "page_visit": RAW_BASE_DIR / "page_visit.parquet",
    "product_buy": RAW_BASE_DIR / "product_buy.parquet",
    "product_properties": RAW_BASE_DIR / "product_properties.parquet",
    "remove_from_cart": RAW_BASE_DIR / "remove_from_cart.parquet",
    "search_query": RAW_BASE_DIR / "search_query.parquet",
}

TRACKED_COLUMNS = ["client_id", "sku", "url", "query", "name", "timestamp", "category", "price"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate sanitized table-level EDA summaries for raw Synerise Parquet tables."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--safe-mode",
        action="store_true",
        help="Use safe mode. Exact row counts are deferred for files above the safe size threshold.",
    )
    mode.add_argument(
        "--full-count",
        action="store_true",
        help="Compute exact row counts for all readable tables. This may take longer.",
    )
    return parser.parse_args()


def human_size(num_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(num_bytes)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{num_bytes} B"


def relative_path(path: Path) -> str:
    return path.relative_to(PROJECT_ROOT).as_posix()


def schema_to_json(df: DataFrame) -> dict[str, Any]:
    return json.loads(df.schema.json())


def short_error_summary(context: str, exc: Exception) -> str:
    return f"{context} ({exc.__class__.__name__})"


def start_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("eda-summary")
        .master("local[*]")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def should_count_rows(size_bytes: int, full_count: bool) -> bool:
    return full_count or size_bytes <= SAFE_COUNT_MAX_BYTES


def build_aggregate_expressions(df: DataFrame) -> list[Any]:
    columns = set(df.columns)
    expressions: list[Any] = []

    for column in TRACKED_COLUMNS:
        if column in columns:
            expressions.append(F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias(f"{column}__null_count"))

    for column in ["client_id", "sku", "url", "category"]:
        if column in columns:
            expressions.append(F.approx_count_distinct(F.col(column)).alias(f"{column}__approx_distinct_count"))

    if "timestamp" in columns:
        parsed_timestamp = F.to_timestamp(F.col("timestamp"))
        expressions.extend(
            [
                F.min(parsed_timestamp).cast("string").alias("timestamp__parsed_min"),
                F.max(parsed_timestamp).cast("string").alias("timestamp__parsed_max"),
                F.sum(F.when(F.col("timestamp").isNotNull() & parsed_timestamp.isNotNull(), 1).otherwise(0)).alias(
                    "timestamp__parse_success_count"
                ),
                F.sum(F.when(F.col("timestamp").isNotNull() & parsed_timestamp.isNull(), 1).otherwise(0)).alias(
                    "timestamp__parse_failure_count"
                ),
            ]
        )

    if "price" in columns:
        expressions.extend(
            [
                F.min(F.col("price")).alias("price__min"),
                F.max(F.col("price")).alias("price__max"),
                F.avg(F.col("price")).alias("price__avg"),
            ]
        )

    return expressions


def collect_aggregate_metrics(df: DataFrame) -> dict[str, Any]:
    expressions = build_aggregate_expressions(df)
    if not expressions:
        return {}
    row = df.agg(*expressions).collect()[0].asDict()
    return {key: normalize_value(value) for key, value in row.items()}


def collect_price_quantiles(df: DataFrame) -> dict[str, float | None]:
    if "price" not in df.columns:
        return {}
    quantiles = df.approxQuantile("price", [0.25, 0.5, 0.75], 0.01)
    if len(quantiles) != 3:
        return {"price__q25": None, "price__median": None, "price__q75": None}
    return {
        "price__q25": normalize_value(quantiles[0]),
        "price__median": normalize_value(quantiles[1]),
        "price__q75": normalize_value(quantiles[2]),
    }


def normalize_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return round(value, 4)
    return str(value)


def summarize_columns(df: DataFrame, table_name: str, metrics: dict[str, Any]) -> list[dict[str, Any]]:
    column_rows = []
    for field in df.schema.fields:
        column = field.name
        column_rows.append(
            {
                "table_name": table_name,
                "column_name": column,
                "data_type": field.dataType.simpleString(),
                "nullable": field.nullable,
                "null_count": metrics.get(f"{column}__null_count"),
                "approx_distinct_count": metrics.get(f"{column}__approx_distinct_count"),
            }
        )
    return column_rows


def summarize_table(
    spark: SparkSession,
    table_name: str,
    path: Path,
    full_count: bool,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    size_bytes = path.stat().st_size if path.exists() else None
    summary: dict[str, Any] = {
        "table_name": table_name,
        "relative_path": relative_path(path),
        "file_size_bytes": size_bytes,
        "file_size_human": human_size(size_bytes) if size_bytes is not None else None,
        "spark_readable": False,
        "schema": None,
        "row_count": None,
        "row_count_status": "not_available",
        "row_count_skipped_reason": None,
        "distinct_client_id_count": None,
        "null_client_id_count": None,
        "distinct_sku_count": None,
        "null_sku_count": None,
        "distinct_url_count": None,
        "null_url_count": None,
        "null_query_count": None,
        "null_name_count": None,
        "timestamp_min": None,
        "timestamp_max": None,
        "null_timestamp_count": None,
        "timestamp_parse_success_count": None,
        "timestamp_parse_failure_count": None,
        "distinct_category_count": None,
        "null_category_count": None,
        "null_price_count": None,
        "price_summary": None,
        "error_summary": None,
        "notes": [],
    }

    if not path.exists():
        summary["error_summary"] = "Expected raw table file is not available."
        return summary, []

    try:
        df = spark.read.parquet(str(path))
        summary["spark_readable"] = True
        summary["schema"] = schema_to_json(df)

        if size_bytes is not None and should_count_rows(size_bytes, full_count):
            summary["row_count"] = df.count()
            summary["row_count_status"] = "counted"
        else:
            summary["row_count_status"] = "deferred_safe_mode"
            summary["row_count_skipped_reason"] = (
                "Full row count was deferred by the EDA safe-mode strategy and can be computed "
                "with --full-count. This is not a Spark readability limitation."
            )

        metrics = collect_aggregate_metrics(df)
        metrics.update(collect_price_quantiles(df))
        apply_metrics(summary, metrics)
        column_rows = summarize_columns(df, table_name, metrics)
        return summary, column_rows
    except Exception as exc:  # noqa: BLE001 - keep per-table inspection resilient.
        summary["error_summary"] = short_error_summary("EDA failed for this table.", exc)
        return summary, []


def apply_metrics(summary: dict[str, Any], metrics: dict[str, Any]) -> None:
    summary["distinct_client_id_count"] = metrics.get("client_id__approx_distinct_count")
    summary["null_client_id_count"] = metrics.get("client_id__null_count")
    summary["distinct_sku_count"] = metrics.get("sku__approx_distinct_count")
    summary["null_sku_count"] = metrics.get("sku__null_count")
    summary["distinct_url_count"] = metrics.get("url__approx_distinct_count")
    summary["null_url_count"] = metrics.get("url__null_count")
    summary["null_query_count"] = metrics.get("query__null_count")
    summary["null_name_count"] = metrics.get("name__null_count")
    summary["timestamp_min"] = metrics.get("timestamp__parsed_min")
    summary["timestamp_max"] = metrics.get("timestamp__parsed_max")
    summary["null_timestamp_count"] = metrics.get("timestamp__null_count")
    summary["timestamp_parse_success_count"] = metrics.get("timestamp__parse_success_count")
    summary["timestamp_parse_failure_count"] = metrics.get("timestamp__parse_failure_count")
    summary["distinct_category_count"] = metrics.get("category__approx_distinct_count")
    summary["null_category_count"] = metrics.get("category__null_count")
    summary["null_price_count"] = metrics.get("price__null_count")

    price_keys = ["price__min", "price__max", "price__avg", "price__q25", "price__median", "price__q75"]
    price_summary = {key.replace("price__", ""): metrics.get(key) for key in price_keys if key in metrics}
    summary["price_summary"] = price_summary or None


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})


def flatten_table_for_csv(summary: dict[str, Any]) -> dict[str, Any]:
    schema_fields = summary.get("schema", {}).get("fields", []) if summary.get("schema") else []
    columns = {field.get("name") for field in schema_fields}
    return {
        "table_name": summary["table_name"],
        "relative_path": summary["relative_path"],
        "file_size_human": summary["file_size_human"],
        "spark_readable": summary["spark_readable"],
        "row_count": summary["row_count"],
        "row_count_status": summary["row_count_status"],
        "column_count": len(schema_fields),
        "timestamp_min": summary["timestamp_min"],
        "timestamp_max": summary["timestamp_max"],
        "has_client_id": "client_id" in columns,
        "has_timestamp": "timestamp" in columns,
        "has_sku": "sku" in columns,
        "has_url": "url" in columns,
        "has_query": "query" in columns,
        "has_category": "category" in columns,
        "has_price": "price" in columns,
    }


def event_type_for(table_name: str) -> str | None:
    return {
        "add_to_cart": "add_to_cart",
        "page_visit": "page_visit",
        "product_buy": "product_buy",
        "remove_from_cart": "remove_from_cart",
        "search_query": "search_query",
    }.get(table_name)


def entity_column_for(table_name: str) -> str | None:
    return {
        "add_to_cart": "sku",
        "product_buy": "sku",
        "remove_from_cart": "sku",
        "page_visit": "url",
        "search_query": "query",
    }.get(table_name)


def flatten_event_table_for_csv(summary: dict[str, Any]) -> dict[str, Any] | None:
    table_name = summary["table_name"]
    event_type = event_type_for(table_name)
    entity_column = entity_column_for(table_name)
    if event_type is None or not summary.get("schema"):
        return None

    distinct_entity_count = None
    null_entity_count = None
    if entity_column == "sku":
        distinct_entity_count = summary["distinct_sku_count"]
        null_entity_count = summary["null_sku_count"]
    elif entity_column == "url":
        distinct_entity_count = summary["distinct_url_count"]
        null_entity_count = summary["null_url_count"]
    elif entity_column == "query":
        null_entity_count = summary["null_query_count"]

    return {
        "table_name": table_name,
        "event_type": event_type,
        "row_count": summary["row_count"],
        "distinct_client_id_count": summary["distinct_client_id_count"],
        "null_client_id_count": summary["null_client_id_count"],
        "timestamp_min": summary["timestamp_min"],
        "timestamp_max": summary["timestamp_max"],
        "null_timestamp_count": summary["null_timestamp_count"],
        "timestamp_parse_success_count": summary["timestamp_parse_success_count"],
        "timestamp_parse_failure_count": summary["timestamp_parse_failure_count"],
        "entity_column": entity_column,
        "distinct_entity_count": distinct_entity_count,
        "null_entity_count": null_entity_count,
    }


def flatten_product_table_for_csv(summary: dict[str, Any]) -> dict[str, Any] | None:
    if summary["table_name"] != "product_properties":
        return None
    price_summary = summary["price_summary"] or {}
    return {
        "table_name": summary["table_name"],
        "row_count": summary["row_count"],
        "distinct_sku_count": summary["distinct_sku_count"],
        "null_sku_count": summary["null_sku_count"],
        "distinct_category_count": summary["distinct_category_count"],
        "null_category_count": summary["null_category_count"],
        "null_price_count": summary["null_price_count"],
        "price_min": price_summary.get("min"),
        "price_max": price_summary.get("max"),
        "price_avg": price_summary.get("avg"),
        "price_q25": price_summary.get("q25"),
        "price_median": price_summary.get("median"),
        "price_q75": price_summary.get("q75"),
        "null_name_count": summary["null_name_count"],
    }


def main() -> int:
    args = parse_args()
    full_count = bool(args.full_count)
    mode = "full-count" if full_count else "safe-mode"

    print("EDA summary job")
    print(f"Mode: {mode}")
    print("Raw data base: data/raw/synerise_dataset")
    if full_count:
        print("Full-count mode enabled. Exact row counts may take longer.")
    else:
        print("Safe mode enabled. Large exact row counts may be deferred.")

    EDA_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    spark = start_spark()

    table_summaries: list[dict[str, Any]] = []
    column_rows: list[dict[str, Any]] = []
    try:
        for table_name, path in TABLES.items():
            print(f"Inspecting {table_name}...")
            table_summary, table_columns = summarize_table(spark, table_name, path, full_count)
            table_summaries.append(table_summary)
            column_rows.extend(table_columns)
            status = table_summary["row_count_status"]
            print(f"  readable={table_summary['spark_readable']} row_count_status={status}")
    finally:
        spark.stop()

    summary_payload = {
        "generated_at_date": datetime.now(timezone.utc).date().isoformat(),
        "phase": "Phase 1: EDA",
        "mode": mode,
        "safe_count_max_bytes": SAFE_COUNT_MAX_BYTES,
        "notes": [
            "Artifacts contain sanitized table-level summaries only.",
            "No raw row samples, actual client ids, query text, or product names are persisted.",
            "Deferred safe-mode counts can be computed with --full-count.",
            "CSV outputs are split into compact general, event-specific, product-specific, and column-level views.",
        ],
        "tables": table_summaries,
    }

    table_rows = [flatten_table_for_csv(summary) for summary in table_summaries]
    event_table_rows = [
        row for row in (flatten_event_table_for_csv(summary) for summary in table_summaries) if row is not None
    ]
    product_table_rows = [
        row for row in (flatten_product_table_for_csv(summary) for summary in table_summaries) if row is not None
    ]
    table_fieldnames = list(table_rows[0].keys()) if table_rows else []
    event_table_fieldnames = list(event_table_rows[0].keys()) if event_table_rows else []
    product_table_fieldnames = list(product_table_rows[0].keys()) if product_table_rows else []
    column_fieldnames = [
        "table_name",
        "column_name",
        "data_type",
        "nullable",
        "null_count",
        "approx_distinct_count",
    ]

    write_json(EDA_SUMMARY_PATH, summary_payload)
    write_csv(TABLE_OVERVIEW_PATH, table_rows, table_fieldnames)
    write_csv(EVENT_TABLE_OVERVIEW_PATH, event_table_rows, event_table_fieldnames)
    write_csv(PRODUCT_TABLE_OVERVIEW_PATH, product_table_rows, product_table_fieldnames)
    write_csv(COLUMN_OVERVIEW_PATH, column_rows, column_fieldnames)

    print("Wrote artifacts:")
    print("  artifacts/eda/eda_summary.json")
    print("  artifacts/eda/table_overview.csv")
    print("  artifacts/eda/event_table_overview.csv")
    print("  artifacts/eda/product_table_overview.csv")
    print("  artifacts/eda/column_overview.csv")
    return 0


if __name__ == "__main__":
    sys.exit(main())
