"""Validate target and feature assumptions with aggregate-only EDA.

This job checks business assumptions behind the current purchase propensity MVP:

- whether target-window purchases usually have prior cart activity
- how the current active cohort differs from a cart-conversion cohort
- whether product metadata is stable at SKU level
- whether selected features show directional relationship with the label
- whether search behavior has useful aggregate signal

It writes sanitized aggregate artifacts only. It does not modify preprocessing,
modeling, scoring, API, or demo code, and it does not persist raw client ids,
raw query text, product names, row samples, or row-level predictions.
"""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.dataset as ds
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "pipeline_config.yaml"
DEFAULT_PROCESSED_BASE = PROJECT_ROOT / "data" / "processed"
DEFAULT_RAW_BASE = PROJECT_ROOT / "data" / "raw" / "synerise_dataset"
ARTIFACT_DIR = PROJECT_ROOT / "artifacts" / "target_validation"

PURCHASE_PATH_PATH = ARTIFACT_DIR / "purchase_path_summary.csv"
COHORT_OVERLAP_PATH = ARTIFACT_DIR / "cohort_overlap_summary.csv"
PRODUCT_METADATA_PATH = ARTIFACT_DIR / "product_metadata_consistency.csv"
FEATURE_TARGET_PATH = ARTIFACT_DIR / "feature_target_relationship.csv"
SEARCH_SIGNAL_PATH = ARTIFACT_DIR / "search_signal_summary.csv"
SUMMARY_MD_PATH = ARTIFACT_DIR / "target_assumption_validation_summary.md"

COUNT_BUCKETS = [
    ("0", 0, 0),
    ("1", 1, 1),
    ("2-3", 2, 3),
    ("4-10", 4, 10),
    ("11-50", 11, 50),
    (">50", 51, None),
]
RECENCY_BUCKETS = [
    ("null/no event", None, None),
    ("0-7 days", 0, 7),
    ("8-30 days", 8, 30),
    ("31-60 days", 31, 60),
    (">60 days", 61, None),
]
FEATURES_TO_BUCKET = [
    "add_to_cart_count",
    "remove_from_cart_count",
    "product_buy_count",
    "search_query_count",
    "active_days_count",
    "days_since_last_add_to_cart",
    "days_since_last_remove_from_cart",
    "days_since_last_product_buy",
    "days_since_last_search_query",
    "buy_to_cart_ratio",
    "remove_to_cart_ratio",
    "search_to_cart_ratio",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate target and feature assumptions with aggregate-only EDA.")
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative config path. Default: configs/pipeline_config.yaml.",
    )
    parser.add_argument(
        "--processed-base",
        default=DEFAULT_PROCESSED_BASE.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative processed data base. Default: data/processed.",
    )
    parser.add_argument(
        "--raw-base",
        default=DEFAULT_RAW_BASE.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative raw Synerise data base. Default: data/raw/synerise_dataset.",
    )
    parser.add_argument(
        "--engine",
        choices=["auto", "spark", "pyarrow"],
        default="auto",
        help="Execution engine. Auto tries Spark first and falls back to PyArrow for Windows local Parquet issues.",
    )
    return parser.parse_args()


def resolve_repo_path(path_text: str) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        raise ValueError("path arguments must be repo-relative")
    return PROJECT_ROOT / path


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
        SparkSession.builder.appName("target-feature-assumption-validation-eda")
        .master("local[*]")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: normalize_value(row.get(key)) for key in fieldnames})


def count_df(df: DataFrame) -> int:
    return int(df.count())


def ratio(numerator: int | float | None, denominator: int | float | None) -> float | None:
    if not denominator:
        return None
    return round(float(numerator or 0) / float(denominator), 6)


def metric_row(metric: str, value: Any, denominator: Any, interpretation: str) -> dict[str, Any]:
    return {
        "metric": metric,
        "value": value,
        "denominator": denominator,
        "rate": ratio(value, denominator) if isinstance(value, (int, float)) and isinstance(denominator, (int, float)) else None,
        "interpretation": interpretation,
    }


def read_event(processed_base: Path, table_name: str, spark: SparkSession) -> DataFrame:
    return spark.read.parquet(str(processed_base / "events" / table_name))


def history_events(df: DataFrame, cutoff_date: str) -> DataFrame:
    return df.where(F.col("event_ts") < F.to_timestamp(F.lit(cutoff_date)))


def target_events(df: DataFrame, cutoff_date: str, target_end: str) -> DataFrame:
    return df.where(
        (F.col("event_ts") >= F.to_timestamp(F.lit(cutoff_date)))
        & (F.col("event_ts") < F.date_add(F.to_date(F.lit(target_end)), 1).cast("timestamp"))
    )


def history_clients(df: DataFrame, cutoff_date: str) -> DataFrame:
    return history_events(df, cutoff_date).select("client_id").distinct()


def purchase_path_summary(add_to_cart: DataFrame, product_buy: DataFrame, cutoff_date: str, target_end: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    history_cart_clients = history_clients(add_to_cart, cutoff_date).cache()
    history_carts = history_events(add_to_cart, cutoff_date).select(
        "client_id", "sku", F.col("event_ts").alias("cart_ts")
    )
    target_purchases = target_events(product_buy, cutoff_date, target_end).select(
        "client_id", "sku", F.col("event_ts").alias("purchase_ts")
    )
    target_buyers = target_purchases.select("client_id").distinct().cache()

    target_buyer_count = count_df(target_buyers)
    buyers_with_prior_cart = count_df(target_buyers.join(history_cart_clients, "client_id", "inner"))
    buyers_without_prior_cart = target_buyer_count - buyers_with_prior_cart

    target_purchase_events = target_purchases.withColumn("purchase_event_id", F.monotonically_increasing_id()).cache()
    target_purchase_count = count_df(target_purchase_events)

    prior_same_sku_by_purchase = (
        target_purchase_events.alias("p")
        .join(
            history_carts.alias("c"),
            (F.col("p.client_id") == F.col("c.client_id"))
            & (F.col("p.sku") == F.col("c.sku"))
            & (F.col("c.cart_ts") < F.col("p.purchase_ts")),
            "left",
        )
        .groupBy("purchase_event_id")
        .agg(F.max("cart_ts").alias("latest_prior_cart_ts"))
        .cache()
    )
    target_purchases_with_prior_same_sku = count_df(prior_same_sku_by_purchase.where(F.col("latest_prior_cart_ts").isNotNull()))

    first_target_purchase_by_client_sku = (
        target_purchases.groupBy("client_id", "sku").agg(F.min("purchase_ts").alias("first_purchase_ts")).cache()
    )
    first_purchase_with_latest_cart = (
        first_target_purchase_by_client_sku.alias("p")
        .join(
            history_carts.alias("c"),
            (F.col("p.client_id") == F.col("c.client_id"))
            & (F.col("p.sku") == F.col("c.sku"))
            & (F.col("c.cart_ts") < F.col("p.first_purchase_ts")),
            "left",
        )
        .groupBy("p.client_id", "p.sku", "p.first_purchase_ts")
        .agg(F.max("c.cart_ts").alias("latest_prior_cart_ts"))
        .cache()
    )
    buyers_with_same_sku_cart_before_first_purchase = count_df(
        first_purchase_with_latest_cart.where(F.col("latest_prior_cart_ts").isNotNull()).select("client_id").distinct()
    )

    delay_days = first_purchase_with_latest_cart.where(F.col("latest_prior_cart_ts").isNotNull()).withColumn(
        "delay_days", F.datediff(F.to_date("first_purchase_ts"), F.to_date("latest_prior_cart_ts"))
    )
    delay_count = count_df(delay_days)
    delay_stats_row = delay_days.agg(
        F.min("delay_days").alias("min_delay_days"),
        F.max("delay_days").alias("max_delay_days"),
    ).collect()[0]
    delay_quantiles = delay_days.approxQuantile("delay_days", [0.5, 0.75, 0.9], 0.01) if delay_count else []

    rows = [
        metric_row(
            "target_window_buyers",
            target_buyer_count,
            target_buyer_count,
            "Distinct clients with at least one purchase in the target window.",
        ),
        metric_row(
            "target_window_buyers_with_add_to_cart_before_cutoff",
            buyers_with_prior_cart,
            target_buyer_count,
            "Share of target buyers already represented in the cart-history cohort.",
        ),
        metric_row(
            "target_window_buyers_without_add_to_cart_before_cutoff",
            buyers_without_prior_cart,
            target_buyer_count,
            "Target buyers missed by a cart-only cohort before cutoff.",
        ),
        metric_row(
            "target_window_buyers_with_prior_same_sku_cart_before_first_purchase",
            buyers_with_same_sku_cart_before_first_purchase,
            target_buyer_count,
            "Clients whose target purchase path includes same-SKU cart history before first target purchase.",
        ),
        metric_row(
            "target_purchase_events",
            target_purchase_count,
            target_purchase_count,
            "Target-window purchase events considered for same-SKU cart path validation.",
        ),
        metric_row(
            "target_purchase_events_with_prior_same_sku_add_to_cart",
            target_purchases_with_prior_same_sku,
            target_purchase_count,
            "Share of target purchase events with a prior same-SKU add_to_cart before cutoff.",
        ),
        metric_row(
            "same_sku_cart_to_first_purchase_delay_min_days",
            delay_stats_row["min_delay_days"],
            delay_count,
            "Minimum delay among first target purchases with prior same-SKU cart history.",
        ),
        metric_row(
            "same_sku_cart_to_first_purchase_delay_median_days",
            delay_quantiles[0] if len(delay_quantiles) == 3 else None,
            delay_count,
            "Approximate median delay from latest prior same-SKU cart to first target purchase.",
        ),
        metric_row(
            "same_sku_cart_to_first_purchase_delay_p75_days",
            delay_quantiles[1] if len(delay_quantiles) == 3 else None,
            delay_count,
            "Approximate p75 delay from latest prior same-SKU cart to first target purchase.",
        ),
        metric_row(
            "same_sku_cart_to_first_purchase_delay_p90_days",
            delay_quantiles[2] if len(delay_quantiles) == 3 else None,
            delay_count,
            "Approximate p90 delay from latest prior same-SKU cart to first target purchase.",
        ),
        metric_row(
            "same_sku_cart_to_first_purchase_delay_max_days",
            delay_stats_row["max_delay_days"],
            delay_count,
            "Maximum delay among first target purchases with prior same-SKU cart history.",
        ),
    ]
    for row in rows:
        if row["metric"].startswith("same_sku_cart_to_first_purchase_delay"):
            row["rate"] = None

    stats = {
        "target_buyer_count": target_buyer_count,
        "buyers_with_prior_cart_rate": ratio(buyers_with_prior_cart, target_buyer_count),
        "buyers_without_prior_cart_rate": ratio(buyers_without_prior_cart, target_buyer_count),
        "target_purchase_same_sku_prior_cart_rate": ratio(target_purchases_with_prior_same_sku, target_purchase_count),
        "same_sku_delay_median_days": normalize_value(delay_quantiles[0] if len(delay_quantiles) == 3 else None),
    }

    for frame in [
        history_cart_clients,
        target_buyers,
        target_purchase_events,
        prior_same_sku_by_purchase,
        first_target_purchase_by_client_sku,
        first_purchase_with_latest_cart,
    ]:
        frame.unpersist()
    return rows, stats


def cohort_overlap_summary(
    add_to_cart: DataFrame,
    product_buy: DataFrame,
    search_query: DataFrame | None,
    features: DataFrame,
    cutoff_date: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    cart_clients = history_clients(add_to_cart, cutoff_date).cache()
    buyer_clients = history_clients(product_buy, cutoff_date).cache()
    search_clients = history_clients(search_query, cutoff_date).cache() if search_query is not None else None
    current_clients = features.where(F.col("is_eligible_purchase_propensity") == 1).select("client_id").distinct().cache()

    cart_count = count_df(cart_clients)
    buyer_count = count_df(buyer_clients)
    current_count = count_df(current_clients)
    search_count = count_df(search_clients) if search_clients is not None else 0
    buyer_without_cart_count = count_df(buyer_clients.join(cart_clients, "client_id", "left_anti"))
    search_only_count = (
        count_df(search_clients.join(current_clients, "client_id", "left_anti")) if search_clients is not None else 0
    )
    cart_buyer_intersection = count_df(cart_clients.join(buyer_clients, "client_id", "inner"))
    cart_buyer_union = cart_count + buyer_count - cart_buyer_intersection
    current_extra_vs_cart = current_count - cart_count

    rows = [
        {
            "cohort_name": "add_to_cart_before_cutoff",
            "client_count": cart_count,
            "relation_to_current_target": "cart conversion cohort",
            "interpretation": "Clients with cart intent before cutoff.",
        },
        {
            "cohort_name": "product_buy_before_cutoff",
            "client_count": buyer_count,
            "relation_to_current_target": "purchase history component",
            "interpretation": "Clients with purchase history before cutoff.",
        },
        {
            "cohort_name": "search_query_before_cutoff",
            "client_count": search_count,
            "relation_to_current_target": "candidate broader future cohort",
            "interpretation": "Clients with search activity before cutoff.",
        },
        {
            "cohort_name": "current_add_to_cart_or_product_buy_before_cutoff",
            "client_count": current_count,
            "relation_to_current_target": "current eligible cohort",
            "interpretation": "Current purchase propensity cohort used by Phase 4 labels.",
        },
        {
            "cohort_name": "cart_conversion_cohort",
            "client_count": cart_count,
            "relation_to_current_target": "subset or near-equivalent comparator",
            "interpretation": "Cart-only eligible cohort for cart conversion framing.",
        },
        {
            "cohort_name": "buyers_without_cart_before_cutoff",
            "client_count": buyer_without_cart_count,
            "relation_to_current_target": "extra clients beyond cart cohort",
            "interpretation": f"Rate among current cohort: {ratio(buyer_without_cart_count, current_count)}.",
        },
        {
            "cohort_name": "search_only_before_cutoff",
            "client_count": search_only_count,
            "relation_to_current_target": "not included in current target",
            "interpretation": f"Search clients outside current cohort; candidate for broader future target. Rate among search clients: {ratio(search_only_count, search_count)}.",
        },
        {
            "cohort_name": "add_to_cart_product_buy_jaccard_overlap",
            "client_count": ratio(cart_buyer_intersection, cart_buyer_union),
            "relation_to_current_target": "derived overlap metric",
            "interpretation": "Jaccard overlap between cart-history and purchase-history clients.",
        },
        {
            "cohort_name": "buyers_without_cart_rate",
            "client_count": ratio(buyer_without_cart_count, current_count),
            "relation_to_current_target": "derived metric",
            "interpretation": "Share of current cohort that would be excluded by cart-only eligibility.",
        },
        {
            "cohort_name": "current_vs_cart_cohort_extra_clients",
            "client_count": current_extra_vs_cart,
            "relation_to_current_target": "derived metric",
            "interpretation": "Additional clients in current cohort compared with cart conversion cohort.",
        },
        {
            "cohort_name": "current_vs_cart_cohort_extra_rate",
            "client_count": ratio(current_extra_vs_cart, current_count),
            "relation_to_current_target": "derived metric",
            "interpretation": "Share of current cohort added by including purchase-history clients without cart history.",
        },
    ]

    stats = {
        "cart_count": cart_count,
        "buyer_count": buyer_count,
        "current_count": current_count,
        "buyer_without_cart_count": buyer_without_cart_count,
        "buyer_without_cart_rate": ratio(buyer_without_cart_count, current_count),
        "current_extra_vs_cart": current_extra_vs_cart,
        "current_extra_vs_cart_rate": ratio(current_extra_vs_cart, current_count),
        "jaccard_overlap": ratio(cart_buyer_intersection, cart_buyer_union),
        "search_only_count": search_only_count,
    }

    for frame in [cart_clients, buyer_clients, current_clients]:
        frame.unpersist()
    if search_clients is not None:
        search_clients.unpersist()
    return rows, stats


def metadata_profile(df: DataFrame, label: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    sku_profile = (
        df.select("sku", "category", "price")
        .groupBy("sku")
        .agg(
            F.count(F.lit(1)).alias("rows_per_sku"),
            F.countDistinct("category").alias("distinct_category_count"),
            F.countDistinct("price").alias("distinct_price_count"),
            F.countDistinct(F.struct("category", "price")).alias("distinct_category_price_pair_count"),
        )
        .cache()
    )
    row = df.agg(F.count(F.lit(1)).alias("row_count"), F.countDistinct("sku").alias("distinct_sku_count")).collect()[0]
    duplicate_row = sku_profile.agg(
        F.sum(F.when(F.col("rows_per_sku") > 1, 1).otherwise(0)).alias("sku_with_multiple_rows"),
        F.sum(F.when(F.col("distinct_category_count") > 1, 1).otherwise(0)).alias("sku_with_multiple_categories"),
        F.sum(F.when(F.col("distinct_price_count") > 1, 1).otherwise(0)).alias("sku_with_multiple_prices"),
        F.sum(F.when(F.col("distinct_category_price_pair_count") > 1, 1).otherwise(0)).alias("sku_with_multiple_category_price_pairs"),
        F.max("rows_per_sku").alias("max_rows_per_sku"),
    ).collect()[0]

    rows = [
        {
            "metric": f"{label}_row_count",
            "value": int(row["row_count"] or 0),
            "interpretation": f"Rows in {label} product metadata.",
        },
        {
            "metric": f"{label}_distinct_sku_count",
            "value": int(row["distinct_sku_count"] or 0),
            "interpretation": f"Distinct SKUs in {label} product metadata.",
        },
        {
            "metric": f"{label}_sku_with_multiple_rows",
            "value": int(duplicate_row["sku_with_multiple_rows"] or 0),
            "interpretation": "Nonzero values indicate possible join row expansion if joined without SKU deduplication.",
        },
        {
            "metric": f"{label}_sku_with_multiple_category_values",
            "value": int(duplicate_row["sku_with_multiple_categories"] or 0),
            "interpretation": "Nonzero values indicate category instability for SKU-level features.",
        },
        {
            "metric": f"{label}_sku_with_multiple_price_values",
            "value": int(duplicate_row["sku_with_multiple_prices"] or 0),
            "interpretation": "Nonzero values indicate price instability for SKU-level features.",
        },
        {
            "metric": f"{label}_sku_with_multiple_category_price_pairs",
            "value": int(duplicate_row["sku_with_multiple_category_price_pairs"] or 0),
            "interpretation": "Nonzero values indicate category-price pair instability.",
        },
        {
            "metric": f"{label}_max_rows_per_sku",
            "value": int(duplicate_row["max_rows_per_sku"] or 0),
            "interpretation": "Maximum metadata rows for one SKU; greater than 1 can expand event joins.",
        },
    ]
    stats = {row["metric"]: row["value"] for row in rows}
    sku_profile.unpersist()
    return rows, stats


def product_metadata_consistency(spark: SparkSession, processed_base: Path, raw_base: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    stats: dict[str, Any] = {}
    raw_path = raw_base / "product_properties.parquet"
    processed_path = processed_base / "product_properties_clean"
    if raw_path.exists():
        raw_df = spark.read.parquet(str(raw_path)).select("sku", "category", "price")
        raw_rows, raw_stats = metadata_profile(raw_df, "raw")
        rows.extend(raw_rows)
        stats.update(raw_stats)
    if processed_path.exists():
        processed_df = spark.read.parquet(str(processed_path)).select("sku", "category", "price")
        processed_rows, processed_stats = metadata_profile(processed_df, "processed")
        rows.extend(processed_rows)
        stats.update(processed_stats)

    raw_unstable = int(stats.get("raw_sku_with_multiple_rows", 0) or 0) > 0
    processed_unstable = int(stats.get("processed_sku_with_multiple_rows", 0) or 0) > 0
    rows.append(
        {
            "metric": "metadata_join_row_expansion_risk",
            "value": "yes" if raw_unstable else "no",
            "interpretation": "Raw metadata can expand event joins when SKU has multiple metadata rows." if raw_unstable else "Raw metadata appears one-row-per-SKU for the checked fields.",
        }
    )
    rows.append(
        {
            "metric": "recommended_handling",
            "value": "use_processed_product_properties_clean" if not processed_unstable else "review_or_deduplicate_before_join",
            "interpretation": (
                "Use deterministic SKU-level processed metadata for MVP features."
                if not processed_unstable
                else "Apply deterministic deduplication by SKU, mode category, robust price handling, or exclude unstable metadata features."
            ),
        }
    )
    stats["processed_unstable"] = processed_unstable
    return rows, stats


def bucket_expression(feature_name: str) -> F.Column:
    col = F.col(feature_name)
    if feature_name.startswith("days_since"):
        expr = F.when(col.isNull(), F.lit("null/no event"))
        for label, lower, upper in RECENCY_BUCKETS[1:]:
            condition = col >= F.lit(lower)
            if upper is not None:
                condition = condition & (col <= F.lit(upper))
            expr = expr.when(condition, F.lit(label))
        return expr.otherwise(F.lit("unbucketed"))
    expr = F.when(col.isNull(), F.lit("null/no value"))
    for label, lower, upper in COUNT_BUCKETS:
        condition = col >= F.lit(lower)
        if upper is not None:
            condition = condition & (col <= F.lit(upper))
        expr = expr.when(condition, F.lit(label))
    return expr.otherwise(F.lit("unbucketed"))


def bucket_interpretation(feature_name: str, bucket: str, positive_rate: float | None, baseline: float | None) -> str:
    if positive_rate is None or baseline is None:
        return "No rate available for this bucket."
    if positive_rate > baseline * 1.2:
        direction = "higher than baseline"
    elif positive_rate < baseline * 0.8:
        direction = "lower than baseline"
    else:
        direction = "near baseline"
    return f"{feature_name} bucket {bucket} has a positive rate {direction}; this is association, not causality."


def feature_target_relationship(training: DataFrame) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    baseline_row = training.agg(F.avg(F.col("label").cast("double")).alias("baseline_positive_rate")).collect()[0]
    baseline = float(baseline_row["baseline_positive_rate"] or 0.0)
    rows: list[dict[str, Any]] = []
    available_features = [feature for feature in FEATURES_TO_BUCKET if feature in training.columns]
    for feature in available_features:
        bucketed = training.select("label", F.col(feature)).withColumn("bucket", bucket_expression(feature))
        for row in bucketed.groupBy("bucket").agg(
            F.count(F.lit(1)).alias("row_count"),
            F.sum(F.col("label").cast("long")).alias("positive_count"),
        ).collect():
            row_count = int(row["row_count"] or 0)
            positive_count = int(row["positive_count"] or 0)
            positive_rate = positive_count / row_count if row_count else None
            rows.append(
                {
                    "feature_name": feature,
                    "bucket": row["bucket"],
                    "row_count": row_count,
                    "positive_count": positive_count,
                    "positive_rate": positive_rate,
                    "baseline_positive_rate": baseline,
                    "lift_vs_baseline": positive_rate / baseline if positive_rate is not None and baseline else None,
                    "interpretation": bucket_interpretation(feature, row["bucket"], positive_rate, baseline),
                }
            )

    def lift_for(feature: str, bucket: str) -> float | None:
        for row in rows:
            if row["feature_name"] == feature and row["bucket"] == bucket:
                return row["lift_vs_baseline"]
        return None

    stats = {
        "baseline_positive_rate": baseline,
        "available_feature_count": len(available_features),
        "add_to_cart_gt_50_lift": lift_for("add_to_cart_count", ">50"),
        "active_days_gt_50_lift": lift_for("active_days_count", ">50"),
        "product_buy_gt_50_lift": lift_for("product_buy_count", ">50"),
    }
    return rows, stats


def search_signal_summary(training: DataFrame) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    baseline = float(training.agg(F.avg(F.col("label").cast("double")).alias("baseline")).collect()[0]["baseline"] or 0.0)
    search_count_col = F.coalesce(F.col("search_query_count"), F.lit(0))
    segmented = training.withColumn(
        "segment",
        F.when(search_count_col == 0, F.lit("no_search"))
        .when(search_count_col <= 3, F.lit("low_search_1_3"))
        .when(search_count_col <= 10, F.lit("medium_search_4_10"))
        .otherwise(F.lit("high_search_gt_10")),
    )
    rows: list[dict[str, Any]] = []
    for row in segmented.groupBy("segment").agg(
        F.count(F.lit(1)).alias("client_count"),
        F.sum(F.col("label").cast("long")).alias("positive_count"),
    ).collect():
        client_count = int(row["client_count"] or 0)
        positive_count = int(row["positive_count"] or 0)
        positive_rate = positive_count / client_count if client_count else None
        lift = positive_rate / baseline if positive_rate is not None and baseline else None
        rows.append(
            {
                "segment": row["segment"],
                "client_count": client_count,
                "positive_count": positive_count,
                "positive_rate": positive_rate,
                "lift_vs_baseline": lift,
                "interpretation": (
                    "Search count segment is above baseline."
                    if lift and lift > 1.2
                    else "Search count segment is below baseline." if lift and lift < 0.8 else "Search count segment is near baseline."
                ),
            }
        )
    search_clients = sum(row["client_count"] for row in rows if row["segment"] != "no_search")
    high_search = next((row for row in rows if row["segment"] == "high_search_gt_10"), None)
    no_search = next((row for row in rows if row["segment"] == "no_search"), None)
    stats = {
        "search_clients": search_clients,
        "high_search_lift": high_search["lift_vs_baseline"] if high_search else None,
        "no_search_lift": no_search["lift_vs_baseline"] if no_search else None,
        "baseline_positive_rate": baseline,
    }
    return rows, stats


def write_summary(stats: dict[str, Any]) -> None:
    target_close_to_cart = (stats.get("current_extra_vs_cart_rate") or 0) < 0.05
    same_sku_rate = stats.get("target_purchase_same_sku_prior_cart_rate") or 0
    search_lift = stats.get("high_search_lift")
    search_signal = search_lift is not None and search_lift > 1.2
    metadata_safe = not stats.get("processed_unstable", False)

    recommendation = (
        "Keep the current target for the MVP, but describe it as active-client purchase propensity "
        "and review whether a cart-conversion framing is clearer."
        if target_close_to_cart
        else "Keep the current purchase propensity target because it includes a meaningful group beyond cart-only clients."
    )
    if search_signal:
        search_recommendation = "Search count shows lift in aggregate, but should remain a simple reviewed feature until query-free sequence features are evaluated."
    else:
        search_recommendation = "Search count appears weak or noisy as a standalone feature; keep it under review and avoid overclaiming its value."

    lines = [
        "# Target and Feature Assumption Validation EDA",
        "",
        f"Generated at date: {datetime.now(timezone.utc).date().isoformat()}",
        "",
        "This aggregate-only EDA validates business assumptions behind the current purchase propensity MVP. It does not retrain models, rescore users, modify API logic, or persist row-level examples.",
        "",
        "## Key Findings",
        "",
        f"1. Target-window buyers with prior cart history rate: {normalize_value(stats.get('buyers_with_prior_cart_rate'))}.",
        f"2. Target purchase events with prior same-SKU add_to_cart rate: {normalize_value(same_sku_rate)}.",
        f"3. Current cohort extra rate versus cart-only cohort: {normalize_value(stats.get('current_extra_vs_cart_rate'))}.",
        f"4. Processed product metadata is {'stable at SKU level for MVP joins' if metadata_safe else 'not stable enough without further SKU deduplication review'}.",
        f"5. Feature bucket artifacts show directional associations only; they do not establish causality.",
        f"6. Search signal recommendation: {search_recommendation}",
        "",
        "## Recommendation",
        "",
        recommendation,
        "",
        "Recommended next actions for mentor review:",
        "",
        "- Decide whether the target name should remain purchase propensity or be reframed as active-client purchase propensity.",
        "- Keep target-window behavior out of features.",
        "- Keep SKU metadata features only if using deduplicated processed metadata.",
        "- Consider sequence-based cart-to-purchase features in a later iteration.",
        "- Treat search count as an aggregate signal, not as proof of intent.",
        "",
        "## Privacy",
        "",
        "Artifacts contain aggregate counts, rates, and interpretations only. No raw client IDs, query text, product names, row-level samples, row-level predictions, or local environment paths are written.",
        "",
    ]
    SUMMARY_MD_PATH.write_text("\n".join(lines), encoding="utf-8")


def read_parquet_pandas(path: Path, columns: list[str]) -> pd.DataFrame:
    dataset = ds.dataset(str(path), format="parquet")
    return dataset.to_table(columns=columns).to_pandas()


def pandas_event(processed_base: Path, table_name: str, columns: list[str]) -> pd.DataFrame:
    frame = read_parquet_pandas(processed_base / "events" / table_name, columns)
    if "event_ts" in frame.columns:
        frame["event_ts"] = pd.to_datetime(frame["event_ts"], errors="coerce")
    return frame.dropna(subset=["client_id"])


def pandas_history(frame: pd.DataFrame, cutoff: pd.Timestamp) -> pd.DataFrame:
    return frame[frame["event_ts"] < cutoff].copy()


def pandas_target(frame: pd.DataFrame, cutoff: pd.Timestamp, target_end: pd.Timestamp) -> pd.DataFrame:
    return frame[(frame["event_ts"] >= cutoff) & (frame["event_ts"] < target_end + pd.Timedelta(days=1))].copy()


def pandas_rate_row(metric: str, value: Any, denominator: Any, interpretation: str) -> dict[str, Any]:
    return {
        "metric": metric,
        "value": normalize_value(value),
        "denominator": normalize_value(denominator),
        "rate": ratio(value, denominator) if isinstance(value, (int, float)) and isinstance(denominator, (int, float)) else None,
        "interpretation": interpretation,
    }


def pandas_purchase_path_summary(
    add_to_cart: pd.DataFrame, product_buy: pd.DataFrame, cutoff: pd.Timestamp, target_end: pd.Timestamp
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    history_cart = pandas_history(add_to_cart[["client_id", "sku", "event_ts"]], cutoff)
    target_purchase = pandas_target(product_buy[["client_id", "sku", "event_ts"]], cutoff, target_end)

    target_buyers = set(target_purchase["client_id"].dropna().unique())
    cart_clients = set(history_cart["client_id"].dropna().unique())
    target_buyer_count = len(target_buyers)
    buyers_with_prior_cart = len(target_buyers.intersection(cart_clients))
    buyers_without_prior_cart = target_buyer_count - buyers_with_prior_cart
    target_purchase_count = int(len(target_purchase))

    latest_cart_by_key = (
        history_cart.dropna(subset=["sku", "event_ts"])
        .groupby(["client_id", "sku"], as_index=False)["event_ts"]
        .max()
        .rename(columns={"event_ts": "latest_prior_cart_ts"})
    )
    target_with_cart = target_purchase.merge(latest_cart_by_key, on=["client_id", "sku"], how="left")
    purchases_with_prior_same_sku = int(target_with_cart["latest_prior_cart_ts"].notna().sum())

    first_target = (
        target_purchase.dropna(subset=["sku", "event_ts"])
        .groupby(["client_id", "sku"], as_index=False)["event_ts"]
        .min()
        .rename(columns={"event_ts": "first_purchase_ts"})
    )
    first_with_cart = first_target.merge(latest_cart_by_key, on=["client_id", "sku"], how="left")
    first_with_cart_prior = first_with_cart[first_with_cart["latest_prior_cart_ts"].notna()].copy()
    buyers_with_same_sku_cart_before_first = int(first_with_cart_prior["client_id"].nunique())
    if not first_with_cart_prior.empty:
        delay_days = (
            first_with_cart_prior["first_purchase_ts"].dt.normalize()
            - first_with_cart_prior["latest_prior_cart_ts"].dt.normalize()
        ).dt.days
        delay_count = int(delay_days.count())
        min_delay = int(delay_days.min())
        median_delay = float(delay_days.quantile(0.5))
        p75_delay = float(delay_days.quantile(0.75))
        p90_delay = float(delay_days.quantile(0.9))
        max_delay = int(delay_days.max())
    else:
        delay_count = 0
        min_delay = median_delay = p75_delay = p90_delay = max_delay = None

    rows = [
        pandas_rate_row("target_window_buyers", target_buyer_count, target_buyer_count, "Distinct clients with at least one purchase in the target window."),
        pandas_rate_row("target_window_buyers_with_add_to_cart_before_cutoff", buyers_with_prior_cart, target_buyer_count, "Share of target buyers already represented in the cart-history cohort."),
        pandas_rate_row("target_window_buyers_without_add_to_cart_before_cutoff", buyers_without_prior_cart, target_buyer_count, "Target buyers missed by a cart-only cohort before cutoff."),
        pandas_rate_row("target_window_buyers_with_prior_same_sku_cart_before_first_purchase", buyers_with_same_sku_cart_before_first, target_buyer_count, "Clients whose target purchase path includes same-SKU cart history before first target purchase."),
        pandas_rate_row("target_purchase_events", target_purchase_count, target_purchase_count, "Target-window purchase events considered for same-SKU cart path validation."),
        pandas_rate_row("target_purchase_events_with_prior_same_sku_add_to_cart", purchases_with_prior_same_sku, target_purchase_count, "Share of target purchase events with a prior same-SKU add_to_cart before cutoff."),
        pandas_rate_row("same_sku_cart_to_first_purchase_delay_min_days", min_delay, delay_count, "Minimum delay among first target purchases with prior same-SKU cart history."),
        pandas_rate_row("same_sku_cart_to_first_purchase_delay_median_days", median_delay, delay_count, "Approximate median delay from latest prior same-SKU cart to first target purchase."),
        pandas_rate_row("same_sku_cart_to_first_purchase_delay_p75_days", p75_delay, delay_count, "Approximate p75 delay from latest prior same-SKU cart to first target purchase."),
        pandas_rate_row("same_sku_cart_to_first_purchase_delay_p90_days", p90_delay, delay_count, "Approximate p90 delay from latest prior same-SKU cart to first target purchase."),
        pandas_rate_row("same_sku_cart_to_first_purchase_delay_max_days", max_delay, delay_count, "Maximum delay among first target purchases with prior same-SKU cart history."),
    ]
    for row in rows:
        if row["metric"].startswith("same_sku_cart_to_first_purchase_delay"):
            row["rate"] = None
    stats = {
        "target_buyer_count": target_buyer_count,
        "buyers_with_prior_cart_rate": ratio(buyers_with_prior_cart, target_buyer_count),
        "buyers_without_prior_cart_rate": ratio(buyers_without_prior_cart, target_buyer_count),
        "target_purchase_same_sku_prior_cart_rate": ratio(purchases_with_prior_same_sku, target_purchase_count),
        "same_sku_delay_median_days": normalize_value(median_delay),
    }
    return rows, stats


def pandas_cohort_overlap_summary(
    add_to_cart: pd.DataFrame,
    product_buy: pd.DataFrame,
    search_query: pd.DataFrame | None,
    features: pd.DataFrame,
    cutoff: pd.Timestamp,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    cart_clients = set(pandas_history(add_to_cart[["client_id", "event_ts"]], cutoff)["client_id"].dropna().unique())
    buyer_clients = set(pandas_history(product_buy[["client_id", "event_ts"]], cutoff)["client_id"].dropna().unique())
    search_clients = (
        set(pandas_history(search_query[["client_id", "event_ts"]], cutoff)["client_id"].dropna().unique())
        if search_query is not None
        else set()
    )
    current_clients = set(features.loc[features["is_eligible_purchase_propensity"] == 1, "client_id"].dropna().unique())

    cart_count = len(cart_clients)
    buyer_count = len(buyer_clients)
    search_count = len(search_clients)
    current_count = len(current_clients)
    buyer_without_cart_count = len(buyer_clients - cart_clients)
    search_only_count = len(search_clients - current_clients)
    intersection = len(cart_clients & buyer_clients)
    union = len(cart_clients | buyer_clients)
    current_extra = current_count - cart_count

    rows = [
        {"cohort_name": "add_to_cart_before_cutoff", "client_count": cart_count, "relation_to_current_target": "cart conversion cohort", "interpretation": "Clients with cart intent before cutoff."},
        {"cohort_name": "product_buy_before_cutoff", "client_count": buyer_count, "relation_to_current_target": "purchase history component", "interpretation": "Clients with purchase history before cutoff."},
        {"cohort_name": "search_query_before_cutoff", "client_count": search_count, "relation_to_current_target": "candidate broader future cohort", "interpretation": "Clients with search activity before cutoff."},
        {"cohort_name": "current_add_to_cart_or_product_buy_before_cutoff", "client_count": current_count, "relation_to_current_target": "current eligible cohort", "interpretation": "Current purchase propensity cohort used by Phase 4 labels."},
        {"cohort_name": "cart_conversion_cohort", "client_count": cart_count, "relation_to_current_target": "subset or near-equivalent comparator", "interpretation": "Cart-only eligible cohort for cart conversion framing."},
        {"cohort_name": "buyers_without_cart_before_cutoff", "client_count": buyer_without_cart_count, "relation_to_current_target": "extra clients beyond cart cohort", "interpretation": f"Rate among current cohort: {ratio(buyer_without_cart_count, current_count)}."},
        {"cohort_name": "search_only_before_cutoff", "client_count": search_only_count, "relation_to_current_target": "not included in current target", "interpretation": f"Search clients outside current cohort; candidate for broader future target. Rate among search clients: {ratio(search_only_count, search_count)}."},
        {"cohort_name": "add_to_cart_product_buy_jaccard_overlap", "client_count": ratio(intersection, union), "relation_to_current_target": "derived overlap metric", "interpretation": "Jaccard overlap between cart-history and purchase-history clients."},
        {"cohort_name": "buyers_without_cart_rate", "client_count": ratio(buyer_without_cart_count, current_count), "relation_to_current_target": "derived metric", "interpretation": "Share of current cohort that would be excluded by cart-only eligibility."},
        {"cohort_name": "current_vs_cart_cohort_extra_clients", "client_count": current_extra, "relation_to_current_target": "derived metric", "interpretation": "Additional clients in current cohort compared with cart conversion cohort."},
        {"cohort_name": "current_vs_cart_cohort_extra_rate", "client_count": ratio(current_extra, current_count), "relation_to_current_target": "derived metric", "interpretation": "Share of current cohort added by including purchase-history clients without cart history."},
    ]
    stats = {
        "cart_count": cart_count,
        "buyer_count": buyer_count,
        "current_count": current_count,
        "buyer_without_cart_count": buyer_without_cart_count,
        "buyer_without_cart_rate": ratio(buyer_without_cart_count, current_count),
        "current_extra_vs_cart": current_extra,
        "current_extra_vs_cart_rate": ratio(current_extra, current_count),
        "jaccard_overlap": ratio(intersection, union),
        "search_only_count": search_only_count,
    }
    return rows, stats


def pandas_metadata_profile(frame: pd.DataFrame, label: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    grouped = frame[["sku", "category", "price"]].groupby("sku", dropna=False).agg(
        rows_per_sku=("sku", "size"),
        distinct_category_count=("category", "nunique"),
        distinct_price_count=("price", "nunique"),
    )
    pair_counts = frame.assign(_pair=list(zip(frame["category"], frame["price"]))).groupby("sku")["_pair"].nunique()
    grouped["distinct_category_price_pair_count"] = pair_counts
    rows = [
        {"metric": f"{label}_row_count", "value": int(len(frame)), "interpretation": f"Rows in {label} product metadata."},
        {"metric": f"{label}_distinct_sku_count", "value": int(frame["sku"].nunique()), "interpretation": f"Distinct SKUs in {label} product metadata."},
        {"metric": f"{label}_sku_with_multiple_rows", "value": int((grouped["rows_per_sku"] > 1).sum()), "interpretation": "Nonzero values indicate possible join row expansion if joined without SKU deduplication."},
        {"metric": f"{label}_sku_with_multiple_category_values", "value": int((grouped["distinct_category_count"] > 1).sum()), "interpretation": "Nonzero values indicate category instability for SKU-level features."},
        {"metric": f"{label}_sku_with_multiple_price_values", "value": int((grouped["distinct_price_count"] > 1).sum()), "interpretation": "Nonzero values indicate price instability for SKU-level features."},
        {"metric": f"{label}_sku_with_multiple_category_price_pairs", "value": int((grouped["distinct_category_price_pair_count"] > 1).sum()), "interpretation": "Nonzero values indicate category-price pair instability."},
        {"metric": f"{label}_max_rows_per_sku", "value": int(grouped["rows_per_sku"].max()), "interpretation": "Maximum metadata rows for one SKU; greater than 1 can expand event joins."},
    ]
    return rows, {row["metric"]: row["value"] for row in rows}


def pandas_product_metadata_consistency(processed_base: Path, raw_base: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    stats: dict[str, Any] = {}
    raw_path = raw_base / "product_properties.parquet"
    processed_path = processed_base / "product_properties_clean"
    if raw_path.exists():
        raw_rows, raw_stats = pandas_metadata_profile(read_parquet_pandas(raw_path, ["sku", "category", "price"]), "raw")
        rows.extend(raw_rows)
        stats.update(raw_stats)
    if processed_path.exists():
        processed_rows, processed_stats = pandas_metadata_profile(read_parquet_pandas(processed_path, ["sku", "category", "price"]), "processed")
        rows.extend(processed_rows)
        stats.update(processed_stats)
    raw_unstable = int(stats.get("raw_sku_with_multiple_rows", 0) or 0) > 0
    processed_unstable = int(stats.get("processed_sku_with_multiple_rows", 0) or 0) > 0
    rows.append({"metric": "metadata_join_row_expansion_risk", "value": "yes" if raw_unstable else "no", "interpretation": "Raw metadata can expand event joins when SKU has multiple metadata rows." if raw_unstable else "Raw metadata appears one-row-per-SKU for the checked fields."})
    rows.append({"metric": "recommended_handling", "value": "use_processed_product_properties_clean" if not processed_unstable else "review_or_deduplicate_before_join", "interpretation": "Use deterministic SKU-level processed metadata for MVP features." if not processed_unstable else "Apply deterministic deduplication by SKU, mode category, robust price handling, or exclude unstable metadata features."})
    stats["processed_unstable"] = processed_unstable
    return rows, stats


def pandas_feature_bucket(series: pd.Series, feature_name: str) -> pd.Series:
    if feature_name.startswith("days_since"):
        result = pd.Series("unbucketed", index=series.index, dtype="object")
        result[series.isna()] = "null/no event"
        result[(series >= 0) & (series <= 7)] = "0-7 days"
        result[(series >= 8) & (series <= 30)] = "8-30 days"
        result[(series >= 31) & (series <= 60)] = "31-60 days"
        result[series > 60] = ">60 days"
        return result
    if "ratio" in feature_name:
        result = pd.Series("unbucketed", index=series.index, dtype="object")
        result[series.isna()] = "null/no value"
        result[series == 0] = "0"
        result[(series > 0) & (series <= 0.25)] = "0-0.25"
        result[(series > 0.25) & (series <= 0.5)] = "0.25-0.5"
        result[(series > 0.5) & (series <= 1.0)] = "0.5-1.0"
        result[series > 1.0] = ">1.0"
        return result
    result = pd.Series("unbucketed", index=series.index, dtype="object")
    result[series.isna()] = "null/no value"
    result[series == 0] = "0"
    result[series == 1] = "1"
    result[(series >= 2) & (series <= 3)] = "2-3"
    result[(series >= 4) & (series <= 10)] = "4-10"
    result[(series >= 11) & (series <= 50)] = "11-50"
    result[series > 50] = ">50"
    return result


def pandas_feature_target_relationship(training: pd.DataFrame) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    baseline = float(training["label"].mean())
    rows: list[dict[str, Any]] = []
    available_features = [feature for feature in FEATURES_TO_BUCKET if feature in training.columns]
    for feature in available_features:
        buckets = pandas_feature_bucket(training[feature], feature)
        grouped = training.assign(_bucket=buckets).groupby("_bucket", dropna=False)["label"].agg(["count", "sum"])
        for bucket, values in grouped.iterrows():
            row_count = int(values["count"])
            positive_count = int(values["sum"])
            positive_rate = positive_count / row_count if row_count else None
            rows.append({
                "feature_name": feature,
                "bucket": bucket,
                "row_count": row_count,
                "positive_count": positive_count,
                "positive_rate": positive_rate,
                "baseline_positive_rate": baseline,
                "lift_vs_baseline": positive_rate / baseline if positive_rate is not None and baseline else None,
                "interpretation": bucket_interpretation(feature, bucket, positive_rate, baseline),
            })

    def lift_for(feature: str, bucket: str) -> float | None:
        for row in rows:
            if row["feature_name"] == feature and row["bucket"] == bucket:
                return row["lift_vs_baseline"]
        return None

    return rows, {
        "baseline_positive_rate": baseline,
        "available_feature_count": len(available_features),
        "add_to_cart_gt_50_lift": lift_for("add_to_cart_count", ">50"),
        "active_days_gt_50_lift": lift_for("active_days_count", ">50"),
        "product_buy_gt_50_lift": lift_for("product_buy_count", ">50"),
    }


def pandas_search_signal_summary(training: pd.DataFrame) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    baseline = float(training["label"].mean())
    counts = training["search_query_count"].fillna(0)
    segments = pd.Series("high_search_gt_10", index=training.index, dtype="object")
    segments[counts == 0] = "no_search"
    segments[(counts >= 1) & (counts <= 3)] = "low_search_1_3"
    segments[(counts >= 4) & (counts <= 10)] = "medium_search_4_10"
    rows: list[dict[str, Any]] = []
    grouped = training.assign(_segment=segments).groupby("_segment")["label"].agg(["count", "sum"])
    for segment, values in grouped.iterrows():
        client_count = int(values["count"])
        positive_count = int(values["sum"])
        positive_rate = positive_count / client_count if client_count else None
        lift = positive_rate / baseline if positive_rate is not None and baseline else None
        rows.append({
            "segment": segment,
            "client_count": client_count,
            "positive_count": positive_count,
            "positive_rate": positive_rate,
            "lift_vs_baseline": lift,
            "interpretation": "Search count segment is above baseline." if lift and lift > 1.2 else "Search count segment is below baseline." if lift and lift < 0.8 else "Search count segment is near baseline.",
        })
    high = next((row for row in rows if row["segment"] == "high_search_gt_10"), None)
    no_search = next((row for row in rows if row["segment"] == "no_search"), None)
    return rows, {
        "search_clients": sum(row["client_count"] for row in rows if row["segment"] != "no_search"),
        "high_search_lift": high["lift_vs_baseline"] if high else None,
        "no_search_lift": no_search["lift_vs_baseline"] if no_search else None,
        "baseline_positive_rate": baseline,
    }


def run_pyarrow_eda(processed_base: Path, raw_base: Path, cutoff_date: str, target_end: str) -> dict[str, Any]:
    cutoff = pd.Timestamp(cutoff_date)
    target_end_ts = pd.Timestamp(target_end)
    add_to_cart = pandas_event(processed_base, "add_to_cart", ["client_id", "sku", "event_ts"])
    product_buy = pandas_event(processed_base, "product_buy", ["client_id", "sku", "event_ts"])
    search_path = processed_base / "events" / "search_query"
    search_query = pandas_event(processed_base, "search_query", ["client_id", "event_ts"]) if search_path.exists() else None
    features = read_parquet_pandas(processed_base / "features" / "user_behavior_features", ["client_id", "is_eligible_purchase_propensity"])
    training_columns = ["label"] + [feature for feature in FEATURES_TO_BUCKET]
    training = read_parquet_pandas(processed_base / "training" / "purchase_propensity_30d", training_columns)

    purchase_rows, purchase_stats = pandas_purchase_path_summary(add_to_cart, product_buy, cutoff, target_end_ts)
    cohort_rows, cohort_stats = pandas_cohort_overlap_summary(add_to_cart, product_buy, search_query, features, cutoff)
    metadata_rows, metadata_stats = pandas_product_metadata_consistency(processed_base, raw_base)
    feature_rows, feature_stats = pandas_feature_target_relationship(training)
    search_rows, search_stats = pandas_search_signal_summary(training)
    combined_stats = {
        **purchase_stats,
        **cohort_stats,
        **metadata_stats,
        **feature_stats,
        **search_stats,
        "engine_used": "pyarrow",
    }

    write_csv(PURCHASE_PATH_PATH, purchase_rows, ["metric", "value", "denominator", "rate", "interpretation"])
    write_csv(COHORT_OVERLAP_PATH, cohort_rows, ["cohort_name", "client_count", "relation_to_current_target", "interpretation"])
    write_csv(PRODUCT_METADATA_PATH, metadata_rows, ["metric", "value", "interpretation"])
    write_csv(FEATURE_TARGET_PATH, feature_rows, ["feature_name", "bucket", "row_count", "positive_count", "positive_rate", "baseline_positive_rate", "lift_vs_baseline", "interpretation"])
    write_csv(SEARCH_SIGNAL_PATH, search_rows, ["segment", "client_count", "positive_count", "positive_rate", "lift_vs_baseline", "interpretation"])
    write_summary(combined_stats)
    return combined_stats


def write_all_spark_outputs(
    purchase_rows: list[dict[str, Any]],
    cohort_rows: list[dict[str, Any]],
    metadata_rows: list[dict[str, Any]],
    feature_rows: list[dict[str, Any]],
    search_rows: list[dict[str, Any]],
    combined_stats: dict[str, Any],
) -> None:
    write_csv(PURCHASE_PATH_PATH, purchase_rows, ["metric", "value", "denominator", "rate", "interpretation"])
    write_csv(COHORT_OVERLAP_PATH, cohort_rows, ["cohort_name", "client_count", "relation_to_current_target", "interpretation"])
    write_csv(PRODUCT_METADATA_PATH, metadata_rows, ["metric", "value", "interpretation"])
    write_csv(FEATURE_TARGET_PATH, feature_rows, ["feature_name", "bucket", "row_count", "positive_count", "positive_rate", "baseline_positive_rate", "lift_vs_baseline", "interpretation"])
    write_csv(SEARCH_SIGNAL_PATH, search_rows, ["segment", "client_count", "positive_count", "positive_rate", "lift_vs_baseline", "interpretation"])
    write_summary(combined_stats)


def main() -> int:
    args = parse_args()
    config = read_simple_yaml(resolve_repo_path(args.config))
    processed_base = resolve_repo_path(args.processed_base)
    raw_base = resolve_repo_path(args.raw_base)
    target_config = config["target"]
    cutoff_date = str(target_config["cutoff_date"])
    target_end = str(target_config["target_end"])

    if args.engine == "pyarrow":
        stats = run_pyarrow_eda(processed_base, raw_base, cutoff_date, target_end)
        print("Target and feature assumption validation EDA completed.")
        print(f"Execution engine: {stats['engine_used']}")
        print(f"Wrote artifacts under {relative_path(ARTIFACT_DIR)}")
        print(f"Target buyers: {stats['target_buyer_count']}")
        print(f"Current cohort: {stats['current_count']}")
        print(f"Baseline positive rate: {normalize_value(stats['baseline_positive_rate'])}")
        return 0

    spark = start_spark()
    training: DataFrame | None = None
    try:
        add_to_cart = read_event(processed_base, "add_to_cart", spark)
        product_buy = read_event(processed_base, "product_buy", spark)
        search_path = processed_base / "events" / "search_query"
        search_query = read_event(processed_base, "search_query", spark) if search_path.exists() else None
        features = spark.read.parquet(str(processed_base / "features" / "user_behavior_features"))
        training = spark.read.parquet(str(processed_base / "training" / "purchase_propensity_30d")).cache()

        purchase_rows, purchase_stats = purchase_path_summary(add_to_cart, product_buy, cutoff_date, target_end)
        cohort_rows, cohort_stats = cohort_overlap_summary(add_to_cart, product_buy, search_query, features, cutoff_date)
        metadata_rows, metadata_stats = product_metadata_consistency(spark, processed_base, raw_base)
        feature_rows, feature_stats = feature_target_relationship(training)
        search_rows, search_stats = search_signal_summary(training)

        combined_stats = {
            **purchase_stats,
            **cohort_stats,
            **metadata_stats,
            **feature_stats,
            **search_stats,
            "engine_used": "spark",
        }
        write_all_spark_outputs(purchase_rows, cohort_rows, metadata_rows, feature_rows, search_rows, combined_stats)

        print("Target and feature assumption validation EDA completed.")
        print(f"Execution engine: {combined_stats['engine_used']}")
        print(f"Wrote artifacts under {relative_path(ARTIFACT_DIR)}")
        print(f"Target buyers: {purchase_stats['target_buyer_count']}")
        print(f"Current cohort: {cohort_stats['current_count']}")
        print(f"Baseline positive rate: {normalize_value(feature_stats['baseline_positive_rate'])}")
        return 0
    except Exception:
        if args.engine == "spark":
            raise
        spark.stop()
        stats = run_pyarrow_eda(processed_base, raw_base, cutoff_date, target_end)
        print("Spark local Parquet read failed; completed aggregate EDA with PyArrow fallback.")
        print(f"Execution engine: {stats['engine_used']}")
        print(f"Wrote artifacts under {relative_path(ARTIFACT_DIR)}")
        print(f"Target buyers: {stats['target_buyer_count']}")
        print(f"Current cohort: {stats['current_count']}")
        print(f"Baseline positive rate: {normalize_value(stats['baseline_positive_rate'])}")
        return 0
    finally:
        try:
            if training is not None:
                training.unpersist()
        except Exception:
            pass
        try:
            spark.stop()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
