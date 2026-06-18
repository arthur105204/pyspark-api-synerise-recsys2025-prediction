"""Train Baseline V2-3b with minimal search-to-cart transition features.

This job tests whether pre-cutoff search-to-cart progression adds signal beyond
Baseline V2-2 raw search activity. It uses the same E1 temporal train/validation
snapshots, Logistic Regression class, median imputation, class weighting, and
TopK evaluation pattern as E1/V2-1/V2-2.

It adds only three aggregate transition features:

- search_before_cart_count
- search_to_cart_rate
- recent_search_then_cart_flag

It does not add query text features, embeddings, session features, trend
features, category transitions, broad search redesign, label changes, temporal
split changes, calibration, hyperparameter tuning, or a new model class.
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
V21_JOB_PATH = PROJECT_ROOT / "jobs" / "05g_train_baseline_v21.py"
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "baseline_v23b_features.json"
DEFAULT_TRAIN_INPUT = (
    PROJECT_ROOT / "data" / "processed" / "training" / "e1_train_2022_10_10" / "purchase_propensity_30d"
)
DEFAULT_VALIDATION_INPUT = (
    PROJECT_ROOT / "data" / "processed" / "training" / "e1_valid_2022_11_09" / "purchase_propensity_30d"
)
DEFAULT_SEARCH_INPUT = PROJECT_ROOT / "data" / "processed" / "events" / "search_query"
DEFAULT_CART_INPUT = PROJECT_ROOT / "data" / "processed" / "events" / "add_to_cart"
DEFAULT_MODEL_OUTPUT = PROJECT_ROOT / "data" / "models" / "purchase_propensity_baseline_v23b"
DEFAULT_ARTIFACT_DIR = PROJECT_ROOT / "artifacts" / "modeling" / "baseline_v2"
DEFAULT_MAX_ITER = 20
TRAIN_CUTOFF_DATE = "2022-10-10"
VALIDATION_CUTOFF_DATE = "2022-11-09"
SEARCH_BEFORE_CART_WINDOW_DAYS = 7
RECENT_SEARCH_WINDOW_DAYS = 30
BASELINE_E1_METRICS = {
    "roc_auc": 0.835559,
    "pr_auc": 0.253155,
    "precision_at_1pct": 0.478742,
    "precision_at_5pct": 0.294837,
    "precision_at_10pct": 0.213922,
    "lift_at_1pct": 10.994062,
    "lift_at_5pct": 6.770770,
    "lift_at_10pct": 4.912611,
}
BASELINE_V21_METRICS = {
    "roc_auc": 0.834273,
    "pr_auc": 0.254871,
    "precision_at_1pct": 0.483766,
    "precision_at_5pct": 0.296102,
    "precision_at_10pct": 0.216174,
    "lift_at_1pct": 11.109429,
    "lift_at_5pct": 6.799825,
    "lift_at_10pct": 4.964312,
}
BASELINE_V22_METRICS = {
    "roc_auc": 0.834208,
    "pr_auc": 0.255374,
    "precision_at_1pct": 0.484882,
    "precision_at_5pct": 0.296158,
    "precision_at_10pct": 0.216155,
    "lift_at_1pct": 11.135066,
    "lift_at_5pct": 6.801107,
    "lift_at_10pct": 4.963885,
}


def load_v21_module() -> Any:
    spec = importlib.util.spec_from_file_location("baseline_v21_job", V21_JOB_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load Baseline V2-1 helper module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


V21 = load_v21_module()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train Baseline V2-3b temporal model.")
    parser.add_argument(
        "--feature-config",
        default=DEFAULT_CONFIG_PATH.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative Baseline V2-3b feature config JSON path.",
    )
    parser.add_argument(
        "--train-input",
        default=DEFAULT_TRAIN_INPUT.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative E1 temporal train snapshot dataset path.",
    )
    parser.add_argument(
        "--validation-input",
        default=DEFAULT_VALIDATION_INPUT.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative E1 temporal validation snapshot dataset path.",
    )
    parser.add_argument(
        "--search-input",
        default=DEFAULT_SEARCH_INPUT.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative processed search_query event path.",
    )
    parser.add_argument(
        "--cart-input",
        default=DEFAULT_CART_INPUT.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative processed add_to_cart event path.",
    )
    parser.add_argument(
        "--model-output",
        default=DEFAULT_MODEL_OUTPUT.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative V2-3b Spark ML model output path.",
    )
    parser.add_argument(
        "--artifact-dir",
        default=DEFAULT_ARTIFACT_DIR.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative Baseline v2 artifact directory.",
    )
    parser.add_argument("--max-iter", type=int, default=DEFAULT_MAX_ITER, help="Logistic Regression max iterations.")
    return parser.parse_args()


def start_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("train-baseline-v23b")
        .master("local[*]")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def normalize_value(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    return value


def safe_divide(numerator: float, denominator: float) -> float:
    return float(numerator) / float(denominator) if denominator else 0.0


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: normalize_value(value) for key, value in row.items()})


def filter_event_history(df: DataFrame, cutoff_date: str) -> DataFrame:
    return df.where(F.col("event_date") < F.to_date(F.lit(cutoff_date)))


def transition_features_for_cutoff(search_events: DataFrame, cart_events: DataFrame, cutoff_date: str) -> DataFrame:
    search_history = (
        filter_event_history(search_events, cutoff_date)
        .select(F.col("client_id"), F.col("event_ts").alias("search_ts"))
        .where(F.col("client_id").isNotNull() & F.col("search_ts").isNotNull())
        .dropDuplicates(["client_id", "search_ts"])
    )
    cart_history = (
        filter_event_history(cart_events, cutoff_date)
        .select(F.col("client_id"), F.col("event_ts").alias("cart_ts"))
        .where(F.col("client_id").isNotNull() & F.col("cart_ts").isNotNull())
        .withColumn("cart_event_id", F.monotonically_increasing_id())
    )
    cart_counts = cart_history.groupBy("client_id").agg(F.count(F.lit(1)).alias("cart_event_count_for_rate"))

    transition_condition = (
        (F.col("c.client_id") == F.col("s.client_id"))
        & (F.col("s.search_ts") <= F.col("c.cart_ts"))
        & (F.col("s.search_ts") >= F.col("c.cart_ts") - F.expr(f"INTERVAL {SEARCH_BEFORE_CART_WINDOW_DAYS} DAYS"))
    )
    search_assisted_carts = (
        cart_history.alias("c")
        .join(search_history.alias("s"), transition_condition, "left_semi")
        .groupBy("client_id")
        .agg(F.count(F.lit(1)).alias("search_before_cart_count"))
    )

    recent_cutoff_condition = (
        (F.col("c.client_id") == F.col("s.client_id"))
        & (F.col("s.search_ts") <= F.col("c.cart_ts"))
        & (F.to_date(F.col("s.search_ts")) >= F.date_sub(F.to_date(F.lit(cutoff_date)), RECENT_SEARCH_WINDOW_DAYS))
    )
    recent_search_then_cart = (
        cart_history.alias("c")
        .join(search_history.alias("s"), recent_cutoff_condition, "left_semi")
        .select("client_id")
        .distinct()
        .withColumn("recent_search_then_cart_flag", F.lit(1))
    )

    return (
        cart_counts.join(search_assisted_carts, "client_id", "left")
        .join(recent_search_then_cart, "client_id", "left")
        .fillna({"search_before_cart_count": 0, "recent_search_then_cart_flag": 0})
        .withColumn(
            "search_to_cart_rate",
            (F.col("search_before_cart_count").cast("double") + F.lit(1.0))
            / (F.col("cart_event_count_for_rate").cast("double") + F.lit(2.0)),
        )
        .select(
            "client_id",
            F.col("search_before_cart_count").cast("long"),
            F.col("search_to_cart_rate").cast("double"),
            F.col("recent_search_then_cart_flag").cast("int"),
        )
    )


def add_transition_features(base_df: DataFrame, transition_df: DataFrame) -> DataFrame:
    return (
        base_df.join(transition_df, "client_id", "left")
        .fillna(
            {
                "search_before_cart_count": 0,
                "search_to_cart_rate": 0.0,
                "recent_search_then_cart_flag": 0,
            }
        )
        .withColumn("search_before_cart_count", F.col("search_before_cart_count").cast("long"))
        .withColumn("search_to_cart_rate", F.col("search_to_cart_rate").cast("double"))
        .withColumn("recent_search_then_cart_flag", F.col("recent_search_then_cart_flag").cast("int"))
    )


def feature_stats(df: DataFrame, feature_name: str, row_count: int) -> dict[str, Any]:
    row = df.agg(
        F.count(F.col(feature_name)).alias("non_null_count"),
        F.approx_count_distinct(F.col(feature_name)).alias("distinct_count"),
        F.mean(F.col(feature_name).cast("double")).alias("mean"),
        F.stddev(F.col(feature_name).cast("double")).alias("stddev"),
        F.min(F.col(feature_name)).alias("min"),
        F.max(F.col(feature_name)).alias("max"),
        F.sum(F.when(F.col(feature_name) == 0, F.lit(1)).otherwise(F.lit(0))).alias("zero_count"),
    ).collect()[0].asDict()
    quantiles = df.approxQuantile(feature_name, [0.5, 0.9, 0.95, 0.99], 0.001)
    non_null_count = int(row["non_null_count"] or 0)
    zero_count = int(row["zero_count"] or 0)
    return {
        "feature_name": feature_name,
        "non_null_count": non_null_count,
        "non_null_rate": safe_divide(non_null_count, row_count),
        "distinct_count": int(row["distinct_count"] or 0),
        "mean": row["mean"],
        "stddev": row["stddev"],
        "min": row["min"],
        "p50": quantiles[0] if len(quantiles) > 0 else None,
        "p90": quantiles[1] if len(quantiles) > 1 else None,
        "p95": quantiles[2] if len(quantiles) > 2 else None,
        "p99": quantiles[3] if len(quantiles) > 3 else None,
        "max": row["max"],
        "zero_count": zero_count,
        "zero_rate": safe_divide(zero_count, row_count),
    }


def transition_positive_rate_rows(df: DataFrame, feature_name: str) -> list[dict[str, Any]]:
    if feature_name == "search_before_cart_count":
        bucket_col = (
            F.when(F.col(feature_name) <= 0, F.lit("0"))
            .when(F.col(feature_name) == 1, F.lit("1"))
            .when(F.col(feature_name) <= 3, F.lit("2-3"))
            .when(F.col(feature_name) <= 10, F.lit("4-10"))
            .otherwise(F.lit(">10"))
        )
    elif feature_name == "search_to_cart_rate":
        bucket_col = (
            F.when(F.col(feature_name) <= 0, F.lit("0"))
            .when(F.col(feature_name) <= 0.25, F.lit("(0,0.25]"))
            .when(F.col(feature_name) <= 0.50, F.lit("(0.25,0.50]"))
            .when(F.col(feature_name) <= 0.75, F.lit("(0.50,0.75]"))
            .otherwise(F.lit("(0.75,1.0]"))
        )
    else:
        bucket_col = F.col(feature_name).cast("string")
    rows = (
        df.withColumn("bucket", bucket_col)
        .groupBy("bucket")
        .agg(F.count(F.lit(1)).alias("row_count"), F.sum(F.col("label").cast("long")).alias("positive_count"))
        .orderBy("bucket")
        .collect()
    )
    result = []
    for row in rows:
        row_count = int(row["row_count"] or 0)
        positive_count = int(row["positive_count"] or 0)
        result.append(
            {
                "feature_name": feature_name,
                "bucket": row["bucket"],
                "row_count": row_count,
                "positive_count": positive_count,
                "positive_rate": safe_divide(positive_count, row_count),
            }
        )
    return result


def metric_delta(candidate: float, baseline: float) -> float:
    return safe_divide(candidate - baseline, baseline)


def performance_decision(metrics: dict[str, Any]) -> str:
    pr_delta = metric_delta(metrics["pr_auc"], BASELINE_V22_METRICS["pr_auc"])
    lift5_delta = metric_delta(metrics["lift_at_5pct"], BASELINE_V22_METRICS["lift_at_5pct"])
    if pr_delta >= 0.005 or lift5_delta >= 0.005:
        return "Adopt V2-3b"
    if pr_delta < -0.005 or lift5_delta < -0.005:
        return "Keep V2-2"
    return "Investigate further"


def evaluate_model(model: Any, validation_df: DataFrame, validation_summary: dict[str, Any]) -> dict[str, Any]:
    predictions = model.transform(validation_df).select(
        "label",
        vector_to_array(F.col("probability"))[1].alias("prediction_score"),
    ).cache()
    roc_auc = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="prediction_score", metricName="areaUnderROC"
    ).evaluate(predictions)
    pr_auc = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="prediction_score", metricName="areaUnderPR"
    ).evaluate(predictions)
    topk = V21.topk_metrics(
        predictions,
        validation_summary["row_count"],
        validation_summary["positive_count"],
        validation_summary["positive_rate"],
    )
    predictions.unpersist()
    return {"roc_auc": roc_auc, "pr_auc": pr_auc, **topk}


def coefficient_rows(model: Any, feature_columns: list[str], engineered_features: list[str]) -> list[dict[str, Any]]:
    lr_model = model.stages[-1]
    coefficients = lr_model.coefficients.toArray().tolist()
    rows = []
    for feature_name in engineered_features:
        if feature_name not in feature_columns:
            continue
        index = feature_columns.index(feature_name)
        coefficient = float(coefficients[index])
        rows.append(
            {
                "feature_name": feature_name,
                "coefficient": coefficient,
                "abs_coefficient": abs(coefficient),
                "rank_among_all_features": None,
            }
        )
    abs_sorted = sorted(
        [(feature, abs(float(coefficients[index]))) for index, feature in enumerate(feature_columns)],
        key=lambda item: item[1],
        reverse=True,
    )
    ranks = {feature: rank + 1 for rank, (feature, _) in enumerate(abs_sorted)}
    for row in rows:
        row["rank_among_all_features"] = ranks[row["feature_name"]]
    return rows


def write_feature_definitions(path: Path) -> None:
    lines = [
        "# Baseline V2-3b Feature Definitions",
        "",
        "## Scope",
        "",
        "Baseline V2-3b adds only aggregate pre-cutoff search-to-cart transition context to Baseline V2-2. It preserves the temporal split, labels, Logistic Regression model class, median imputation, class weighting, and TopK evaluation.",
        "",
        "## New Features",
        "",
        "| Feature | Definition | Aggregation logic | Null handling | Leakage review | Privacy review | Complexity |",
        "|---|---|---|---|---|---|---|",
        f"| `search_before_cart_count` | Count of add-to-cart events preceded by at least one search within {SEARCH_BEFORE_CART_WINDOW_DAYS} days. | Filter search/cart events before cutoff, link by `client_id`, require `search_ts <= cart_ts`, then aggregate to user level. | Missing transition count is filled with 0. | Uses pre-cutoff events only. | Does not use raw query text or row examples. | Medium temporal join. |",
        "| `search_to_cart_rate` | Smoothed share of cart events with search context. | `(search_before_cart_count + 1) / (add_to_cart_count + 2)` using pre-cutoff cart count from transition source. | Users without transition rows are filled with 0. | Uses pre-cutoff events only. | Aggregate numeric feature only. | Low after transition count is computed. |",
        f"| `recent_search_then_cart_flag` | 1 when a search in the final {RECENT_SEARCH_WINDOW_DAYS} pre-cutoff days was followed by add-to-cart before cutoff. | Link recent pre-cutoff search to later pre-cutoff cart for the same user. | Missing flag is filled with 0. | Uses pre-cutoff events only. | Aggregate binary feature only. | Medium temporal join. |",
        "",
        "## Explicit Non-Goals",
        "",
        "- No full search family redesign.",
        "- No raw query text features.",
        "- No query embeddings.",
        "- No session features.",
        "- No trend features.",
        "- No category transition features.",
        "- No label, split, model class, threshold, or calibration changes.",
        "",
        "## Privacy",
        "",
        "The new features use only pre-cutoff timestamps and aggregate user-level counts/rates/flags. Artifacts do not include raw client IDs, raw query text, product names, row-level examples, or row-level predictions.",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_evaluation_report(
    path: Path,
    engineered_features: list[str],
    feature_count_e1: int,
    feature_count_v23b: int,
    metrics: dict[str, Any],
    train_summary: dict[str, Any],
    validation_summary: dict[str, Any],
    model_output_path: Path,
    coefficient_summary: list[dict[str, Any]],
    distribution_rows: list[dict[str, Any]],
    positive_rate_rows: list[dict[str, Any]],
) -> None:
    metric_rows = [
        ("ROC-AUC", "roc_auc"),
        ("PR-AUC", "pr_auc"),
        ("Precision@1%", "precision_at_1pct"),
        ("Precision@5%", "precision_at_5pct"),
        ("Precision@10%", "precision_at_10pct"),
        ("Lift@1%", "lift_at_1pct"),
        ("Lift@5%", "lift_at_5pct"),
        ("Lift@10%", "lift_at_10pct"),
    ]
    pr_delta = metric_delta(metrics["pr_auc"], BASELINE_V22_METRICS["pr_auc"])
    lift5_delta = metric_delta(metrics["lift_at_5pct"], BASELINE_V22_METRICS["lift_at_5pct"])
    if pr_delta >= 0 and lift5_delta >= 0:
        interpretation = "Transition features improved the primary temporal ranking metrics relative to V2-2."
    elif pr_delta >= -0.005 and lift5_delta >= -0.005:
        interpretation = "Transition features maintained temporal ranking metrics within a small tolerance relative to V2-2."
    else:
        interpretation = "Transition features reduced at least one primary temporal ranking metric enough to prefer V2-2 for now."

    lines = [
        "# Baseline V2-3b Evaluation",
        "",
        "## New Features",
        "",
    ]
    for feature in engineered_features:
        lines.append(f"- `{feature}`")
    lines.extend(
        [
            "",
            "## Metric Comparison",
            "",
            "| Metric | E1 temporal baseline | V2-1 | V2-2 | V2-3b | V2-3b vs V2-2 |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for label, key in metric_rows:
        lines.append(
            f"| {label} | {BASELINE_E1_METRICS[key]:.6f} | {BASELINE_V21_METRICS[key]:.6f} | "
            f"{BASELINE_V22_METRICS[key]:.6f} | {metrics[key]:.6f} | {metric_delta(metrics[key], BASELINE_V22_METRICS[key]):.2%} |"
        )

    lines.extend(
        [
            "",
            "## Search Family Analysis",
            "",
            interpretation,
            "",
            "### Coefficient Summary",
            "",
            "| Feature | Coefficient | Abs coefficient | Abs-coefficient rank among model features |",
            "|---|---:|---:|---:|",
        ]
    )
    for row in coefficient_summary:
        lines.append(
            f"| {row['feature_name']} | {row['coefficient']:.6f} | {row['abs_coefficient']:.6f} | {row['rank_among_all_features']} |"
        )
    lines.extend(
        [
            "",
            "### Feature Distributions",
            "",
            "| Feature | Non-null rate | Distinct | Mean | Stddev | Min | P50 | P90 | P95 | P99 | Max | Zero rate |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in distribution_rows:
        lines.append(
            "| {feature_name} | {non_null_rate:.6f} | {distinct_count} | {mean} | {stddev} | {min} | {p50} | {p90} | {p95} | {p99} | {max} | {zero_rate:.6f} |".format(
                **{key: "" if value is None else normalize_value(value) for key, value in row.items()}
            )
        )
    lines.extend(
        [
            "",
            "### Positive-Rate Segmentation",
            "",
            "| Feature | Bucket | Row count | Positive count | Positive rate |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for row in positive_rate_rows:
        lines.append(
            f"| {row['feature_name']} | {row['bucket']} | {row['row_count']} | {row['positive_count']} | {row['positive_rate']:.6f} |"
        )
    lines.extend(
        [
            "",
            "## Complexity Added",
            "",
            f"- E1 feature count: {feature_count_e1}",
            "- V2-1 feature count: 31",
            "- V2-2 feature count: 27",
            f"- V2-3b feature count: {feature_count_v23b}",
            f"- New transition features added: {len(engineered_features)}",
            f"- Train rows: {train_summary['row_count']:,}",
            f"- Validation rows: {validation_summary['row_count']:,}",
            f"- Train positive rate: {train_summary['positive_rate']:.6f}",
            f"- Validation positive rate: {validation_summary['positive_rate']:.6f}",
            f"- Model output: `{V21.relative_path(model_output_path)}`",
            "",
            "## Recommendation",
            "",
            performance_decision(metrics),
            "",
            "## Privacy",
            "",
            "Artifacts are aggregate-only. No raw client IDs, raw query text, product names, row-level scores, row-level prediction examples, row-level transitions, or raw model internals are persisted in artifacts.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    config_path = V21.resolve_repo_path(args.feature_config)
    train_input = V21.resolve_repo_path(args.train_input)
    validation_input = V21.resolve_repo_path(args.validation_input)
    search_input = V21.resolve_repo_path(args.search_input)
    cart_input = V21.resolve_repo_path(args.cart_input)
    model_output = V21.resolve_repo_path(args.model_output)
    artifact_dir = V21.resolve_repo_path(args.artifact_dir)
    definitions_path = artifact_dir / "v23b_feature_definitions.md"
    evaluation_path = artifact_dir / "v23b_evaluation.md"
    distribution_path = artifact_dir / "v23b_transition_feature_distribution.csv"
    positive_rate_path = artifact_dir / "v23b_transition_positive_rate.csv"
    coefficient_path = artifact_dir / "v23b_transition_coefficients.csv"
    summary_path = artifact_dir / "v23b_summary.json"

    feature_config = V21.load_feature_config(config_path)
    removed_features = list(feature_config["removed_features"])
    engineered_features = list(feature_config.get("engineered_features", []))
    removed_feature_set = set(removed_features)

    spark = start_spark()
    try:
        train_base = spark.read.parquet(str(train_input)).cache()
        validation_base = spark.read.parquet(str(validation_input)).cache()
        search_events = spark.read.parquet(str(search_input)).cache()
        cart_events = spark.read.parquet(str(cart_input)).cache()
        if V21.label_like_columns(train_base.columns) or V21.label_like_columns(validation_base.columns):
            raise ValueError("Training or validation dataset contains label-like feature columns")

        feature_columns_before = V21.numeric_feature_columns(train_base.schema, set())
        missing_removed = sorted(removed_feature_set.difference(feature_columns_before))
        if missing_removed:
            raise ValueError(f"Configured removed features are absent from training dataset: {missing_removed}")

        train_transitions = transition_features_for_cutoff(search_events, cart_events, TRAIN_CUTOFF_DATE).cache()
        validation_transitions = transition_features_for_cutoff(search_events, cart_events, VALIDATION_CUTOFF_DATE).cache()
        train_augmented = add_transition_features(train_base, train_transitions).cache()
        validation_augmented = add_transition_features(validation_base, validation_transitions).cache()

        feature_columns_v23b = V21.numeric_feature_columns(train_augmented.schema, removed_feature_set)
        validation_feature_columns_v23b = V21.numeric_feature_columns(validation_augmented.schema, removed_feature_set)
        if validation_feature_columns_v23b != feature_columns_v23b:
            raise ValueError("V2-3b train and validation feature columns differ")
        missing_engineered = sorted(set(engineered_features).difference(feature_columns_v23b))
        if missing_engineered:
            raise ValueError(f"Engineered features missing from model input: {missing_engineered}")

        train_summary = V21.dataset_summary(train_augmented, "Train")
        validation_summary = V21.dataset_summary(validation_augmented, "Validation")
        distribution_rows = [
            feature_stats(train_augmented, feature, train_summary["row_count"]) for feature in engineered_features
        ]
        positive_rate_rows: list[dict[str, Any]] = []
        for feature in engineered_features:
            positive_rate_rows.extend(transition_positive_rate_rows(train_augmented, feature))
        write_csv(
            distribution_path,
            distribution_rows,
            [
                "feature_name",
                "non_null_count",
                "non_null_rate",
                "distinct_count",
                "mean",
                "stddev",
                "min",
                "p50",
                "p90",
                "p95",
                "p99",
                "max",
                "zero_count",
                "zero_rate",
            ],
        )
        write_csv(
            positive_rate_path,
            positive_rate_rows,
            ["feature_name", "bucket", "row_count", "positive_count", "positive_rate"],
        )
        write_feature_definitions(definitions_path)

        positive_weight = train_summary["row_count"] / (2.0 * train_summary["positive_count"])
        negative_weight = train_summary["row_count"] / (2.0 * train_summary["negative_count"])
        train_df = V21.add_class_weight(train_augmented, positive_weight, negative_weight).cache()

        imputed_columns = [f"{column}__imputed" for column in feature_columns_v23b]
        pipeline = V21.Pipeline(
            stages=[
                V21.Imputer(inputCols=feature_columns_v23b, outputCols=imputed_columns, strategy="median"),
                V21.VectorAssembler(inputCols=imputed_columns, outputCol="features"),
                V21.LogisticRegression(
                    featuresCol="features",
                    labelCol="label",
                    weightCol="class_weight",
                    maxIter=int(args.max_iter),
                    standardization=True,
                ),
            ]
        )
        model = pipeline.fit(train_df)
        V21.clean_output_dir(model_output, PROJECT_ROOT / "data" / "models")
        model.write().overwrite().save(str(model_output))
        metrics = evaluate_model(model, validation_augmented, validation_summary)
        coefficients = coefficient_rows(model, feature_columns_v23b, engineered_features)
        write_csv(coefficient_path, coefficients, ["feature_name", "coefficient", "abs_coefficient", "rank_among_all_features"])

        write_evaluation_report(
            evaluation_path,
            engineered_features=engineered_features,
            feature_count_e1=len(feature_columns_before),
            feature_count_v23b=len(feature_columns_v23b),
            metrics=metrics,
            train_summary=train_summary,
            validation_summary=validation_summary,
            model_output_path=model_output,
            coefficient_summary=coefficients,
            distribution_rows=distribution_rows,
            positive_rate_rows=positive_rate_rows,
        )

        run_summary = {
            "experiment": "baseline_v2_3b",
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "train_input": args.train_input,
            "validation_input": args.validation_input,
            "search_input": args.search_input,
            "cart_input": args.cart_input,
            "model_output": args.model_output,
            "feature_count_e1": len(feature_columns_before),
            "feature_count_v21": 31,
            "feature_count_v22": 27,
            "feature_count_v23b": len(feature_columns_v23b),
            "removed_features": removed_features,
            "engineered_features": engineered_features,
            "search_before_cart_window_days": SEARCH_BEFORE_CART_WINDOW_DAYS,
            "recent_search_window_days": RECENT_SEARCH_WINDOW_DAYS,
            "metrics": {key: normalize_value(value) for key, value in metrics.items()},
        }
        summary_path.write_text(json.dumps(run_summary, indent=2), encoding="utf-8")

        print("Baseline V2-3b training completed.")
        print(f"Features before: {len(feature_columns_before)}")
        print(f"Features after: {len(feature_columns_v23b)}")
        print(f"Engineered transition features: {len(engineered_features)}")
        print(f"ROC-AUC: {metrics['roc_auc']:.6f}")
        print(f"PR-AUC: {metrics['pr_auc']:.6f}")
        print(f"Lift@5%: {metrics['lift_at_5pct']:.6f}")
        print(f"Feature definitions: {V21.relative_path(definitions_path)}")
        print(f"Evaluation: {V21.relative_path(evaluation_path)}")
        print(f"Model output: {V21.relative_path(model_output)}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
