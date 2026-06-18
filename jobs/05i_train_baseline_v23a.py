"""Train Baseline V2-3a with search quick-win features only.

This job validates whether simple search-intent representations outperform the
raw search count/recency representation in Baseline V2-2. It uses the same E1
temporal train/validation snapshots, Logistic Regression class, median
imputation, class weighting, and TopK evaluation pattern as E1/V2-1/V2-2.

It adds only three engineered search quick-win features:

- search_count_bucket
- search_recency_bucket
- recent_search_flag

It removes the remaining raw search count/day/recency features so the experiment
tests a small replacement representation. It does not implement search-to-cart
transitions, normalized search intensity, trend features, session features,
query semantics, query embeddings, category transitions, calibration,
hyperparameter tuning, or a new model class.
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
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "baseline_v23a_features.json"
DEFAULT_TRAIN_INPUT = (
    PROJECT_ROOT / "data" / "processed" / "training" / "e1_train_2022_10_10" / "purchase_propensity_30d"
)
DEFAULT_VALIDATION_INPUT = (
    PROJECT_ROOT / "data" / "processed" / "training" / "e1_valid_2022_11_09" / "purchase_propensity_30d"
)
DEFAULT_MODEL_OUTPUT = PROJECT_ROOT / "data" / "models" / "purchase_propensity_baseline_v23a"
DEFAULT_ARTIFACT_DIR = PROJECT_ROOT / "artifacts" / "modeling" / "baseline_v2"
DEFAULT_MAX_ITER = 20
RECENT_SEARCH_WINDOW_DAYS = 30
SEARCH_COUNT_BUCKET_LABELS = {
    0: "0",
    1: "1",
    2: "2-5",
    3: "6-20",
    4: ">20",
}
SEARCH_FEATURES_REVIEWED = [
    "search_query_count",
    "distinct_search_days",
    "days_since_last_search_query",
    "search_query_count_30d",
    "search_query_count_60d",
    "search_query_count_90d",
]
V23A_REPLACED_SEARCH_FEATURES = [
    "search_query_count",
    "distinct_search_days",
    "days_since_last_search_query",
    "search_query_count_30d",
    "search_query_count_90d",
]
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
    parser = argparse.ArgumentParser(description="Train Baseline V2-3a temporal model.")
    parser.add_argument(
        "--feature-config",
        default=DEFAULT_CONFIG_PATH.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative Baseline V2-3a feature config JSON path.",
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
        "--model-output",
        default=DEFAULT_MODEL_OUTPUT.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative V2-3a Spark ML model output path.",
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
        SparkSession.builder.appName("train-baseline-v23a")
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


def derive_recency_thresholds(train_df: DataFrame) -> list[int]:
    non_null_count = train_df.where(F.col("days_since_last_search_query").isNotNull()).count()
    if non_null_count == 0:
        return [7, 30, 60]
    quantiles = train_df.approxQuantile("days_since_last_search_query", [0.25, 0.50, 0.75], 0.001)
    thresholds = sorted({max(1, int(round(value))) for value in quantiles if value is not None})
    if len(thresholds) < 3:
        return [7, 30, 60]
    return thresholds[:3]


def add_search_quick_win_features(df: DataFrame, recency_thresholds: list[int]) -> DataFrame:
    recency_1, recency_2, recency_3 = recency_thresholds
    search_count = F.coalesce(F.col("search_query_count"), F.lit(0))
    search_recency = F.col("days_since_last_search_query")
    return (
        df.withColumn(
            "search_count_bucket",
            F.when(search_count <= 0, F.lit(0))
            .when(search_count == 1, F.lit(1))
            .when(search_count <= 5, F.lit(2))
            .when(search_count <= 20, F.lit(3))
            .otherwise(F.lit(4))
            .cast("int"),
        )
        .withColumn(
            "search_recency_bucket",
            F.when(search_count <= 0, F.lit(0))
            .when(search_recency.isNull(), F.lit(0))
            .when(search_recency <= recency_1, F.lit(1))
            .when(search_recency <= recency_2, F.lit(2))
            .when(search_recency <= recency_3, F.lit(3))
            .otherwise(F.lit(4))
            .cast("int"),
        )
        .withColumn(
            "recent_search_flag",
            F.when((search_count > 0) & (search_recency <= RECENT_SEARCH_WINDOW_DAYS), F.lit(1)).otherwise(F.lit(0)).cast("int"),
        )
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


def bucket_distribution(df: DataFrame, bucket_column: str) -> list[dict[str, Any]]:
    rows = (
        df.groupBy(bucket_column)
        .agg(
            F.count(F.lit(1)).alias("row_count"),
            F.sum(F.col("label").cast("long")).alias("positive_count"),
        )
        .orderBy(bucket_column)
        .collect()
    )
    result = []
    for row in rows:
        row_count = int(row["row_count"] or 0)
        positive_count = int(row["positive_count"] or 0)
        result.append(
            {
                "bucket_column": bucket_column,
                "bucket": int(row[bucket_column]),
                "row_count": row_count,
                "positive_count": positive_count,
                "positive_rate": safe_divide(positive_count, row_count),
            }
        )
    return result


def write_search_feature_review(
    path: Path,
    stats_rows: list[dict[str, Any]],
    bucket_rows: list[dict[str, Any]],
    recency_thresholds: list[int],
) -> None:
    lines = [
        "# Baseline V2-3a Search Feature Review",
        "",
        "## Scope",
        "",
        "This review documents the current search feature distribution before training Baseline V2-3a. It is aggregate-only and does not include raw search text or row-level examples.",
        "",
        "## Current Search Features",
        "",
        "| Feature | Non-null rate | Distinct | Mean | Stddev | Min | P50 | P90 | P95 | P99 | Max | Zero rate | Interpretation |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in stats_rows:
        interpretation = "Heavy-tailed count feature" if "count" in row["feature_name"] else "Sparse recency/day feature"
        lines.append(
            "| {feature_name} | {non_null_rate:.6f} | {distinct_count} | {mean} | {stddev} | {min} | {p50} | {p90} | {p95} | {p99} | {max} | {zero_rate:.6f} | {interpretation} |".format(
                **{key: "" if value is None else normalize_value(value) for key, value in row.items()},
                interpretation=interpretation,
            )
        )
    lines.extend(
        [
            "",
            "## V2-3a Bucket Thresholds",
            "",
            "- `search_count_bucket`: 0, 1, 2-5, 6-20, >20 searches.",
            f"- `search_recency_bucket`: no search, <= {recency_thresholds[0]} days, <= {recency_thresholds[1]} days, <= {recency_thresholds[2]} days, > {recency_thresholds[2]} days.",
            f"- `recent_search_flag`: 1 when search recency is <= {RECENT_SEARCH_WINDOW_DAYS} days, else 0.",
            "",
            "## Bucket Target Relationship On Training Snapshot",
            "",
            "| Bucket feature | Bucket | Row count | Positive count | Positive rate |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for row in bucket_rows:
        lines.append(
            f"| {row['bucket_column']} | {row['bucket']} | {row['row_count']} | {row['positive_count']} | {row['positive_rate']:.6f} |"
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "Current search features are behaviorally meaningful but noisy. Count features have high maximum values and search-day features overlap with general activity. V2-3a tests whether coarse buckets and a recent-search flag capture search intent more robustly than raw count/recency values.",
            "",
            "## Privacy",
            "",
            "This artifact contains aggregate feature statistics only. It does not include raw client IDs, raw query text, product names, row-level scores, row-level predictions, or row-level examples.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_feature_definitions(path: Path, recency_thresholds: list[int], replaced_search_features: list[str]) -> None:
    lines = [
        "# Baseline V2-3a Feature Definitions",
        "",
        "## Scope",
        "",
        "Baseline V2-3a implements only search quick-win features. It keeps the V2-2 temporal split, labels, model class, median imputation, class weighting, and evaluation unchanged.",
        "",
            "## Raw Search Features Replaced Relative To V2-2",
            "",
    ]
    for feature in replaced_search_features:
        lines.append(f"- `{feature}`")
    lines.extend(
        [
            "",
            "## New Features",
            "",
            "| Feature | Type | Definition | Rationale | Null handling | Leakage review |",
            "|---|---|---|---|---|---|",
            "| `search_count_bucket` | Integer bucket | 0 searches = 0; 1 search = 1; 2-5 searches = 2; 6-20 searches = 3; >20 searches = 4. | Reduces heavy-tail sensitivity and keeps search intensity interpretable. | Null search count is treated as 0. | Uses pre-cutoff aggregate search count only. |",
            f"| `search_recency_bucket` | Integer bucket | No search = 0; <= {recency_thresholds[0]} days = 1; <= {recency_thresholds[1]} days = 2; <= {recency_thresholds[2]} days = 3; > {recency_thresholds[2]} days = 4. | Tests whether freshness is more robust than raw recency. Thresholds are derived from the temporal training snapshot. | Null recency or no search is bucket 0. | Uses pre-cutoff search recency only. |",
            f"| `recent_search_flag` | Binary indicator | 1 if the user searched within {RECENT_SEARCH_WINDOW_DAYS} days before cutoff, else 0. | Business-friendly recent-intent flag aligned with the existing 30-day window. | Null recency or no search is 0. | Uses pre-cutoff search recency only. |",
            "",
            "## Explicit Non-Goals",
            "",
            "- No search-to-cart transition features.",
            "- No normalized search intensity ratios.",
            "- No trend features.",
            "- No session features.",
            "- No query semantics.",
            "- No query embeddings.",
            "- No category transition features.",
            "- No model class, label, temporal split, threshold, or calibration changes.",
            "",
            "## Privacy",
            "",
            "The new features use only numeric pre-cutoff aggregate search counts and recency. No raw query text, raw client IDs, product names, row-level examples, or row-level predictions are persisted.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def metric_delta(candidate: float, baseline: float) -> float:
    return safe_divide(candidate - baseline, baseline)


def performance_decision(metrics: dict[str, Any]) -> str:
    pr_delta_vs_v22 = metric_delta(metrics["pr_auc"], BASELINE_V22_METRICS["pr_auc"])
    lift5_delta_vs_v22 = metric_delta(metrics["lift_at_5pct"], BASELINE_V22_METRICS["lift_at_5pct"])
    if pr_delta_vs_v22 >= 0.005 or lift5_delta_vs_v22 >= 0.005:
        return "Adopt V2-3a"
    if pr_delta_vs_v22 < -0.005 or lift5_delta_vs_v22 < -0.005:
        return "Keep V2-2"
    return "Investigate further"


def write_evaluation_report(
    path: Path,
    engineered_features: list[str],
    replaced_search_features: list[str],
    feature_count_e1: int,
    feature_count_v23a: int,
    metrics: dict[str, Any],
    train_summary: dict[str, Any],
    validation_summary: dict[str, Any],
    model_output_path: Path,
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
    lines = [
        "# Baseline V2-3a Evaluation",
        "",
        "## New Features",
        "",
    ]
    for feature in engineered_features:
        lines.append(f"- `{feature}`")
    lines.extend(
        [
            "",
            "Raw search representation features replaced relative to V2-2:",
        ]
    )
    for feature in replaced_search_features:
        lines.append(f"- `{feature}`")
    lines.extend(
        [
            "",
            "## Metric Comparison",
            "",
            "| Metric | E1 temporal baseline | V2-1 | V2-2 | V2-3a | V2-3a vs V2-2 |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for label, key in metric_rows:
        lines.append(
            f"| {label} | {BASELINE_E1_METRICS[key]:.6f} | {BASELINE_V21_METRICS[key]:.6f} | "
            f"{BASELINE_V22_METRICS[key]:.6f} | {metrics[key]:.6f} | {metric_delta(metrics[key], BASELINE_V22_METRICS[key]):.2%} |"
        )

    pr_delta = metric_delta(metrics["pr_auc"], BASELINE_V22_METRICS["pr_auc"])
    lift5_delta = metric_delta(metrics["lift_at_5pct"], BASELINE_V22_METRICS["lift_at_5pct"])
    if pr_delta >= 0 and lift5_delta >= 0:
        interpretation = "Search quick-win features improved the primary ranking metrics relative to V2-2."
    elif pr_delta >= -0.005 and lift5_delta >= -0.005:
        interpretation = "Search quick-win features maintained the primary ranking metrics within a small tolerance relative to V2-2."
    else:
        interpretation = "Search quick-win features reduced at least one primary ranking metric enough to prefer V2-2 for now."

    lines.extend(
        [
            "",
            "## Search Family Impact",
            "",
            interpretation,
            "",
            "V2-3a tests only whether coarse search intensity, search freshness, and a recent-search flag are a better representation than raw search count/day/recency features. It does not test transition features, trend features, sessions, query semantics, query embeddings, or category-aware behavior.",
            "",
            "## Complexity Added",
            "",
            f"- E1 feature count: {feature_count_e1}",
            "- V2-1 feature count: 31",
            "- V2-2 feature count: 27",
            f"- V2-3a feature count: {feature_count_v23a}",
            f"- New quick-win features added: {len(engineered_features)}",
            f"- Raw search representation features replaced relative to V2-2: {len(replaced_search_features)}",
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
            "Artifacts are aggregate-only. No raw client IDs, raw query text, product names, row-level scores, row-level prediction examples, or raw model internals are persisted in artifacts.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


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


def main() -> int:
    args = parse_args()
    config_path = V21.resolve_repo_path(args.feature_config)
    train_input = V21.resolve_repo_path(args.train_input)
    validation_input = V21.resolve_repo_path(args.validation_input)
    model_output = V21.resolve_repo_path(args.model_output)
    artifact_dir = V21.resolve_repo_path(args.artifact_dir)
    search_review_path = artifact_dir / "v23a_search_feature_review.md"
    feature_definitions_path = artifact_dir / "v23a_feature_definitions.md"
    evaluation_path = artifact_dir / "v23a_evaluation.md"
    summary_path = artifact_dir / "v23a_summary.json"

    feature_config = V21.load_feature_config(config_path)
    removed_features = list(feature_config["removed_features"])
    engineered_features = list(feature_config.get("engineered_features", []))
    removed_feature_set = set(removed_features)
    replaced_search_features = [feature for feature in V23A_REPLACED_SEARCH_FEATURES if feature in removed_features]

    spark = start_spark()
    try:
        train_base = spark.read.parquet(str(train_input)).cache()
        validation_base = spark.read.parquet(str(validation_input)).cache()
        train_label_like = V21.label_like_columns(train_base.columns)
        validation_label_like = V21.label_like_columns(validation_base.columns)
        if train_label_like or validation_label_like:
            raise ValueError("Training or validation dataset contains label-like feature columns")

        feature_columns_before = V21.numeric_feature_columns(train_base.schema, set())
        missing_removed = sorted(removed_feature_set.difference(feature_columns_before))
        if missing_removed:
            raise ValueError(f"Configured removed features are absent from training dataset: {missing_removed}")

        recency_thresholds = derive_recency_thresholds(train_base)
        train_augmented = add_search_quick_win_features(train_base, recency_thresholds).cache()
        validation_augmented = add_search_quick_win_features(validation_base, recency_thresholds).cache()

        feature_columns_v23a = V21.numeric_feature_columns(train_augmented.schema, removed_feature_set)
        validation_feature_columns_v23a = V21.numeric_feature_columns(validation_augmented.schema, removed_feature_set)
        if validation_feature_columns_v23a != feature_columns_v23a:
            raise ValueError("V2-3a train and validation feature columns differ")
        missing_engineered = sorted(set(engineered_features).difference(feature_columns_v23a))
        if missing_engineered:
            raise ValueError(f"Engineered features missing from model input: {missing_engineered}")

        train_summary = V21.dataset_summary(train_augmented, "Train")
        validation_summary = V21.dataset_summary(validation_augmented, "Validation")

        stats_rows = [feature_stats(train_base, feature, train_summary["row_count"]) for feature in SEARCH_FEATURES_REVIEWED]
        bucket_rows = bucket_distribution(train_augmented, "search_count_bucket")
        bucket_rows.extend(bucket_distribution(train_augmented, "search_recency_bucket"))
        bucket_rows.extend(bucket_distribution(train_augmented, "recent_search_flag"))
        write_search_feature_review(search_review_path, stats_rows, bucket_rows, recency_thresholds)
        write_feature_definitions(feature_definitions_path, recency_thresholds, replaced_search_features)

        positive_weight = train_summary["row_count"] / (2.0 * train_summary["positive_count"])
        negative_weight = train_summary["row_count"] / (2.0 * train_summary["negative_count"])
        train_df = V21.add_class_weight(train_augmented, positive_weight, negative_weight).cache()

        imputed_columns = [f"{column}__imputed" for column in feature_columns_v23a]
        pipeline = V21.Pipeline(
            stages=[
                V21.Imputer(inputCols=feature_columns_v23a, outputCols=imputed_columns, strategy="median"),
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

        write_evaluation_report(
            evaluation_path,
            engineered_features=engineered_features,
            replaced_search_features=replaced_search_features,
            feature_count_e1=len(feature_columns_before),
            feature_count_v23a=len(feature_columns_v23a),
            metrics=metrics,
            train_summary=train_summary,
            validation_summary=validation_summary,
            model_output_path=model_output,
        )

        run_summary = {
            "experiment": "baseline_v2_3a",
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "train_input": args.train_input,
            "validation_input": args.validation_input,
            "model_output": args.model_output,
            "feature_count_e1": len(feature_columns_before),
            "feature_count_v21": 31,
            "feature_count_v22": 27,
            "feature_count_v23a": len(feature_columns_v23a),
            "removed_features": removed_features,
            "replaced_search_features": replaced_search_features,
            "engineered_features": engineered_features,
            "search_recency_thresholds": recency_thresholds,
            "recent_search_window_days": RECENT_SEARCH_WINDOW_DAYS,
            "metrics": {key: normalize_value(value) for key, value in metrics.items()},
        }
        summary_path.write_text(json.dumps(run_summary, indent=2), encoding="utf-8")

        print("Baseline V2-3a training completed.")
        print(f"Features before: {len(feature_columns_before)}")
        print(f"Features after: {len(feature_columns_v23a)}")
        print(f"Engineered search features: {len(engineered_features)}")
        print(f"Raw search features replaced: {len(replaced_search_features)}")
        print(f"ROC-AUC: {metrics['roc_auc']:.6f}")
        print(f"PR-AUC: {metrics['pr_auc']:.6f}")
        print(f"Lift@5%: {metrics['lift_at_5pct']:.6f}")
        print(f"Search review: {V21.relative_path(search_review_path)}")
        print(f"Feature definitions: {V21.relative_path(feature_definitions_path)}")
        print(f"Evaluation: {V21.relative_path(evaluation_path)}")
        print(f"Model output: {V21.relative_path(model_output)}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
