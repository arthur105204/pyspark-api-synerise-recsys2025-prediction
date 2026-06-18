"""Train Baseline V2-1 with high-confidence feature removals only.

This job validates the first Baseline v2 experiment. It uses the same E1
temporal train/validation snapshots, Logistic Regression class, median
imputation, class weighting, and TopK evaluation pattern. It removes only the
high-confidence defective features from the feature rationalization review.

It does not add features, redesign feature families, modify labels, change
temporal splits, tune hyperparameters, calibrate scores, overwrite E1
artifacts, persist row-level predictions, or write client-level outputs.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import Imputer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "baseline_v21_features.json"
DEFAULT_TRAIN_INPUT = (
    PROJECT_ROOT / "data" / "processed" / "training" / "e1_train_2022_10_10" / "purchase_propensity_30d"
)
DEFAULT_VALIDATION_INPUT = (
    PROJECT_ROOT / "data" / "processed" / "training" / "e1_valid_2022_11_09" / "purchase_propensity_30d"
)
DEFAULT_MODEL_OUTPUT = PROJECT_ROOT / "data" / "models" / "purchase_propensity_baseline_v21"
DEFAULT_ARTIFACT_DIR = PROJECT_ROOT / "artifacts" / "modeling" / "baseline_v2"
DEFAULT_MAX_ITER = 20
TOPK_PERCENTS = (0.01, 0.05, 0.10)
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
BASE_EXCLUDED_COLUMNS = {
    "client_id",
    "label",
    "target_window_start",
    "target_window_end",
    "target_event_count",
    "features",
    "rawPrediction",
    "probability",
    "prediction",
    "class_weight",
}
LABEL_LIKE_PREFIXES = ("label_", "target_")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train Baseline V2-1 temporal model.")
    parser.add_argument(
        "--feature-config",
        default=DEFAULT_CONFIG_PATH.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative Baseline V2-1 feature config JSON path.",
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
        help="Repo-relative V2-1 Spark ML model output path.",
    )
    parser.add_argument(
        "--artifact-dir",
        default=DEFAULT_ARTIFACT_DIR.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative V2-1 artifact directory.",
    )
    parser.add_argument("--max-iter", type=int, default=DEFAULT_MAX_ITER, help="Logistic Regression max iterations.")
    return parser.parse_args()


def relative_path(path: Path) -> str:
    return path.relative_to(PROJECT_ROOT).as_posix()


def resolve_repo_path(path_text: str) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        raise ValueError("path arguments must be repo-relative")
    return PROJECT_ROOT / path


def normalize_value(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    return value


def safe_divide(numerator: float, denominator: float) -> float:
    return float(numerator) / float(denominator) if denominator else 0.0


def load_feature_config(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    removed = payload.get("removed_features", [])
    if not isinstance(removed, list) or not all(isinstance(item, str) for item in removed):
        raise ValueError("Feature config must contain removed_features as a list of strings")
    return payload


def start_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("train-baseline-v21")
        .master("local[*]")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def clean_output_dir(path: Path, allowed_base: Path) -> None:
    resolved = path.resolve()
    allowed_root = allowed_base.resolve()
    if allowed_root != resolved and allowed_root not in resolved.parents:
        raise ValueError(f"Refusing to clean path outside {allowed_base}")
    if path.exists():
        shutil.rmtree(path)


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: normalize_value(value) for key, value in row.items()})


def label_like_columns(columns: list[str]) -> list[str]:
    flagged = []
    for column in columns:
        lowered = column.lower()
        if lowered in {"target", "y"} or lowered.startswith(LABEL_LIKE_PREFIXES):
            flagged.append(column)
    return flagged


def numeric_feature_columns(schema: T.StructType, removed_features: set[str]) -> list[str]:
    numeric_types = (
        T.ByteType,
        T.ShortType,
        T.IntegerType,
        T.LongType,
        T.FloatType,
        T.DoubleType,
        T.DecimalType,
    )
    blocked = BASE_EXCLUDED_COLUMNS.union(removed_features)
    return [
        field.name
        for field in schema.fields
        if field.name not in blocked and isinstance(field.dataType, numeric_types)
    ]


def dataset_summary(df: DataFrame, dataset_name: str) -> dict[str, Any]:
    row = df.agg(
        F.count("*").alias("row_count"),
        F.sum(F.col("label")).alias("positive_count"),
    ).collect()[0]
    row_count = int(row["row_count"])
    positive_count = int(row["positive_count"] or 0)
    negative_count = row_count - positive_count
    if not row_count or not positive_count or not negative_count:
        raise ValueError(f"{dataset_name} dataset must contain positive and negative labels")
    return {
        "row_count": row_count,
        "positive_count": positive_count,
        "negative_count": negative_count,
        "positive_rate": safe_divide(positive_count, row_count),
    }


def add_class_weight(df: DataFrame, positive_weight: float, negative_weight: float) -> DataFrame:
    return df.withColumn(
        "class_weight",
        F.when(F.col("label") == 1, F.lit(float(positive_weight))).otherwise(F.lit(float(negative_weight))),
    )


def topk_metrics(predictions: DataFrame, row_count: int, positive_count: int, positive_rate: float) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    scored = predictions.select("label", "prediction_score")
    for percent in TOPK_PERCENTS:
        top_k_count = max(1, int(math.ceil(row_count * percent)))
        row = scored.orderBy(F.desc("prediction_score")).limit(top_k_count).agg(
            F.count(F.lit(1)).alias("top_k_count"),
            F.sum(F.col("label").cast("long")).alias("positive_count"),
        ).collect()[0]
        observed_top_k_count = int(row["top_k_count"] or 0)
        observed_positive_count = int(row["positive_count"] or 0)
        precision_at_k = safe_divide(observed_positive_count, observed_top_k_count)
        lift_at_k = safe_divide(precision_at_k, positive_rate)
        key = int(percent * 100)
        metrics[f"precision_at_{key}pct"] = precision_at_k
        metrics[f"lift_at_{key}pct"] = lift_at_k
    return metrics


def train_and_evaluate(
    train_df: DataFrame,
    validation_df: DataFrame,
    feature_columns: list[str],
    max_iter: int,
    validation_summary: dict[str, Any],
    model_output_path: Path,
) -> dict[str, Any]:
    imputed_columns = [f"{column}__imputed" for column in feature_columns]
    pipeline = Pipeline(
        stages=[
            Imputer(inputCols=feature_columns, outputCols=imputed_columns, strategy="median"),
            VectorAssembler(inputCols=imputed_columns, outputCol="features"),
            LogisticRegression(
                featuresCol="features",
                labelCol="label",
                weightCol="class_weight",
                maxIter=int(max_iter),
                standardization=True,
            ),
        ]
    )
    model = pipeline.fit(train_df)
    clean_output_dir(model_output_path, PROJECT_ROOT / "data" / "models")
    model.write().overwrite().save(str(model_output_path))

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
    topk = topk_metrics(
        predictions,
        validation_summary["row_count"],
        validation_summary["positive_count"],
        validation_summary["positive_rate"],
    )
    predictions.unpersist()
    return {"roc_auc": roc_auc, "pr_auc": pr_auc, **topk}


def feature_family(feature_name: str) -> str:
    if feature_name.startswith("add_to_cart"):
        return "add_to_cart_activity"
    if feature_name.startswith("product_buy"):
        return "product_buy_activity"
    if feature_name.startswith("remove_from_cart"):
        return "remove_from_cart_activity"
    if feature_name.startswith("search_query"):
        return "search_activity"
    return "unknown"


def window_number(feature_name: str) -> int:
    if feature_name.endswith("_30d"):
        return 30
    if feature_name.endswith("_60d"):
        return 60
    if feature_name.endswith("_90d"):
        return 90
    raise ValueError(f"Not a rolling-window feature: {feature_name}")


def base_window_name(feature_name: str) -> str:
    for suffix in ("_30d", "_60d", "_90d"):
        if feature_name.endswith(suffix):
            return feature_name[: -len(suffix)]
    raise ValueError(f"Not a rolling-window feature: {feature_name}")


def safe_corr(df: DataFrame, left: str | None, right: str | None, variances: dict[str, float | None]) -> float | None:
    if not left or not right:
        return None
    if not variances.get(left) or not variances.get(right):
        return None
    value = df.stat.corr(left, right)
    if value is None or math.isnan(value) or math.isinf(value):
        return None
    return float(value)


def rolling_window_review(df: DataFrame, feature_columns_before_removal: list[str]) -> list[dict[str, Any]]:
    window_features = sorted(
        [column for column in feature_columns_before_removal if column.endswith(("_count_30d", "_count_60d", "_count_90d"))],
        key=lambda column: (base_window_name(column), window_number(column)),
    )
    variance_exprs = [F.variance(F.col(column).cast("double")).alias(column) for column in window_features]
    variance_row = df.agg(*variance_exprs).collect()[0].asDict() if variance_exprs else {}
    variances = {
        column: (
            float(variance_row[column])
            if variance_row.get(column) is not None and not math.isnan(float(variance_row[column]))
            else None
        )
        for column in window_features
    }
    feature_set = set(window_features)
    rows = []
    for column in window_features:
        base_name = base_window_name(column)
        window = window_number(column)
        previous_window = f"{base_name}_{window - 30}d" if window in {60, 90} else None
        next_window = f"{base_name}_{window + 30}d" if window in {30, 60} else None
        previous_window = previous_window if previous_window in feature_set else None
        next_window = next_window if next_window in feature_set else None
        rows.append(
            {
                "feature_name": column,
                "family": feature_family(column),
                "window_days": window,
                "variance": variances.get(column),
                "correlation_with_previous_window": safe_corr(df, column, previous_window, variances),
                "correlation_with_next_window": safe_corr(df, column, next_window, variances),
                "recommendation": "REVIEW_ONLY_KEEP_IN_V21",
            }
        )
    return rows


def metric_delta(v21: float, e1: float) -> float:
    return safe_divide(v21 - e1, e1)


def performance_decision(metrics: dict[str, Any]) -> str:
    pr_delta = metric_delta(metrics["pr_auc"], BASELINE_E1_METRICS["pr_auc"])
    lift5_delta = metric_delta(metrics["lift_at_5pct"], BASELINE_E1_METRICS["lift_at_5pct"])
    if pr_delta >= -0.005 and lift5_delta >= -0.005:
        return "Adopt V2-1 as new baseline"
    if pr_delta < -0.01 or lift5_delta < -0.01:
        return "Keep E1"
    return "Investigate further"


def write_evaluation_report(
    path: Path,
    removed_features: list[str],
    feature_count_before: int,
    feature_count_after: int,
    metrics: dict[str, Any],
    train_summary: dict[str, Any],
    validation_summary: dict[str, Any],
    model_output_path: Path,
) -> None:
    decision = performance_decision(metrics)
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
        "# Baseline V2-1 Evaluation",
        "",
        "## Changes Made",
        "",
        "Baseline V2-1 removes only high-confidence defective features and keeps the E1 temporal split, preprocessing output, model class, class weighting, median imputation, and TopK evaluation pattern unchanged.",
        "",
        "Removed features:",
    ]
    for feature in removed_features:
        lines.append(f"- `{feature}`")

    lines.extend(
        [
            "",
            "No rolling-window features were removed in V2-1. Rolling windows are reviewed separately in `v21_window_review.csv`.",
            "",
            "## Metric Comparison",
            "",
            "| Metric | E1 temporal baseline | V2-1 | Relative change |",
            "|---|---:|---:|---:|",
        ]
    )
    for label, key in metric_rows:
        e1_value = BASELINE_E1_METRICS[key]
        v21_value = metrics[key]
        lines.append(f"| {label} | {e1_value:.6f} | {v21_value:.6f} | {metric_delta(v21_value, e1_value):.2%} |")

    lines.extend(
        [
            "",
            "## Feature Count Comparison",
            "",
            f"- E1 candidate feature count before V2-1 removals: {feature_count_before}",
            f"- V2-1 feature count after removals: {feature_count_after}",
            f"- Features removed: {feature_count_before - feature_count_after}",
            "",
            "## Temporal Setup",
            "",
            f"- Train rows: {train_summary['row_count']:,}",
            f"- Validation rows: {validation_summary['row_count']:,}",
            f"- Train positive rate: {train_summary['positive_rate']:.6f}",
            f"- Validation positive rate: {validation_summary['positive_rate']:.6f}",
            f"- Model output: `{relative_path(model_output_path)}`",
            "",
            "## Interpretation",
            "",
        ]
    )
    pr_delta = metric_delta(metrics["pr_auc"], BASELINE_E1_METRICS["pr_auc"])
    lift5_delta = metric_delta(metrics["lift_at_5pct"], BASELINE_E1_METRICS["lift_at_5pct"])
    if pr_delta >= 0 and lift5_delta >= 0:
        interpretation = "Removing the constant feature and raw ratio features improved performance."
    elif pr_delta >= -0.005 and lift5_delta >= -0.005:
        interpretation = "Removing the constant feature and raw ratio features maintained performance within a small tolerance."
    else:
        interpretation = "Removing the constant feature and raw ratio features hurt at least one primary metric enough to require review."
    lines.extend(
        [
            interpretation,
            "",
            "This experiment does not prove that the removed feature concepts are useless in all forms. It only validates whether the current constant indicator and raw ratio implementations should remain in the baseline feature set.",
            "",
            "## Recommendation",
            "",
            decision,
            "",
            "## Privacy",
            "",
            "Artifacts are aggregate-only. No raw client IDs, raw query text, product names, row-level scores, row-level prediction examples, or raw model internals are persisted in artifacts.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    config_path = resolve_repo_path(args.feature_config)
    train_input = resolve_repo_path(args.train_input)
    validation_input = resolve_repo_path(args.validation_input)
    model_output = resolve_repo_path(args.model_output)
    artifact_dir = resolve_repo_path(args.artifact_dir)
    window_review_path = artifact_dir / "v21_window_review.csv"
    evaluation_path = artifact_dir / "v21_evaluation.md"

    feature_config = load_feature_config(config_path)
    removed_features = list(feature_config["removed_features"])
    removed_feature_set = set(removed_features)

    spark = start_spark()
    try:
        train_base = spark.read.parquet(str(train_input)).cache()
        validation_base = spark.read.parquet(str(validation_input)).cache()
        train_label_like = label_like_columns(train_base.columns)
        validation_label_like = label_like_columns(validation_base.columns)
        if train_label_like or validation_label_like:
            raise ValueError("Training or validation dataset contains label-like feature columns")

        feature_columns_before = numeric_feature_columns(train_base.schema, set())
        missing_removed = sorted(removed_feature_set.difference(feature_columns_before))
        if missing_removed:
            raise ValueError(f"Configured removed features are absent from training dataset: {missing_removed}")
        feature_columns_v21 = numeric_feature_columns(train_base.schema, removed_feature_set)
        validation_feature_columns_v21 = numeric_feature_columns(validation_base.schema, removed_feature_set)
        if validation_feature_columns_v21 != feature_columns_v21:
            raise ValueError("V2-1 train and validation feature columns differ")

        window_rows = rolling_window_review(train_base, feature_columns_before)
        write_csv(
            window_review_path,
            window_rows,
            [
                "feature_name",
                "family",
                "window_days",
                "variance",
                "correlation_with_previous_window",
                "correlation_with_next_window",
                "recommendation",
            ],
        )

        train_summary = dataset_summary(train_base, "Train")
        validation_summary = dataset_summary(validation_base, "Validation")
        positive_weight = train_summary["row_count"] / (2.0 * train_summary["positive_count"])
        negative_weight = train_summary["row_count"] / (2.0 * train_summary["negative_count"])
        train_df = add_class_weight(train_base, positive_weight, negative_weight).cache()
        metrics = train_and_evaluate(
            train_df=train_df,
            validation_df=validation_base,
            feature_columns=feature_columns_v21,
            max_iter=args.max_iter,
            validation_summary=validation_summary,
            model_output_path=model_output,
        )

        write_evaluation_report(
            evaluation_path,
            removed_features=removed_features,
            feature_count_before=len(feature_columns_before),
            feature_count_after=len(feature_columns_v21),
            metrics=metrics,
            train_summary=train_summary,
            validation_summary=validation_summary,
            model_output_path=model_output,
        )

        print("Baseline V2-1 training completed.")
        print(f"Features before: {len(feature_columns_before)}")
        print(f"Features after: {len(feature_columns_v21)}")
        print(f"ROC-AUC: {metrics['roc_auc']:.6f}")
        print(f"PR-AUC: {metrics['pr_auc']:.6f}")
        print(f"Lift@5%: {metrics['lift_at_5pct']:.6f}")
        print(f"Window review: {relative_path(window_review_path)}")
        print(f"Evaluation: {relative_path(evaluation_path)}")
        print(f"Model output: {relative_path(model_output)}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
