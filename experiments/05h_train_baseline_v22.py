"""Train Baseline V2-2 with rolling-window reduction only.

This job validates whether overlapping rolling-window count features can be
simplified after Baseline V2-1. It uses the same E1 temporal train/validation
snapshots, Logistic Regression class, median imputation, class weighting, and
TopK evaluation pattern as E1/V2-1.

It removes the V2-1 high-confidence defective features plus standalone 60-day
rolling count features. It does not add features, redesign feature families,
modify labels, change temporal splits, tune hyperparameters, calibrate scores,
overwrite E1/V2-1 artifacts, persist row-level predictions, or write
client-level outputs.
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
V21_JOB_PATH = PROJECT_ROOT / "experiments" / "05g_train_baseline_v21.py"
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "baseline_v22_features.json"
DEFAULT_TRAIN_INPUT = (
    PROJECT_ROOT / "data" / "processed" / "training" / "e1_train_2022_10_10" / "purchase_propensity_30d"
)
DEFAULT_VALIDATION_INPUT = (
    PROJECT_ROOT / "data" / "processed" / "training" / "e1_valid_2022_11_09" / "purchase_propensity_30d"
)
DEFAULT_MODEL_OUTPUT = PROJECT_ROOT / "data" / "models" / "purchase_propensity_baseline_v22"
DEFAULT_ARTIFACT_DIR = PROJECT_ROOT / "artifacts" / "modeling" / "baseline_v2"
DEFAULT_MAX_ITER = 20
ROLLING_BASE_COUNTS = (
    "add_to_cart_count",
    "product_buy_count",
    "remove_from_cart_count",
    "search_query_count",
)
WINDOW_DAYS = (30, 60, 90)
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


def load_v21_module() -> Any:
    spec = importlib.util.spec_from_file_location("baseline_v21_job", V21_JOB_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load Baseline V2-1 helper module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


V21 = load_v21_module()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train Baseline V2-2 temporal model.")
    parser.add_argument(
        "--feature-config",
        default=DEFAULT_CONFIG_PATH.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative Baseline V2-2 feature config JSON path.",
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
        help="Repo-relative V2-2 Spark ML model output path.",
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
        SparkSession.builder.appName("train-baseline-v22")
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


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: normalize_value(value) for key, value in row.items()})


def family_for_base_count(base_count: str) -> str:
    if base_count == "add_to_cart_count":
        return "add_to_cart_activity"
    if base_count == "product_buy_count":
        return "product_buy_activity"
    if base_count == "remove_from_cart_count":
        return "remove_from_cart_activity"
    if base_count == "search_query_count":
        return "search_activity"
    return "unknown"


def safe_corr(df: DataFrame, left: str | None, right: str | None, variances: dict[str, float | None]) -> float | None:
    if not left or not right:
        return None
    if not variances.get(left) or not variances.get(right):
        return None
    value = df.stat.corr(left, right)
    if value is None or math.isnan(value) or math.isinf(value):
        return None
    return float(value)


def rolling_review_features(columns: list[str]) -> list[str]:
    ordered = []
    column_set = set(columns)
    for base_count in ROLLING_BASE_COUNTS:
        if base_count in column_set:
            ordered.append(base_count)
        for days in WINDOW_DAYS:
            feature = f"{base_count}_{days}d"
            if feature in column_set:
                ordered.append(feature)
    return ordered


def recommendation_for_window(feature_name: str, removed_features: set[str]) -> str:
    if feature_name in removed_features:
        return "REMOVE_IN_V22"
    if feature_name.endswith("_90d"):
        return "KEEP_IN_V22_EVALUATE_SEPARATELY"
    return "KEEP_IN_V22"


def build_window_selection_rows(
    df: DataFrame,
    feature_columns_before_removal: list[str],
    removed_features: set[str],
) -> list[dict[str, Any]]:
    review_features = rolling_review_features(feature_columns_before_removal)
    variance_exprs = [F.variance(F.col(column).cast("double")).alias(column) for column in review_features]
    variance_row = df.agg(*variance_exprs).collect()[0].asDict() if variance_exprs else {}
    variances = {
        column: (
            float(variance_row[column])
            if variance_row.get(column) is not None and not math.isnan(float(variance_row[column]))
            else None
        )
        for column in review_features
    }
    feature_set = set(review_features)
    rows = []
    for base_count in ROLLING_BASE_COUNTS:
        total_feature = base_count if base_count in feature_set else None
        count_30d = f"{base_count}_30d" if f"{base_count}_30d" in feature_set else None
        for feature in [base_count, f"{base_count}_30d", f"{base_count}_60d", f"{base_count}_90d"]:
            if feature not in feature_set:
                continue
            previous_window = None
            next_window = None
            if feature.endswith("_60d"):
                previous_window = f"{base_count}_30d"
                next_window = f"{base_count}_90d"
            elif feature.endswith("_90d"):
                previous_window = f"{base_count}_60d"
            elif feature.endswith("_30d"):
                next_window = f"{base_count}_60d"
            previous_window = previous_window if previous_window in feature_set else None
            next_window = next_window if next_window in feature_set else None
            rows.append(
                {
                    "feature_name": feature,
                    "family": family_for_base_count(base_count),
                    "window_type": "total_count" if feature == base_count else feature.rsplit("_", 1)[-1],
                    "variance": variances.get(feature),
                    "correlation_with_total_count": safe_corr(df, feature, total_feature, variances)
                    if feature != total_feature
                    else 1.0,
                    "correlation_with_30d": safe_corr(df, feature, count_30d, variances)
                    if feature != count_30d
                    else 1.0,
                    "correlation_with_previous_window": safe_corr(df, feature, previous_window, variances),
                    "correlation_with_next_window": safe_corr(df, feature, next_window, variances),
                    "recommendation": recommendation_for_window(feature, removed_features),
                }
            )
    return rows


def write_window_selection_review(path: Path, rows: list[dict[str, Any]], removed_rolling_features: list[str]) -> None:
    lines = [
        "# Baseline V2-2 Rolling Window Selection Review",
        "",
        "## Objective",
        "",
        "Validate whether overlapping rolling-window count features can be simplified before adding any new behavioral features.",
        "",
        "## Selection Policy",
        "",
        "- Keep total count features as broad lifetime activity signals.",
        "- Keep 30-day count features as recent activity signals.",
        "- Remove standalone 60-day count features because they sit between 30-day and 90-day windows and showed high neighboring-window correlation in V2-1 review.",
        "- Keep 90-day count features in V2-2 so this experiment isolates 60-day removal. 90-day removal should be evaluated separately after V2-2 metrics are available.",
        "",
        "## Rolling Features Removed In V2-2",
        "",
    ]
    for feature in removed_rolling_features:
        lines.append(f"- `{feature}`")
    lines.extend(
        [
            "",
            "## Feature Review",
            "",
            "| Feature | Family | Window | Variance | Corr total | Corr 30d | Corr previous | Corr next | Recommendation |",
            "|---|---|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    for row in rows:
        lines.append(
            "| {feature_name} | {family} | {window_type} | {variance} | {correlation_with_total_count} | "
            "{correlation_with_30d} | {correlation_with_previous_window} | {correlation_with_next_window} | "
            "{recommendation} |".format(
                **{key: "" if value is None else normalize_value(value) for key, value in row.items()}
            )
        )
    lines.extend(
        [
            "",
            "## Privacy",
            "",
            "This review is aggregate-only. It contains feature-level variance and correlation statistics, not raw client IDs, raw query text, product names, row-level scores, or row-level examples.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def metric_delta(candidate: float, baseline: float) -> float:
    return V21.safe_divide(candidate - baseline, baseline)


def performance_decision(metrics: dict[str, Any]) -> str:
    pr_delta_vs_v21 = metric_delta(metrics["pr_auc"], BASELINE_V21_METRICS["pr_auc"])
    lift5_delta_vs_v21 = metric_delta(metrics["lift_at_5pct"], BASELINE_V21_METRICS["lift_at_5pct"])
    if pr_delta_vs_v21 >= -0.005 and lift5_delta_vs_v21 >= -0.005:
        return "Adopt V2-2 as new experimental baseline"
    if pr_delta_vs_v21 < -0.01 or lift5_delta_vs_v21 < -0.01:
        return "Keep V2-1"
    return "Investigate further"


def write_evaluation_report(
    path: Path,
    removed_features: list[str],
    removed_rolling_features: list[str],
    feature_count_e1: int,
    feature_count_v22: int,
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
        "# Baseline V2-2 Evaluation",
        "",
        "## Features Removed",
        "",
        "Baseline V2-2 starts from V2-1 and removes only standalone 60-day rolling count features.",
        "",
        "All removed features:",
    ]
    for feature in removed_features:
        lines.append(f"- `{feature}`")
    lines.extend(
        [
            "",
            "Rolling-window features removed in V2-2:",
        ]
    )
    for feature in removed_rolling_features:
        lines.append(f"- `{feature}`")
    lines.extend(
        [
            "",
            "## Metric Comparison",
            "",
            "| Metric | E1 temporal baseline | V2-1 | V2-2 | V2-2 vs V2-1 |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for label, key in metric_rows:
        lines.append(
            f"| {label} | {BASELINE_E1_METRICS[key]:.6f} | {BASELINE_V21_METRICS[key]:.6f} | "
            f"{metrics[key]:.6f} | {metric_delta(metrics[key], BASELINE_V21_METRICS[key]):.2%} |"
        )

    pr_delta_vs_v21 = metric_delta(metrics["pr_auc"], BASELINE_V21_METRICS["pr_auc"])
    lift5_delta_vs_v21 = metric_delta(metrics["lift_at_5pct"], BASELINE_V21_METRICS["lift_at_5pct"])
    if pr_delta_vs_v21 >= 0 and lift5_delta_vs_v21 >= 0:
        interpretation = "Rolling-window reduction improved the primary temporal ranking metrics relative to V2-1."
    elif pr_delta_vs_v21 >= -0.005 and lift5_delta_vs_v21 >= -0.005:
        interpretation = "Rolling-window reduction maintained temporal ranking performance within a small tolerance relative to V2-1."
    else:
        interpretation = "Rolling-window reduction reduced at least one primary temporal ranking metric enough to require review."

    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            interpretation,
            "",
            "V2-2 evaluates representation simplification only. It does not test search redesign, purchase cadence, remove-from-cart sequences, transition features, trend features, ratios, calibration, or a new model class.",
            "",
            "## Complexity Reduction",
            "",
            f"- E1 feature count: {feature_count_e1}",
            "- V2-1 feature count: 31",
            f"- V2-2 feature count: {feature_count_v22}",
            f"- Total features removed vs E1: {feature_count_e1 - feature_count_v22}",
            f"- Rolling-window features removed vs V2-1: {len(removed_rolling_features)}",
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


def evaluate_existing_score_model(
    model: Any,
    validation_df: DataFrame,
    validation_summary: dict[str, Any],
) -> dict[str, Any]:
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
    window_csv_path = artifact_dir / "v22_window_selection_review.csv"
    window_md_path = artifact_dir / "v22_window_selection_review.md"
    evaluation_path = artifact_dir / "v22_evaluation.md"

    feature_config = V21.load_feature_config(config_path)
    removed_features = list(feature_config["removed_features"])
    removed_feature_set = set(removed_features)
    removed_rolling_features = [feature for feature in removed_features if feature.endswith("_count_60d")]

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
        feature_columns_v22 = V21.numeric_feature_columns(train_base.schema, removed_feature_set)
        validation_feature_columns_v22 = V21.numeric_feature_columns(validation_base.schema, removed_feature_set)
        if validation_feature_columns_v22 != feature_columns_v22:
            raise ValueError("V2-2 train and validation feature columns differ")

        window_rows = build_window_selection_rows(train_base, feature_columns_before, removed_feature_set)
        window_fieldnames = [
            "feature_name",
            "family",
            "window_type",
            "variance",
            "correlation_with_total_count",
            "correlation_with_30d",
            "correlation_with_previous_window",
            "correlation_with_next_window",
            "recommendation",
        ]
        write_csv(window_csv_path, window_rows, window_fieldnames)
        write_window_selection_review(window_md_path, window_rows, removed_rolling_features)

        train_summary = V21.dataset_summary(train_base, "Train")
        validation_summary = V21.dataset_summary(validation_base, "Validation")
        positive_weight = train_summary["row_count"] / (2.0 * train_summary["positive_count"])
        negative_weight = train_summary["row_count"] / (2.0 * train_summary["negative_count"])
        train_df = V21.add_class_weight(train_base, positive_weight, negative_weight).cache()

        imputed_columns = [f"{column}__imputed" for column in feature_columns_v22]
        pipeline = V21.Pipeline(
            stages=[
                V21.Imputer(inputCols=feature_columns_v22, outputCols=imputed_columns, strategy="median"),
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
        metrics = evaluate_existing_score_model(model, validation_base, validation_summary)

        write_evaluation_report(
            evaluation_path,
            removed_features=removed_features,
            removed_rolling_features=removed_rolling_features,
            feature_count_e1=len(feature_columns_before),
            feature_count_v22=len(feature_columns_v22),
            metrics=metrics,
            train_summary=train_summary,
            validation_summary=validation_summary,
            model_output_path=model_output,
        )

        run_summary = {
            "experiment": "baseline_v2_2",
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "train_input": args.train_input,
            "validation_input": args.validation_input,
            "model_output": args.model_output,
            "feature_count_e1": len(feature_columns_before),
            "feature_count_v21": 31,
            "feature_count_v22": len(feature_columns_v22),
            "removed_features": removed_features,
            "removed_rolling_features": removed_rolling_features,
            "metrics": {key: normalize_value(value) for key, value in metrics.items()},
        }
        (artifact_dir / "v22_summary.json").write_text(json.dumps(run_summary, indent=2), encoding="utf-8")

        print("Baseline V2-2 training completed.")
        print(f"Features before: {len(feature_columns_before)}")
        print(f"Features after: {len(feature_columns_v22)}")
        print(f"Rolling-window features removed: {len(removed_rolling_features)}")
        print(f"ROC-AUC: {metrics['roc_auc']:.6f}")
        print(f"PR-AUC: {metrics['pr_auc']:.6f}")
        print(f"Lift@5%: {metrics['lift_at_5pct']:.6f}")
        print(f"Window review: {V21.relative_path(window_md_path)}")
        print(f"Evaluation: {V21.relative_path(evaluation_path)}")
        print(f"Model output: {V21.relative_path(model_output)}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
