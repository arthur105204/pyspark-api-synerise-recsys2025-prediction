"""Train Baseline V2-3c with one pruned transition flag.

This job isolates the strongest V2-3b transition signal by adding only
recent_search_then_cart_flag to Baseline V2-2. It uses the same E1 temporal
train/validation snapshots, Logistic Regression class, median imputation, class
weighting, and TopK evaluation pattern as E1/V2-1/V2-2.

It does not include search_before_cart_count, search_to_cart_rate, additional
transition features, search family redesign, query text features, embeddings,
session features, trend features, category transitions, label changes, temporal
split changes, calibration, hyperparameter tuning, or a new model class.
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
V21_JOB_PATH = PROJECT_ROOT / "jobs" / "05g_train_baseline_v21.py"
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "baseline_v23c_features.json"
DEFAULT_TRAIN_INPUT = (
    PROJECT_ROOT / "data" / "processed" / "training" / "e1_train_2022_10_10" / "purchase_propensity_30d"
)
DEFAULT_VALIDATION_INPUT = (
    PROJECT_ROOT / "data" / "processed" / "training" / "e1_valid_2022_11_09" / "purchase_propensity_30d"
)
DEFAULT_SEARCH_INPUT = PROJECT_ROOT / "data" / "processed" / "events" / "search_query"
DEFAULT_CART_INPUT = PROJECT_ROOT / "data" / "processed" / "events" / "add_to_cart"
DEFAULT_MODEL_OUTPUT = PROJECT_ROOT / "data" / "models" / "purchase_propensity_baseline_v23c"
DEFAULT_ARTIFACT_DIR = PROJECT_ROOT / "artifacts" / "modeling" / "baseline_v2"
DEFAULT_MAX_ITER = 20
TRAIN_CUTOFF_DATE = "2022-10-10"
VALIDATION_CUTOFF_DATE = "2022-11-09"
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
BASELINE_V23B_METRICS = {
    "roc_auc": 0.834653,
    "pr_auc": 0.255093,
    "precision_at_1pct": 0.485347,
    "precision_at_5pct": 0.296679,
    "precision_at_10pct": 0.215285,
    "lift_at_1pct": 11.145748,
    "lift_at_5pct": 6.813071,
    "lift_at_10pct": 4.943909,
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
    parser = argparse.ArgumentParser(description="Train Baseline V2-3c temporal model.")
    parser.add_argument("--feature-config", default=DEFAULT_CONFIG_PATH.relative_to(PROJECT_ROOT).as_posix())
    parser.add_argument("--train-input", default=DEFAULT_TRAIN_INPUT.relative_to(PROJECT_ROOT).as_posix())
    parser.add_argument("--validation-input", default=DEFAULT_VALIDATION_INPUT.relative_to(PROJECT_ROOT).as_posix())
    parser.add_argument("--search-input", default=DEFAULT_SEARCH_INPUT.relative_to(PROJECT_ROOT).as_posix())
    parser.add_argument("--cart-input", default=DEFAULT_CART_INPUT.relative_to(PROJECT_ROOT).as_posix())
    parser.add_argument("--model-output", default=DEFAULT_MODEL_OUTPUT.relative_to(PROJECT_ROOT).as_posix())
    parser.add_argument("--artifact-dir", default=DEFAULT_ARTIFACT_DIR.relative_to(PROJECT_ROOT).as_posix())
    parser.add_argument("--max-iter", type=int, default=DEFAULT_MAX_ITER)
    return parser.parse_args()


def start_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("train-baseline-v23c")
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


def recent_search_then_cart_for_cutoff(search_events: DataFrame, cart_events: DataFrame, cutoff_date: str) -> DataFrame:
    search_history = (
        filter_event_history(search_events, cutoff_date)
        .select(F.col("client_id"), F.col("event_ts").alias("search_ts"))
        .where(F.col("client_id").isNotNull() & F.col("search_ts").isNotNull())
        .where(F.to_date(F.col("search_ts")) >= F.date_sub(F.to_date(F.lit(cutoff_date)), RECENT_SEARCH_WINDOW_DAYS))
        .dropDuplicates(["client_id", "search_ts"])
    )
    cart_history = (
        filter_event_history(cart_events, cutoff_date)
        .select(F.col("client_id"), F.col("event_ts").alias("cart_ts"))
        .where(F.col("client_id").isNotNull() & F.col("cart_ts").isNotNull())
    )
    condition = (F.col("c.client_id") == F.col("s.client_id")) & (F.col("s.search_ts") <= F.col("c.cart_ts"))
    return (
        cart_history.alias("c")
        .join(search_history.alias("s"), condition, "left_semi")
        .select("client_id")
        .distinct()
        .withColumn("recent_search_then_cart_flag", F.lit(1).cast("int"))
    )


def add_recent_flag(base_df: DataFrame, flag_df: DataFrame) -> DataFrame:
    return (
        base_df.join(flag_df, "client_id", "left")
        .fillna({"recent_search_then_cart_flag": 0})
        .withColumn("recent_search_then_cart_flag", F.col("recent_search_then_cart_flag").cast("int"))
    )


def feature_stats(df: DataFrame, row_count: int) -> dict[str, Any]:
    feature_name = "recent_search_then_cart_flag"
    row = df.agg(
        F.count(F.col(feature_name)).alias("non_null_count"),
        F.approx_count_distinct(F.col(feature_name)).alias("distinct_count"),
        F.mean(F.col(feature_name).cast("double")).alias("mean"),
        F.stddev(F.col(feature_name).cast("double")).alias("stddev"),
        F.min(F.col(feature_name)).alias("min"),
        F.max(F.col(feature_name)).alias("max"),
        F.sum(F.when(F.col(feature_name) == 0, F.lit(1)).otherwise(F.lit(0))).alias("zero_count"),
    ).collect()[0].asDict()
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
        "max": row["max"],
        "zero_count": zero_count,
        "zero_rate": safe_divide(zero_count, row_count),
    }


def positive_rate_rows(df: DataFrame) -> list[dict[str, Any]]:
    feature_name = "recent_search_then_cart_flag"
    rows = (
        df.groupBy(feature_name)
        .agg(F.count(F.lit(1)).alias("row_count"), F.sum(F.col("label").cast("long")).alias("positive_count"))
        .orderBy(feature_name)
        .collect()
    )
    result = []
    for row in rows:
        row_count = int(row["row_count"] or 0)
        positive_count = int(row["positive_count"] or 0)
        result.append(
            {
                "feature_name": feature_name,
                "bucket": int(row[feature_name]),
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
        return "Adopt V2-3c"
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


def coefficient_row(model: Any, feature_columns: list[str]) -> dict[str, Any]:
    lr_model = model.stages[-1]
    coefficients = lr_model.coefficients.toArray().tolist()
    feature_name = "recent_search_then_cart_flag"
    index = feature_columns.index(feature_name)
    coefficient = float(coefficients[index])
    ranked = sorted(
        [(feature, abs(float(coefficients[idx]))) for idx, feature in enumerate(feature_columns)],
        key=lambda item: item[1],
        reverse=True,
    )
    ranks = {feature: rank + 1 for rank, (feature, _) in enumerate(ranked)}
    return {
        "feature_name": feature_name,
        "coefficient": coefficient,
        "abs_coefficient": abs(coefficient),
        "rank_among_all_features": ranks[feature_name],
    }


def write_evaluation_report(
    path: Path,
    feature_count_e1: int,
    feature_count_v23c: int,
    metrics: dict[str, Any],
    train_summary: dict[str, Any],
    validation_summary: dict[str, Any],
    model_output_path: Path,
    coefficient: dict[str, Any],
    distribution: dict[str, Any],
    positive_rates: list[dict[str, Any]],
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
        interpretation = "The single transition flag improved the primary temporal ranking metrics relative to V2-2."
    elif pr_delta >= -0.005 and lift5_delta >= -0.005:
        interpretation = "The single transition flag maintained temporal ranking metrics within a small tolerance relative to V2-2."
    else:
        interpretation = "The single transition flag reduced at least one primary temporal ranking metric enough to prefer V2-2."

    lines = [
        "# Baseline V2-3c Evaluation",
        "",
        "## Feature Change",
        "",
        "Start from Baseline V2-2 and add only:",
        "",
        "- `recent_search_then_cart_flag`",
        "",
        "Removed from the V2-3b transition set:",
        "",
        "- `search_before_cart_count`",
        "- `search_to_cart_rate`",
        "",
        "## Metric Comparison",
        "",
        "| Metric | E1 temporal baseline | V2-2 | V2-3b | V2-3c | V2-3c vs V2-2 |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for label, key in metric_rows:
        lines.append(
            f"| {label} | {BASELINE_E1_METRICS[key]:.6f} | {BASELINE_V22_METRICS[key]:.6f} | "
            f"{BASELINE_V23B_METRICS[key]:.6f} | {metrics[key]:.6f} | {metric_delta(metrics[key], BASELINE_V22_METRICS[key]):.2%} |"
        )
    lines.extend(
        [
            "",
            "## Transition Signal Value",
            "",
            interpretation,
            "",
            "| Feature | Coefficient | Abs coefficient | Abs-coefficient rank among model features |",
            "|---|---:|---:|---:|",
            f"| {coefficient['feature_name']} | {coefficient['coefficient']:.6f} | {coefficient['abs_coefficient']:.6f} | {coefficient['rank_among_all_features']} |",
            "",
            "Distribution:",
            "",
            f"- Non-null rate: {distribution['non_null_rate']:.6f}",
            f"- Mean: {distribution['mean']:.6f}",
            f"- Zero rate: {distribution['zero_rate']:.6f}",
            f"- Distinct values: {distribution['distinct_count']}",
            "",
            "Positive-rate segmentation:",
            "",
            "| Flag value | Row count | Positive count | Positive rate |",
            "|---:|---:|---:|---:|",
        ]
    )
    for row in positive_rates:
        lines.append(f"| {row['bucket']} | {row['row_count']} | {row['positive_count']} | {row['positive_rate']:.6f} |")
    lines.extend(
        [
            "",
            "## Redundancy Analysis",
            "",
            "V2-3c removes the two noisier V2-3b transition features and keeps only the flag with the cleanest behavioral interpretation. This isolates whether recent search followed by cart activity is independently additive to the V2-2 baseline.",
            "",
            "The experiment does not test full transition engineering, search family redesign, sessionization, query semantics, trend features, or category-aware behavior.",
            "",
            "## Complexity Added",
            "",
            f"- E1 feature count: {feature_count_e1}",
            "- V2-2 feature count: 27",
            "- V2-3b feature count: 30",
            f"- V2-3c feature count: {feature_count_v23c}",
            "- New transition features added: 1",
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
    evaluation_path = artifact_dir / "v23c_evaluation.md"
    summary_path = artifact_dir / "v23c_summary.json"
    distribution_path = artifact_dir / "v23c_feature_distribution.csv"
    positive_rate_path = artifact_dir / "v23c_positive_rate.csv"
    coefficient_path = artifact_dir / "v23c_coefficient.csv"

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

        train_flag = recent_search_then_cart_for_cutoff(search_events, cart_events, TRAIN_CUTOFF_DATE).cache()
        validation_flag = recent_search_then_cart_for_cutoff(search_events, cart_events, VALIDATION_CUTOFF_DATE).cache()
        train_augmented = add_recent_flag(train_base, train_flag).cache()
        validation_augmented = add_recent_flag(validation_base, validation_flag).cache()

        feature_columns_v23c = V21.numeric_feature_columns(train_augmented.schema, removed_feature_set)
        validation_feature_columns_v23c = V21.numeric_feature_columns(validation_augmented.schema, removed_feature_set)
        if validation_feature_columns_v23c != feature_columns_v23c:
            raise ValueError("V2-3c train and validation feature columns differ")
        missing_engineered = sorted(set(engineered_features).difference(feature_columns_v23c))
        if missing_engineered:
            raise ValueError(f"Engineered features missing from model input: {missing_engineered}")

        train_summary = V21.dataset_summary(train_augmented, "Train")
        validation_summary = V21.dataset_summary(validation_augmented, "Validation")
        distribution = feature_stats(train_augmented, train_summary["row_count"])
        positive_rates = positive_rate_rows(train_augmented)
        write_csv(
            distribution_path,
            [distribution],
            ["feature_name", "non_null_count", "non_null_rate", "distinct_count", "mean", "stddev", "min", "max", "zero_count", "zero_rate"],
        )
        write_csv(positive_rate_path, positive_rates, ["feature_name", "bucket", "row_count", "positive_count", "positive_rate"])

        positive_weight = train_summary["row_count"] / (2.0 * train_summary["positive_count"])
        negative_weight = train_summary["row_count"] / (2.0 * train_summary["negative_count"])
        train_df = V21.add_class_weight(train_augmented, positive_weight, negative_weight).cache()
        imputed_columns = [f"{column}__imputed" for column in feature_columns_v23c]
        pipeline = V21.Pipeline(
            stages=[
                V21.Imputer(inputCols=feature_columns_v23c, outputCols=imputed_columns, strategy="median"),
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
        coefficient = coefficient_row(model, feature_columns_v23c)
        write_csv(coefficient_path, [coefficient], ["feature_name", "coefficient", "abs_coefficient", "rank_among_all_features"])
        write_evaluation_report(
            evaluation_path,
            feature_count_e1=len(feature_columns_before),
            feature_count_v23c=len(feature_columns_v23c),
            metrics=metrics,
            train_summary=train_summary,
            validation_summary=validation_summary,
            model_output_path=model_output,
            coefficient=coefficient,
            distribution=distribution,
            positive_rates=positive_rates,
        )

        run_summary = {
            "experiment": "baseline_v2_3c",
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "train_input": args.train_input,
            "validation_input": args.validation_input,
            "search_input": args.search_input,
            "cart_input": args.cart_input,
            "model_output": args.model_output,
            "feature_count_e1": len(feature_columns_before),
            "feature_count_v22": 27,
            "feature_count_v23b": 30,
            "feature_count_v23c": len(feature_columns_v23c),
            "removed_features": removed_features,
            "engineered_features": engineered_features,
            "pruned_features_from_v23b": feature_config.get("pruned_features_from_v23b", []),
            "recent_search_window_days": RECENT_SEARCH_WINDOW_DAYS,
            "metrics": {key: normalize_value(value) for key, value in metrics.items()},
        }
        summary_path.write_text(json.dumps(run_summary, indent=2), encoding="utf-8")

        print("Baseline V2-3c training completed.")
        print(f"Features before: {len(feature_columns_before)}")
        print(f"Features after: {len(feature_columns_v23c)}")
        print("Engineered transition features: 1")
        print(f"ROC-AUC: {metrics['roc_auc']:.6f}")
        print(f"PR-AUC: {metrics['pr_auc']:.6f}")
        print(f"Lift@5%: {metrics['lift_at_5pct']:.6f}")
        print(f"Evaluation: {V21.relative_path(evaluation_path)}")
        print(f"Model output: {V21.relative_path(model_output)}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
