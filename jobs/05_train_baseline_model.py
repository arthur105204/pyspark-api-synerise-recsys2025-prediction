"""Train a baseline purchase propensity model with Spark ML.

This Phase 5 job reads the Phase 4 training-ready dataset, trains a baseline
Logistic Regression model, writes the Spark ML model locally, and writes
sanitized aggregate-only modeling artifacts.

It does not create API serving code, production batch scoring outputs,
row-level prediction artifacts, or client-level exports.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import shutil
import sys
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
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "pipeline_config.yaml"
DEFAULT_INPUT_PATH = PROJECT_ROOT / "data" / "processed" / "training" / "purchase_propensity_30d"
DEFAULT_TEMPORAL_TRAIN_INPUT = PROJECT_ROOT / "data" / "processed" / "training" / "e1_train_2022_10_10" / "purchase_propensity_30d"
DEFAULT_TEMPORAL_VALIDATION_INPUT = PROJECT_ROOT / "data" / "processed" / "training" / "e1_valid_2022_11_09" / "purchase_propensity_30d"
DEFAULT_MODEL_OUTPUT = PROJECT_ROOT / "data" / "models" / "purchase_propensity_baseline"
DEFAULT_TEMPORAL_MODEL_OUTPUT = PROJECT_ROOT / "data" / "models" / "purchase_propensity_baseline_temporal"
DEFAULT_ARTIFACTS_BASE = PROJECT_ROOT / "artifacts"
DEFAULT_TEMPORAL_ARTIFACT_DIR = "modeling/e1_temporal_validation"
DEFAULT_TEMPORAL_TRAIN_SNAPSHOT = "e1_train_2022_10_10"
DEFAULT_TEMPORAL_VALIDATION_SNAPSHOT = "e1_valid_2022_11_09"
DEFAULT_SEED = 42
DEFAULT_SAMPLE_FRACTION = 1.0
DEFAULT_MAX_ITER = 20
EXCLUDED_COLUMNS = {
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
TOPK_PERCENTS = (0.01, 0.05, 0.10)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train a baseline purchase propensity model.")
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative pipeline config path. Default: configs/pipeline_config.yaml.",
    )
    parser.add_argument(
        "--input-path",
        default=DEFAULT_INPUT_PATH.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative Phase 4 training dataset path.",
    )
    parser.add_argument(
        "--model-output",
        default=DEFAULT_MODEL_OUTPUT.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative Spark ML model output path.",
    )
    parser.add_argument(
        "--artifacts-base",
        default=DEFAULT_ARTIFACTS_BASE.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative artifacts base path. Default: artifacts.",
    )
    parser.add_argument(
        "--sample-fraction",
        type=float,
        default=DEFAULT_SAMPLE_FRACTION,
        help="Optional Spark sample fraction for debugging. Use 1.0 for final metrics.",
    )
    parser.add_argument(
        "--mode",
        choices=["random", "temporal"],
        default="random",
        help="Evaluation mode. Default random preserves the existing random 80/20 split workflow.",
    )
    parser.add_argument(
        "--validation-input-path",
        default=DEFAULT_TEMPORAL_VALIDATION_INPUT.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative validation snapshot training dataset path for --mode temporal.",
    )
    parser.add_argument(
        "--train-snapshot-name",
        default=DEFAULT_TEMPORAL_TRAIN_SNAPSHOT,
        help="Snapshot name recorded in temporal mode summary.",
    )
    parser.add_argument(
        "--validation-snapshot-name",
        default=DEFAULT_TEMPORAL_VALIDATION_SNAPSHOT,
        help="Validation snapshot name recorded in temporal mode summary.",
    )
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="Deterministic split seed.")
    parser.add_argument("--max-iter", type=int, default=DEFAULT_MAX_ITER, help="Logistic Regression max iterations.")
    parser.add_argument(
        "--no-class-weights",
        action="store_true",
        help="Disable class weights. Class weights are enabled by default for imbalance handling.",
    )
    return parser.parse_args()


def relative_path(path: Path) -> str:
    return path.relative_to(PROJECT_ROOT).as_posix()


def resolve_repo_path(path_text: str) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        raise ValueError("path arguments must be repo-relative")
    return PROJECT_ROOT / path


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


def start_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("train-purchase-propensity-baseline")
        .master("local[*]")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def clean_output_dir(path: Path, allowed_base: Path) -> None:
    resolved = path.resolve()
    allowed_root = allowed_base.resolve()
    if not str(resolved).startswith(str(allowed_root)):
        raise ValueError("Refusing to clean an output directory outside the allowed base")
    if path.exists():
        shutil.rmtree(path)


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
            writer.writerow({key: normalize_value(value) for key, value in row.items()})


def write_notes(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Baseline Model Notes",
        "",
        "This artifact contains aggregate-only modeling notes.",
        "",
        f"Task: {summary['task']}.",
        f"Model type: {summary['model_type']}.",
        f"Feature count: {summary['feature_count']}.",
        f"ROC-AUC: {summary['roc_auc']}.",
        f"PR-AUC: {summary['pr_auc']}.",
        f"Class weighting enabled: {summary['class_weighting_enabled']}.",
        "No row-level predictions, client ids, API endpoint, or production scoring output was created.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def numeric_feature_columns(schema: T.StructType) -> list[str]:
    numeric_types = (
        T.ByteType,
        T.ShortType,
        T.IntegerType,
        T.LongType,
        T.FloatType,
        T.DoubleType,
        T.DecimalType,
    )
    columns: list[str] = []
    for field in schema.fields:
        name = field.name
        lowered = name.lower()
        if name in EXCLUDED_COLUMNS:
            continue
        if lowered in {"target", "y"} or lowered.startswith(LABEL_LIKE_PREFIXES):
            continue
        if isinstance(field.dataType, numeric_types):
            columns.append(name)
    return columns


def label_like_columns(columns: list[str]) -> list[str]:
    flagged: list[str] = []
    for column in columns:
        lowered = column.lower()
        if column in EXCLUDED_COLUMNS:
            continue
        if lowered in {"target", "y"} or lowered.startswith(LABEL_LIKE_PREFIXES):
            flagged.append(column)
    return flagged


def duplicate_client_count(df: DataFrame) -> int:
    row = (
        df.groupBy("client_id")
        .count()
        .where(F.col("count") > 1)
        .agg(F.count(F.lit(1)).alias("duplicate_client_id_count"))
        .collect()[0]
    )
    return int(row["duplicate_client_id_count"] or 0)


def positive_count(df: DataFrame) -> int:
    return int(df.where(F.col("label") == 1).count())


def validate_modeling_dataset(df: DataFrame, dataset_name: str) -> dict[str, Any]:
    required_columns = {"client_id", "label"}
    missing_required = sorted(required_columns.difference(df.columns))
    if missing_required:
        raise ValueError(f"{dataset_name} dataset is missing required columns: {', '.join(missing_required)}")
    label_null_count = df.where(F.col("label").isNull()).count()
    invalid_label_count = df.where(~F.col("label").isin([0, 1]) | F.col("label").isNull()).count()
    duplicate_count = duplicate_client_count(df)
    row_count = df.count()
    positive_rows = positive_count(df)
    negative_rows = row_count - positive_rows
    positive_rate = positive_rows / row_count if row_count else 0.0
    if not row_count or not positive_rows or not negative_rows:
        raise ValueError(f"{dataset_name} dataset must contain positive and negative rows")
    if label_null_count or invalid_label_count or duplicate_count:
        raise ValueError(f"{dataset_name} dataset failed label or duplicate validation")
    return {
        "row_count": row_count,
        "positive_count": positive_rows,
        "negative_count": negative_rows,
        "positive_rate": positive_rate,
        "label_null_count": label_null_count,
        "invalid_label_count": invalid_label_count,
        "duplicate_client_id_count": duplicate_count,
    }


def confusion_metrics(predictions: DataFrame, threshold: float) -> dict[str, Any]:
    scored = predictions.withColumn("predicted_label", (F.col("prediction_score") >= threshold).cast("int"))
    row = scored.agg(
        F.sum(F.when((F.col("label") == 1) & (F.col("predicted_label") == 1), 1).otherwise(0)).alias("tp"),
        F.sum(F.when((F.col("label") == 0) & (F.col("predicted_label") == 1), 1).otherwise(0)).alias("fp"),
        F.sum(F.when((F.col("label") == 0) & (F.col("predicted_label") == 0), 1).otherwise(0)).alias("tn"),
        F.sum(F.when((F.col("label") == 1) & (F.col("predicted_label") == 0), 1).otherwise(0)).alias("fn"),
    ).collect()[0]
    tp = int(row["tp"] or 0)
    fp = int(row["fp"] or 0)
    tn = int(row["tn"] or 0)
    fn = int(row["fn"] or 0)
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if precision + recall else 0.0
    return {
        "tp": tp,
        "fp": fp,
        "tn": tn,
        "fn": fn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


def topk_metrics(predictions: DataFrame, test_count: int, test_positive_count: int, test_positive_rate: float) -> list[dict[str, Any]]:
    scored = predictions.select("label", "prediction_score")
    rows: list[dict[str, Any]] = []
    for percent in TOPK_PERCENTS:
        top_k_count = max(1, int(math.ceil(test_count * percent)))
        row = scored.orderBy(F.desc("prediction_score")).limit(top_k_count).agg(
            F.count(F.lit(1)).alias("top_k_count"),
            F.sum(F.col("label").cast("long")).alias("positive_count"),
        ).collect()[0]
        observed_top_k_count = int(row["top_k_count"] or 0)
        observed_positive_count = int(row["positive_count"] or 0)
        precision_at_k = observed_positive_count / observed_top_k_count if observed_top_k_count else 0.0
        recall_at_k = observed_positive_count / test_positive_count if test_positive_count else 0.0
        lift_at_k = precision_at_k / test_positive_rate if test_positive_rate else 0.0
        rows.append(
            {
                "k_percent": percent,
                "top_k_count": observed_top_k_count,
                "positive_count": observed_positive_count,
                "precision_at_k": precision_at_k,
                "recall_at_k": recall_at_k,
                "lift_at_k": lift_at_k,
            }
        )
    return rows


def main() -> int:
    args = parse_args()
    if not 0 < args.sample_fraction <= 1:
        raise ValueError("--sample-fraction must be greater than 0 and at most 1.0")

    config_path = resolve_repo_path(args.config)
    input_path = resolve_repo_path(args.input_path)
    validation_input_path = resolve_repo_path(args.validation_input_path)
    default_input_arg = DEFAULT_INPUT_PATH.relative_to(PROJECT_ROOT).as_posix()
    default_model_arg = DEFAULT_MODEL_OUTPUT.relative_to(PROJECT_ROOT).as_posix()
    if args.mode == "temporal" and args.input_path == default_input_arg:
        input_path = DEFAULT_TEMPORAL_TRAIN_INPUT
    if args.mode == "temporal" and args.model_output == default_model_arg:
        model_output_path = DEFAULT_TEMPORAL_MODEL_OUTPUT
    else:
        model_output_path = resolve_repo_path(args.model_output)
    artifacts_base = resolve_repo_path(args.artifacts_base)
    modeling_artifact_dir = (
        artifacts_base / DEFAULT_TEMPORAL_ARTIFACT_DIR if args.mode == "temporal" else artifacts_base / "modeling"
    )
    summary_path = modeling_artifact_dir / "baseline_model_summary.json"
    baseline_metrics_path = modeling_artifact_dir / "baseline_metrics.csv"
    topk_metrics_path = modeling_artifact_dir / "topk_metrics.csv"
    feature_processing_path = modeling_artifact_dir / "feature_processing_summary.csv"
    notes_path = modeling_artifact_dir / "baseline_model_notes.md"

    config = read_simple_yaml(config_path)
    target_config = config.get("target", {})
    task = str(target_config.get("task"))
    seed = int(args.seed)
    use_class_weights = not args.no_class_weights
    threshold = 0.5

    spark = start_spark()
    try:
        dataset = spark.read.parquet(str(input_path))
        validation_dataset = None
        if args.mode == "temporal":
            validation_dataset = spark.read.parquet(str(validation_input_path)).cache()
        if args.sample_fraction < 1.0:
            dataset = dataset.sample(withReplacement=False, fraction=args.sample_fraction, seed=seed)
            if validation_dataset is not None:
                validation_dataset = validation_dataset.sample(withReplacement=False, fraction=args.sample_fraction, seed=seed)
        dataset = dataset.cache()

        label_like = label_like_columns(dataset.columns)
        feature_columns = numeric_feature_columns(dataset.schema)
        if not feature_columns:
            raise ValueError("No numeric feature columns found for modeling")
        leakage_feature_columns = sorted(set(feature_columns).intersection(EXCLUDED_COLUMNS).union(label_like))
        if leakage_feature_columns:
            raise ValueError("Label-like or metadata columns were selected as features")
        if validation_dataset is not None:
            validation_label_like = label_like_columns(validation_dataset.columns)
            validation_feature_columns = numeric_feature_columns(validation_dataset.schema)
            if validation_label_like:
                raise ValueError("Validation dataset contains label-like metadata columns")
            if validation_feature_columns != feature_columns:
                raise ValueError("Temporal validation dataset feature columns do not match training dataset")

        train_dataset_metrics = validate_modeling_dataset(dataset, "Training")
        total_count = train_dataset_metrics["row_count"]
        label_null_count = train_dataset_metrics["label_null_count"]
        invalid_label_count = train_dataset_metrics["invalid_label_count"]
        duplicate_count = train_dataset_metrics["duplicate_client_id_count"]
        total_positive_count = train_dataset_metrics["positive_count"]
        total_negative_count = train_dataset_metrics["negative_count"]
        positive_rate = train_dataset_metrics["positive_rate"]
        validation_dataset_metrics = (
            validate_modeling_dataset(validation_dataset, "Temporal validation") if validation_dataset is not None else None
        )

        positive_weight = total_count / (2 * total_positive_count)
        negative_weight = total_count / (2 * total_negative_count)
        prepared = dataset
        if use_class_weights:
            prepared = prepared.withColumn(
                "class_weight",
                F.when(F.col("label") == 1, F.lit(float(positive_weight))).otherwise(F.lit(float(negative_weight))),
            )

        null_exprs = [
            F.sum(F.when(F.col(column).isNull() | F.isnan(F.col(column).cast("double")), 1).otherwise(0)).alias(column)
            for column in feature_columns
        ]
        null_counts_row = prepared.agg(*null_exprs).collect()[0].asDict()
        feature_processing_rows = [
            {
                "feature_name": column,
                "input_type": str(prepared.schema[column].dataType),
                "null_count_before_imputation": int(null_counts_row.get(column) or 0),
                "imputation_strategy": "median",
                "used_in_model": True,
            }
            for column in feature_columns
        ]

        if args.mode == "random":
            train_df, test_df = prepared.randomSplit([0.8, 0.2], seed=seed)
            split_strategy = "random 80/20 split"
            evaluation_input_path = input_path
        else:
            train_df = prepared
            test_df = validation_dataset
            split_strategy = "temporal snapshot holdout"
            evaluation_input_path = validation_input_path
        train_df = train_df.cache()
        test_df = test_df.cache()
        train_count = train_df.count()
        test_count = test_df.count()
        train_positive_count = positive_count(train_df)
        test_positive_count = positive_count(test_df)
        train_positive_rate = train_positive_count / train_count if train_count else 0.0
        test_positive_rate = test_positive_count / test_count if test_count else 0.0
        if not train_count or not test_count or not train_positive_count or not test_positive_count:
            raise ValueError("Train/test split must produce nonzero rows and positives")

        imputed_columns = [f"{column}__imputed" for column in feature_columns]
        imputer = Imputer(inputCols=feature_columns, outputCols=imputed_columns, strategy="median")
        assembler = VectorAssembler(inputCols=imputed_columns, outputCol="features")
        lr_kwargs: dict[str, Any] = {
            "featuresCol": "features",
            "labelCol": "label",
            "maxIter": int(args.max_iter),
            "standardization": True,
        }
        if use_class_weights:
            lr_kwargs["weightCol"] = "class_weight"
        lr = LogisticRegression(**lr_kwargs)
        pipeline = Pipeline(stages=[imputer, assembler, lr])
        model = pipeline.fit(train_df)

        clean_output_dir(model_output_path, PROJECT_ROOT / "data" / "models")
        model.write().overwrite().save(str(model_output_path))

        predictions = model.transform(test_df).select(
            "label",
            vector_to_array(F.col("probability"))[1].alias("prediction_score"),
            "prediction",
        ).cache()

        roc_auc = BinaryClassificationEvaluator(
            labelCol="label", rawPredictionCol="prediction_score", metricName="areaUnderROC"
        ).evaluate(predictions)
        pr_auc = BinaryClassificationEvaluator(
            labelCol="label", rawPredictionCol="prediction_score", metricName="areaUnderPR"
        ).evaluate(predictions)
        confusion = confusion_metrics(predictions, threshold)
        topk_rows = topk_metrics(predictions, test_count, test_positive_count, test_positive_rate)

        model_output_exists = model_output_path.exists()
        leakage_passed = (
            not label_like
            and not leakage_feature_columns
            and "label" not in feature_columns
            and "target_window_start" not in feature_columns
            and "target_window_end" not in feature_columns
            and "target_event_count" not in feature_columns
            and model_output_exists
        )
        summary = {
            "generated_at_date": datetime.now(timezone.utc).date().isoformat(),
            "generated_at_timestamp": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "phase": "Phase 5: Baseline Modeling",
            "status": "success",
            "mode": args.mode,
            "task": task,
            "model_type": "LogisticRegression",
            "input_training_dataset_path": relative_path(input_path),
            "input_validation_dataset_path": relative_path(evaluation_input_path),
            "train_snapshot_name": args.train_snapshot_name if args.mode == "temporal" else None,
            "validation_snapshot_name": args.validation_snapshot_name if args.mode == "temporal" else None,
            "model_output_path": relative_path(model_output_path),
            "row_count": total_count,
            "positive_count": total_positive_count,
            "negative_count": total_negative_count,
            "positive_rate": positive_rate,
            "train_rows": train_count,
            "test_rows": test_count,
            "validation_rows": test_count,
            "train_positive_count": train_positive_count,
            "test_positive_count": test_positive_count,
            "validation_positive_count": test_positive_count,
            "train_positive_rate": train_positive_rate,
            "test_positive_rate": test_positive_rate,
            "validation_positive_rate": test_positive_rate,
            "feature_count": len(feature_columns),
            "imputation_strategy": "median",
            "class_weighting_enabled": use_class_weights,
            "positive_class_weight": positive_weight if use_class_weights else None,
            "negative_class_weight": negative_weight if use_class_weights else None,
            "split_strategy": split_strategy,
            "seed": seed,
            "sample_fraction": args.sample_fraction,
            "max_iter": int(args.max_iter),
            "roc_auc": roc_auc,
            "pr_auc": pr_auc,
            "threshold": threshold,
            "confusion_matrix": {
                "tp": confusion["tp"],
                "fp": confusion["fp"],
                "tn": confusion["tn"],
                "fn": confusion["fn"],
            },
            "threshold_metrics": {
                "precision": confusion["precision"],
                "recall": confusion["recall"],
                "f1": confusion["f1"],
            },
            "topk_percents": list(TOPK_PERCENTS),
            "validation": {
                "label_null_count": label_null_count,
                "invalid_label_count": invalid_label_count,
                "duplicate_client_id_count": duplicate_count,
                "validation_label_null_count": (
                    validation_dataset_metrics["label_null_count"] if validation_dataset_metrics else None
                ),
                "validation_invalid_label_count": (
                    validation_dataset_metrics["invalid_label_count"] if validation_dataset_metrics else None
                ),
                "validation_duplicate_client_id_count": (
                    validation_dataset_metrics["duplicate_client_id_count"] if validation_dataset_metrics else None
                ),
                "model_output_exists": model_output_exists,
            },
            "leakage_validation": {
                "input_dataset_source": (
                    "Phase 4 temporal snapshot training datasets" if args.mode == "temporal" else "Phase 4 training dataset"
                ),
                "phase_4_labels_leakage_safe": True,
                "target_window_features_created": False,
                "excluded_columns": sorted(EXCLUDED_COLUMNS),
                "label_like_columns_in_features": label_like,
                "passed": leakage_passed,
            },
            "api_created": False,
            "production_scoring_output_created": False,
            "privacy": {
                "artifact_level": "aggregate_only",
                "row_level_predictions_persisted": False,
                "raw_client_ids_persisted": False,
                "raw_query_text_persisted": False,
                "product_names_persisted": False,
            },
        }

        if args.mode == "random":
            baseline_metric_rows = [
                {"metric_name": "train_rows", "value": train_count, "notes": "Rows in deterministic train split."},
                {"metric_name": "test_rows", "value": test_count, "notes": "Rows in deterministic test split."},
                {"metric_name": "train_positive_rate", "value": train_positive_rate, "notes": "Positive rate in train split."},
                {"metric_name": "test_positive_rate", "value": test_positive_rate, "notes": "Positive rate in test split."},
            ]
        else:
            baseline_metric_rows = [
                {"metric_name": "mode", "value": args.mode, "notes": "Evaluation mode used for this run."},
                {"metric_name": "train_rows", "value": train_count, "notes": "Rows used for model training."},
                {"metric_name": "validation_rows", "value": test_count, "notes": "Rows used for model evaluation."},
                {"metric_name": "train_positive_rate", "value": train_positive_rate, "notes": "Positive rate in training rows."},
                {
                    "metric_name": "validation_positive_rate",
                    "value": test_positive_rate,
                    "notes": "Positive rate in evaluation rows.",
                },
            ]
        baseline_metric_rows.extend(
            [
            {"metric_name": "roc_auc", "value": roc_auc, "notes": "Area under ROC curve."},
            {"metric_name": "pr_auc", "value": pr_auc, "notes": "Area under precision-recall curve."},
            {"metric_name": "threshold", "value": threshold, "notes": "Threshold used for confusion matrix."},
            {"metric_name": "tp", "value": confusion["tp"], "notes": "True positives at threshold."},
            {"metric_name": "fp", "value": confusion["fp"], "notes": "False positives at threshold."},
            {"metric_name": "tn", "value": confusion["tn"], "notes": "True negatives at threshold."},
            {"metric_name": "fn", "value": confusion["fn"], "notes": "False negatives at threshold."},
            {"metric_name": "precision", "value": confusion["precision"], "notes": "Precision at threshold."},
            {"metric_name": "recall", "value": confusion["recall"], "notes": "Recall at threshold."},
            {"metric_name": "f1", "value": confusion["f1"], "notes": "F1 at threshold."},
            ]
        )

        write_json(summary_path, summary)
        write_csv(baseline_metrics_path, baseline_metric_rows, ["metric_name", "value", "notes"])
        write_csv(
            topk_metrics_path,
            topk_rows,
            ["k_percent", "top_k_count", "positive_count", "precision_at_k", "recall_at_k", "lift_at_k"],
        )
        write_csv(
            feature_processing_path,
            feature_processing_rows,
            ["feature_name", "input_type", "null_count_before_imputation", "imputation_strategy", "used_in_model"],
        )
        write_notes(notes_path, summary)

        artifact_paths = [summary_path, baseline_metrics_path, topk_metrics_path, feature_processing_path, notes_path]
        artifacts_exist = all(path.exists() for path in artifact_paths)
        if not artifacts_exist or not leakage_passed:
            return 1

        print("Baseline modeling completed.")
        print(f"Mode: {args.mode}")
        print(f"Train rows: {train_count}")
        print(f"Evaluation rows: {test_count}")
        print(f"ROC-AUC: {round(roc_auc, 6)}")
        print(f"PR-AUC: {round(pr_auc, 6)}")
        print(f"Model output: {relative_path(model_output_path)}")
        print(f"Sanitized artifacts: {relative_path(modeling_artifact_dir)}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
