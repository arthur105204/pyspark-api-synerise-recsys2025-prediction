"""Generate batch purchase propensity scores with a trained Spark ML model.

This Phase 6 job loads the Phase 5 baseline Spark ML model, scores eligible
clients from the Phase 3 feature table, writes a local serving-ready Parquet
score table, and writes sanitized aggregate-only scoring artifacts.

It does not implement API serving, create an online endpoint, retrain a model,
alter labels, or write row-level prediction artifacts for commit.
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

from pyspark.ml.functions import vector_to_array
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "pipeline_config.yaml"
DEFAULT_FEATURE_INPUT = PROJECT_ROOT / "data" / "processed" / "features" / "user_behavior_features"
DEFAULT_MODEL_INPUT = PROJECT_ROOT / "data" / "models" / "purchase_propensity_baseline"
DEFAULT_SCORE_OUTPUT = PROJECT_ROOT / "data" / "processed" / "scoring" / "purchase_propensity_scores"
DEFAULT_ARTIFACTS_BASE = PROJECT_ROOT / "artifacts"
DEFAULT_MODEL_VERSION = "baseline_lr_v1"
DEFAULT_SCORE_THRESHOLD = 0.5
FORBIDDEN_OUTPUT_COLUMNS = {"label", "target_window_start", "target_window_end", "target_event_count"}
SCORE_BUCKETS = [
    ("0.0-0.1", 0.0, 0.1),
    ("0.1-0.2", 0.1, 0.2),
    ("0.2-0.3", 0.2, 0.3),
    ("0.3-0.4", 0.3, 0.4),
    ("0.4-0.5", 0.4, 0.5),
    ("0.5-0.6", 0.5, 0.6),
    ("0.6-0.7", 0.6, 0.7),
    ("0.7-0.8", 0.7, 0.8),
    ("0.8-0.9", 0.8, 0.9),
    ("0.9-1.0", 0.9, 1.0),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate batch purchase propensity scores.")
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative pipeline config path. Default: configs/pipeline_config.yaml.",
    )
    parser.add_argument(
        "--feature-input",
        default=DEFAULT_FEATURE_INPUT.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative Phase 3 feature table path.",
    )
    parser.add_argument(
        "--model-input",
        default=DEFAULT_MODEL_INPUT.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative trained Spark ML model path.",
    )
    parser.add_argument(
        "--score-output",
        default=DEFAULT_SCORE_OUTPUT.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative batch score output path.",
    )
    parser.add_argument(
        "--artifacts-base",
        default=DEFAULT_ARTIFACTS_BASE.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative artifacts base path. Default: artifacts.",
    )
    parser.add_argument("--model-version", default=DEFAULT_MODEL_VERSION, help="Model version written to score output.")
    parser.add_argument(
        "--score-threshold",
        type=float,
        default=DEFAULT_SCORE_THRESHOLD,
        help="Threshold used to create prediction_label. Default: 0.5.",
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
        SparkSession.builder.appName("batch-score-purchase-propensity")
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
        "# Batch Scoring Notes",
        "",
        "This artifact contains aggregate-only scoring notes.",
        "",
        f"Task: {summary['task']}.",
        f"Model version: {summary['model_version']}.",
        f"Score row count: {summary['score_row_count']}.",
        f"Predicted positive rate: {summary['predicted_positive_rate']}.",
        f"Score threshold: {summary['score_threshold']}.",
        "No API endpoint, row-level score artifact, raw feature sample, or model binary was created for commit.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def check_row(check_name: str, status: bool, observed: Any, expected: Any, notes: str) -> dict[str, Any]:
    return {
        "check_name": check_name,
        "status": "pass" if status else "fail",
        "observed_value": normalize_value(observed),
        "expected_value": normalize_value(expected),
        "notes": notes,
    }


def duplicate_client_count(df: DataFrame) -> int:
    row = (
        df.groupBy("client_id")
        .count()
        .where(F.col("count") > 1)
        .agg(F.count(F.lit(1)).alias("duplicate_client_id_count"))
        .collect()[0]
    )
    return int(row["duplicate_client_id_count"] or 0)


def score_bucket_expr() -> Any:
    expression = None
    for label, lower, upper in SCORE_BUCKETS:
        condition = (F.col("prediction_score") >= F.lit(lower)) & (
            F.col("prediction_score") <= F.lit(upper) if upper == 1.0 else F.col("prediction_score") < F.lit(upper)
        )
        expression = F.when(condition, F.lit(label)) if expression is None else expression.when(condition, F.lit(label))
    return expression.otherwise(F.lit("outside_range"))


def score_distribution(scores: DataFrame, score_count: int) -> list[dict[str, Any]]:
    bucketed = scores.select("prediction_score").withColumn("bucket", score_bucket_expr())
    rows = {row["bucket"]: row.asDict() for row in bucketed.groupBy("bucket").agg(
        F.min("prediction_score").alias("min_score"),
        F.max("prediction_score").alias("max_score"),
        F.count(F.lit(1)).alias("row_count"),
    ).collect()}
    distribution_rows: list[dict[str, Any]] = []
    for label, _lower, _upper in SCORE_BUCKETS:
        row = rows.get(label)
        row_count = int(row["row_count"]) if row else 0
        distribution_rows.append(
            {
                "bucket": label,
                "min_score": row["min_score"] if row else None,
                "max_score": row["max_score"] if row else None,
                "row_count": row_count,
                "row_rate": row_count / score_count if score_count else 0.0,
            }
        )
    return distribution_rows


def main() -> int:
    args = parse_args()
    if not 0 <= args.score_threshold <= 1:
        raise ValueError("--score-threshold must be between 0 and 1")
    if not args.model_version:
        raise ValueError("--model-version must be non-empty")

    config_path = resolve_repo_path(args.config)
    feature_input_path = resolve_repo_path(args.feature_input)
    model_input_path = resolve_repo_path(args.model_input)
    score_output_path = resolve_repo_path(args.score_output)
    artifacts_base = resolve_repo_path(args.artifacts_base)
    scoring_artifact_dir = artifacts_base / "scoring"
    summary_path = scoring_artifact_dir / "scoring_summary.json"
    distribution_path = scoring_artifact_dir / "score_distribution.csv"
    validation_path = scoring_artifact_dir / "scoring_validation.csv"
    notes_path = scoring_artifact_dir / "scoring_notes.md"

    config = read_simple_yaml(config_path)
    task = str(config.get("target", {}).get("task"))
    scored_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    spark = start_spark()
    model_loaded = False
    score_table_written = False
    try:
        model = PipelineModel.load(str(model_input_path))
        model_loaded = True

        features = spark.read.parquet(str(feature_input_path))
        input_row_count = features.count()
        eligible_features = features.where(F.col("is_eligible_purchase_propensity") == 1).cache()
        eligible_row_count = eligible_features.count()
        if eligible_row_count == 0:
            raise ValueError("Eligible scoring cohort is empty")

        transformed = model.transform(eligible_features)
        scores = (
            transformed.select(
                "client_id",
                vector_to_array(F.col("probability"))[1].alias("prediction_score"),
            )
            .withColumn("prediction_label", (F.col("prediction_score") >= F.lit(float(args.score_threshold))).cast("int"))
            .withColumn("model_version", F.lit(args.model_version))
            .withColumn("scored_at", F.lit(scored_at))
            .select("client_id", "prediction_score", "prediction_label", "model_version", "scored_at")
        )

        clean_output_dir(score_output_path, PROJECT_ROOT / "data" / "processed")
        scores.write.mode("overwrite").parquet(str(score_output_path))
        score_table_written = True

        written_scores = spark.read.parquet(str(score_output_path)).cache()
        score_row_count = written_scores.count()
        duplicate_count = duplicate_client_count(written_scores)
        null_score_count = written_scores.where(F.col("prediction_score").isNull()).count()
        score_out_of_range_count = written_scores.where(
            (F.col("prediction_score") < 0) | (F.col("prediction_score") > 1)
        ).count()
        invalid_label_count = written_scores.where(~F.col("prediction_label").isin([0, 1]) | F.col("prediction_label").isNull()).count()
        null_model_version_count = written_scores.where(F.col("model_version").isNull()).count()
        null_scored_at_count = written_scores.where(F.col("scored_at").isNull()).count()
        forbidden_present = sorted(FORBIDDEN_OUTPUT_COLUMNS.intersection(written_scores.columns))
        score_stats = written_scores.agg(
            F.min("prediction_score").alias("min_prediction_score"),
            F.max("prediction_score").alias("max_prediction_score"),
            F.avg("prediction_score").alias("avg_prediction_score"),
            F.sum(F.col("prediction_label").cast("long")).alias("predicted_positive_count"),
        ).collect()[0]
        predicted_positive_count = int(score_stats["predicted_positive_count"] or 0)
        predicted_positive_rate = predicted_positive_count / score_row_count if score_row_count else 0.0
        distribution_rows = score_distribution(written_scores, score_row_count)

        leakage_passed = (
            not forbidden_present
            and score_row_count == eligible_row_count
            and duplicate_count == 0
            and null_score_count == 0
            and score_out_of_range_count == 0
            and invalid_label_count == 0
        )

        validation_rows = [
            check_row(
                "output_row_count_equals_eligible_input_row_count",
                score_row_count == eligible_row_count,
                score_row_count,
                eligible_row_count,
                "Score output should include one row per eligible client.",
            ),
            check_row("no_null_prediction_score", null_score_count == 0, null_score_count, 0, "Scores are required."),
            check_row(
                "prediction_score_in_0_1",
                score_out_of_range_count == 0,
                score_out_of_range_count,
                0,
                "Scores must be probabilities in [0, 1].",
            ),
            check_row(
                "prediction_label_values_only_0_or_1",
                invalid_label_count == 0,
                invalid_label_count,
                0,
                "Prediction labels are thresholded binary values.",
            ),
            check_row("no_duplicate_client_id", duplicate_count == 0, duplicate_count, 0, "Score output should be unique by client_id."),
            check_row("model_version_non_null", null_model_version_count == 0, null_model_version_count, 0, "Model version is required."),
            check_row("scored_at_non_null", null_scored_at_count == 0, null_scored_at_count, 0, "Score timestamp is required."),
            check_row("no_label_column_in_score_output", "label" not in written_scores.columns, "label" in written_scores.columns, False, "Labels are not part of serving output."),
            check_row(
                "no_target_window_metadata_in_score_output",
                not forbidden_present,
                ",".join(forbidden_present),
                "",
                "Target-window metadata is not part of serving output.",
            ),
            check_row("model_loaded_successfully", model_loaded, model_loaded, True, "Spark ML model must load before scoring."),
            check_row("score_table_written_successfully", score_table_written, score_table_written, True, "Spark writer must create score output."),
        ]

        summary = {
            "generated_at_date": datetime.now(timezone.utc).date().isoformat(),
            "phase": "Phase 6: Batch Scoring",
            "status": "success",
            "task": task,
            "model_version": args.model_version,
            "model_type": "Spark ML PipelineModel",
            "feature_input_path": relative_path(feature_input_path),
            "model_input_path": relative_path(model_input_path),
            "score_output_path": relative_path(score_output_path),
            "input_row_count": input_row_count,
            "eligible_row_count": eligible_row_count,
            "score_row_count": score_row_count,
            "min_prediction_score": score_stats["min_prediction_score"],
            "max_prediction_score": score_stats["max_prediction_score"],
            "avg_prediction_score": score_stats["avg_prediction_score"],
            "score_threshold": args.score_threshold,
            "predicted_positive_count": predicted_positive_count,
            "predicted_positive_rate": predicted_positive_rate,
            "validation": {
                "duplicate_client_id_count": duplicate_count,
                "null_prediction_score_count": null_score_count,
                "score_out_of_range_count": score_out_of_range_count,
                "invalid_prediction_label_count": invalid_label_count,
                "model_loaded": model_loaded,
                "score_table_written": score_table_written,
            },
            "leakage_validation": {
                "input_dataset_source": "Phase 3 feature table",
                "phase_3_features_before_cutoff": True,
                "phase_5_model_trained_on_leakage_safe_dataset": True,
                "labels_or_target_window_events_used": False,
                "forbidden_output_columns": forbidden_present,
                "passed": leakage_passed,
            },
            "api_created": False,
            "production_endpoint_created": False,
            "privacy": {
                "artifact_level": "aggregate_only",
                "row_level_scores_persisted": False,
                "raw_client_ids_persisted": False,
                "raw_query_text_persisted": False,
                "product_names_persisted": False,
            },
        }

        write_json(summary_path, summary)
        write_csv(distribution_path, distribution_rows, ["bucket", "min_score", "max_score", "row_count", "row_rate"])
        write_csv(validation_path, validation_rows, ["check_name", "status", "observed_value", "expected_value", "notes"])
        write_notes(notes_path, summary)

        artifacts_exist = all(path.exists() for path in [summary_path, distribution_path, validation_path, notes_path])
        if not artifacts_exist or not leakage_passed or any(row["status"] != "pass" for row in validation_rows):
            return 1

        print("Batch scoring completed.")
        print(f"Eligible rows: {eligible_row_count}")
        print(f"Score rows: {score_row_count}")
        print(f"Predicted positives: {predicted_positive_count}")
        print(f"Predicted positive rate: {round(predicted_positive_rate, 6)}")
        print(f"Score output: {relative_path(score_output_path)}")
        print(f"Sanitized artifacts: {relative_path(scoring_artifact_dir)}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
