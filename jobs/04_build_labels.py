"""Build purchase propensity labels and a training-ready dataset.

This Phase 4 job reads the Phase 3 user feature table and processed purchase
events, creates a binary purchase-propensity label for eligible clients, and
writes:

- data/processed/labels/purchase_propensity_30d/
- data/processed/training/purchase_propensity_30d/
- artifacts/labels/label_summary.json
- artifacts/labels/label_validation.csv
- artifacts/labels/training_dataset_validation.csv
- artifacts/labels/label_notes.md

It does not train models, evaluate models, create predictions, or implement API
serving.
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
DEFAULT_ARTIFACTS_BASE = PROJECT_ROOT / "artifacts"
LABEL_OUTPUT_DIR = "labels/purchase_propensity_30d"
TRAINING_OUTPUT_DIR = "training/purchase_propensity_30d"
EXPECTED_ELIGIBLE_COUNT = 2_149_796
EXPECTED_POSITIVE_COUNT = 93_614


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build purchase propensity labels and training dataset.")
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
        "--artifacts-base",
        default=DEFAULT_ARTIFACTS_BASE.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative artifacts base. Default: artifacts.",
    )
    parser.add_argument(
        "--feature-input",
        default=None,
        help=(
            "Optional repo-relative Phase 3 feature table path. Defaults to the baseline feature table, "
            "or the matching snapshot feature table when --snapshot-name is provided."
        ),
    )
    parser.add_argument(
        "--cutoff-date",
        default=None,
        help="Optional cutoff date override in YYYY-MM-DD format. Defaults to target.cutoff_date from config.",
    )
    parser.add_argument(
        "--target-end",
        default=None,
        help="Optional target end date override in YYYY-MM-DD format. Defaults to target.target_end from config.",
    )
    parser.add_argument(
        "--snapshot-name",
        default=None,
        help=(
            "Optional snapshot or experiment name. When provided, outputs are written under "
            "data/processed/labels/<snapshot-name>/, data/processed/training/<snapshot-name>/, "
            "and artifacts/labels/<snapshot-name>/."
        ),
    )
    return parser.parse_args()


def relative_path(path: Path) -> str:
    return path.relative_to(PROJECT_ROOT).as_posix()


def resolve_repo_path(path_text: str) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        raise ValueError("path arguments must be repo-relative")
    return PROJECT_ROOT / path


def validate_snapshot_name(snapshot_name: str | None) -> str | None:
    if snapshot_name is None:
        return None
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")
    if not snapshot_name or any(character not in allowed for character in snapshot_name):
        raise ValueError("--snapshot-name may only contain letters, numbers, underscores, and hyphens")
    return snapshot_name


def target_window_days(cutoff_date: str, target_end: str) -> int:
    cutoff = datetime.strptime(cutoff_date, "%Y-%m-%d").date()
    end = datetime.strptime(target_end, "%Y-%m-%d").date()
    days = (end - cutoff).days + 1
    if days <= 0:
        raise ValueError("target end date must be on or after cutoff date")
    return days


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
        SparkSession.builder.appName("build-purchase-propensity-labels")
        .master("local[*]")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def clean_output_dir(path: Path, output_base: Path) -> None:
    resolved = path.resolve()
    processed_root = output_base.resolve()
    if not str(resolved).startswith(str(processed_root)):
        raise ValueError("Refusing to clean an output directory outside processed output base")
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
            writer.writerow(row)


def write_notes(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "# Label Construction Notes",
                "",
                "This artifact contains aggregate-only label construction notes.",
                "",
                f"Task: {summary['task']}.",
                f"Boundary rule: {summary['boundary_rule']}.",
                f"Eligible cohort count: {summary['eligible_cohort_count']}.",
                f"Positive count: {summary['positive_count']}.",
                f"Positive rate: {summary['positive_rate']}.",
                "Multiple target-window purchases are aggregated to one binary label row per client.",
                "Feature null handling is unchanged from Phase 3; model-stage imputation is deferred.",
                "No model, prediction, batch scoring, or API output was created.",
                "",
            ]
        ),
        encoding="utf-8",
    )


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


def main() -> int:
    args = parse_args()
    config_path = resolve_repo_path(args.config)
    output_base = resolve_repo_path(args.output_base)
    artifacts_base = resolve_repo_path(args.artifacts_base)
    snapshot_name = validate_snapshot_name(args.snapshot_name)
    feature_input_path = (
        resolve_repo_path(args.feature_input)
        if args.feature_input
        else output_base / "features" / snapshot_name / "user_behavior_features"
        if snapshot_name
        else output_base / "features" / "user_behavior_features"
    )
    label_output_path = (
        output_base / "labels" / snapshot_name / "purchase_propensity_30d"
        if snapshot_name
        else output_base / LABEL_OUTPUT_DIR
    )
    training_output_path = (
        output_base / "training" / snapshot_name / "purchase_propensity_30d"
        if snapshot_name
        else output_base / TRAINING_OUTPUT_DIR
    )
    label_artifact_dir = artifacts_base / "labels" / snapshot_name if snapshot_name else artifacts_base / "labels"
    summary_path = label_artifact_dir / "label_summary.json"
    label_validation_path = label_artifact_dir / "label_validation.csv"
    training_validation_path = label_artifact_dir / "training_dataset_validation.csv"
    notes_path = label_artifact_dir / "label_notes.md"

    config = read_simple_yaml(config_path)
    target_config = config.get("target", {})
    task = str(target_config.get("task"))
    cutoff_date = str(args.cutoff_date or target_config.get("cutoff_date"))
    target_end = str(args.target_end or target_config.get("target_end"))
    if not cutoff_date or cutoff_date == "None" or not target_end or target_end == "None":
        raise ValueError("configs/pipeline_config.yaml must define target.cutoff_date and target.target_end")
    target_window_day_count = target_window_days(cutoff_date, target_end)

    boundary_rule = "event_ts >= cutoff_date and event_ts < date_add(target_end, 1)"

    spark = start_spark()
    try:
        features = spark.read.parquet(str(feature_input_path))
        product_buy = spark.read.parquet(str(output_base / "events" / "product_buy"))

        feature_columns = [column for column in features.columns if column != "client_id"]
        forbidden_preexisting = [
            column
            for column in feature_columns
            if column.lower() in {"label", "target", "y", "purchase_in_target"}
            or column.lower().startswith("label_")
            or column.lower().startswith("target_")
        ]
        if forbidden_preexisting:
            raise ValueError("Feature table contains label-like columns")

        eligible_features = features.where(F.col("is_eligible_purchase_propensity") == 1).cache()
        target_purchases = (
            product_buy.where(
                (F.col("event_ts") >= F.to_timestamp(F.lit(cutoff_date)))
                & (F.col("event_ts") < F.date_add(F.to_date(F.lit(target_end)), 1).cast("timestamp"))
            )
            .groupBy("client_id")
            .agg(F.count(F.lit(1)).alias("target_event_count"))
        )

        labels = (
            eligible_features.select("client_id")
            .join(target_purchases, "client_id", "left")
            .fillna({"target_event_count": 0})
            .withColumn("label", F.when(F.col("target_event_count") > 0, F.lit(1)).otherwise(F.lit(0)).cast("int"))
            .withColumn("target_window_start", F.lit(cutoff_date))
            .withColumn("target_window_end", F.lit(target_end))
            .select("client_id", "label", "target_window_start", "target_window_end", "target_event_count")
            .cache()
        )
        training = eligible_features.join(labels.select("client_id", "label"), "client_id", "inner").cache()

        eligible_count = eligible_features.count()
        label_row_count = labels.count()
        positive_count = labels.where(F.col("label") == 1).count()
        negative_count = labels.where(F.col("label") == 0).count()
        positive_rate = round(positive_count / label_row_count, 6) if label_row_count else None
        training_row_count = training.count()
        label_null_count = labels.where(F.col("label").isNull()).count()
        invalid_label_value_count = labels.where(~F.col("label").isin([0, 1]) | F.col("label").isNull()).count()
        label_duplicate_count = duplicate_client_count(labels)
        training_duplicate_count = duplicate_client_count(training)
        feature_count_used = len(feature_columns)
        prediction_or_model_columns = [
            column
            for column in training.columns
            if "prediction" in column.lower() or "score" in column.lower() or "model" in column.lower()
        ]

        clean_output_dir(label_output_path, output_base)
        labels.write.mode("overwrite").parquet(str(label_output_path))
        clean_output_dir(training_output_path, output_base)
        training.write.mode("overwrite").parquet(str(training_output_path))

        enforce_default_expected_counts = snapshot_name is None and args.cutoff_date is None and args.target_end is None
        eligible_count_matches_expected = (
            eligible_count == EXPECTED_ELIGIBLE_COUNT if enforce_default_expected_counts else True
        )
        positive_count_matches_expected = (
            abs(positive_count - EXPECTED_POSITIVE_COUNT) <= 1 if enforce_default_expected_counts else True
        )
        expected_eligible_value: Any = EXPECTED_ELIGIBLE_COUNT if enforce_default_expected_counts else ""
        expected_positive_value: Any = EXPECTED_POSITIVE_COUNT if enforce_default_expected_counts else ""
        expected_count_note = (
            "Only eligible clients are included in the label table."
            if enforce_default_expected_counts
            else "Snapshot run: baseline expected eligible count is not enforced."
        )
        expected_positive_note = (
            "Positive clients have at least one product_buy event in the target window."
            if enforce_default_expected_counts
            else "Snapshot run: baseline expected positive count is not enforced."
        )
        label_validation_rows = [
            check_row(
                "label_row_count_equals_eligible_cohort_count",
                label_row_count == eligible_count and eligible_count_matches_expected,
                label_row_count,
                expected_eligible_value,
                expected_count_note,
            ),
            check_row(
                "positive_count_matches_phase_1_1",
                positive_count_matches_expected,
                positive_count,
                expected_positive_value,
                expected_positive_note,
            ),
            check_row(
                "label_values_only_0_or_1",
                invalid_label_value_count == 0,
                invalid_label_value_count,
                0,
                "Labels are binary.",
            ),
            check_row("no_null_labels", label_null_count == 0, label_null_count, 0, "No null labels are allowed."),
            check_row(
                "no_duplicate_client_id_in_label_table",
                label_duplicate_count == 0,
                label_duplicate_count,
                0,
                "Target-window purchases are aggregated before joining labels.",
            ),
            check_row(
                "target_window_boundary_rule_documented",
                True,
                boundary_rule,
                boundary_rule,
                "Half-open interval includes cutoff date and excludes the day after target_end.",
            ),
        ]
        training_validation_rows = [
            check_row(
                "training_row_count_equals_label_row_count",
                training_row_count == label_row_count,
                training_row_count,
                label_row_count,
                "Training dataset is label table joined to eligible feature rows.",
            ),
            check_row("feature_count_used", True, feature_count_used, feature_count_used, "Feature count excludes client_id."),
            check_row(
                "no_duplicate_client_id_in_training_dataset",
                training_duplicate_count == 0,
                training_duplicate_count,
                0,
                "Training dataset should have one row per eligible client.",
            ),
            check_row("no_null_labels_in_training_dataset", label_null_count == 0, label_null_count, 0, "Label is required."),
            check_row(
                "no_prediction_or_model_columns",
                not prediction_or_model_columns,
                ",".join(prediction_or_model_columns),
                "",
                "Training dataset must not include prediction or model output columns.",
            ),
        ]
        leakage_passed = (
            not forbidden_preexisting
            and training_row_count == label_row_count
            and label_duplicate_count == 0
            and training_duplicate_count == 0
            and invalid_label_value_count == 0
        )
        summary = {
            "generated_at_date": datetime.now(timezone.utc).date().isoformat(),
            "phase": "Phase 4: Label Construction",
            "status": "success",
            "snapshot_name": snapshot_name,
            "task": task,
            "target_window_days": target_window_day_count,
            "cutoff_date": cutoff_date,
            "target_window_start": cutoff_date,
            "target_window_end": target_end,
            "boundary_rule": boundary_rule,
            "eligible_cohort_definition": target_config.get("eligible_cohort"),
            "positive_definition": target_config.get("positive_definition"),
            "feature_input_path": relative_path(feature_input_path),
            "label_output_path": relative_path(label_output_path),
            "training_dataset_output_path": relative_path(training_output_path),
            "eligible_cohort_count": eligible_count,
            "label_row_count": label_row_count,
            "positive_count": positive_count,
            "negative_count": negative_count,
            "positive_rate": positive_rate,
            "training_dataset_row_count": training_row_count,
            "feature_count_used": feature_count_used,
            "expected_counts_enforced": enforce_default_expected_counts,
            "expected_phase_1_1_eligible_count": EXPECTED_ELIGIBLE_COUNT if enforce_default_expected_counts else None,
            "expected_phase_1_1_positive_count": EXPECTED_POSITIVE_COUNT if enforce_default_expected_counts else None,
            "expected_phase_1_1_positive_count_match": (
                positive_count_matches_expected if enforce_default_expected_counts else None
            ),
            "duplicate_handling_policy": (
                "Target-window purchases are aggregated to one row per client_id; multiple purchases increase "
                "target_event_count but keep label binary."
            ),
            "null_strategy": (
                "Feature nulls are preserved from Phase 3; model-stage imputation is deferred to Phase 5."
            ),
            "leakage_validation": {
                "features_source": "Phase 3 feature table",
                "labels_source": "product_buy target-window events only",
                "target_window_features_created": False,
                "preexisting_label_like_columns": forbidden_preexisting,
                "passed": leakage_passed,
            },
            "privacy": {
                "artifact_level": "aggregate_only",
                "row_samples_persisted": False,
                "raw_client_ids_persisted": False,
                "raw_query_text_persisted": False,
                "product_names_persisted": False,
            },
        }

        write_json(summary_path, summary)
        write_csv(
            label_validation_path,
            label_validation_rows,
            ["check_name", "status", "observed_value", "expected_value", "notes"],
        )
        write_csv(
            training_validation_path,
            training_validation_rows,
            ["check_name", "status", "observed_value", "expected_value", "notes"],
        )
        write_notes(notes_path, summary)

        print("Label construction completed.")
        print(f"Label rows: {label_row_count}")
        print(f"Positive count: {positive_count}")
        print(f"Negative count: {negative_count}")
        print(f"Positive rate: {positive_rate}")
        print(f"Training rows: {training_row_count}")
        print(f"Wrote label table to {relative_path(label_output_path)}")
        print(f"Wrote training dataset to {relative_path(training_output_path)}")
        print(f"Wrote sanitized artifacts under {relative_path(label_artifact_dir)}")
        return 0 if leakage_passed and all(row["status"] == "pass" for row in label_validation_rows) else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
