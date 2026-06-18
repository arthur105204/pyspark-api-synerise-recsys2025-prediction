"""Run E3 calibration analysis for the temporal baseline model.

This job loads the existing temporal Spark ML model and temporal validation
snapshot, computes a 10-bucket calibration curve, and writes sanitized
aggregate-only calibration artifacts.

It does not retrain the model, fit a calibration model, alter features, tune
hyperparameters, change thresholds, persist row-level predictions, or write
client-level outputs.
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.ml.functions import vector_to_array
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MODEL_INPUT = PROJECT_ROOT / "data" / "models" / "purchase_propensity_baseline_temporal"
DEFAULT_VALIDATION_INPUT = (
    PROJECT_ROOT / "data" / "processed" / "training" / "e1_valid_2022_11_09" / "purchase_propensity_30d"
)
DEFAULT_ARTIFACT_DIR = PROJECT_ROOT / "artifacts" / "modeling" / "e3_calibration"
BUCKET_COUNT = 10
GAP_PASS_THRESHOLD = 0.05
ECE_FAIL_THRESHOLD = 0.10
HIGH_SCORE_OVERCONFIDENCE_THRESHOLD = 0.10
HIGH_SCORE_BUCKETS = {"0.8-0.9", "0.9-1.0"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run aggregate E3 calibration analysis.")
    parser.add_argument(
        "--model-input",
        default=DEFAULT_MODEL_INPUT.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative temporal Spark ML model path.",
    )
    parser.add_argument(
        "--validation-input",
        default=DEFAULT_VALIDATION_INPUT.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative temporal validation training dataset path.",
    )
    parser.add_argument(
        "--artifact-dir",
        default=DEFAULT_ARTIFACT_DIR.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative output artifact directory.",
    )
    return parser.parse_args()


def relative_path(path: Path) -> str:
    return path.relative_to(PROJECT_ROOT).as_posix()


def resolve_repo_path(path_text: str) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        raise ValueError("path arguments must be repo-relative")
    return PROJECT_ROOT / path


def safe_divide(numerator: float, denominator: float) -> float:
    return float(numerator) / float(denominator) if denominator else 0.0


def normalize_value(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    return value


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=normalize_value)
        handle.write("\n")


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: normalize_value(value) for key, value in row.items()})


def bucket_label(bucket_index: int) -> str:
    lower = bucket_index / BUCKET_COUNT
    upper = (bucket_index + 1) / BUCKET_COUNT
    return f"{lower:.1f}-{upper:.1f}"


def score_validation(model: PipelineModel, validation: DataFrame) -> DataFrame:
    return model.transform(validation).select(
        F.col("label").cast("int").alias("label"),
        vector_to_array(F.col("probability"))[1].alias("prediction_score"),
    )


def build_calibration_curve(predictions: DataFrame) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    scored = predictions.select("label", "prediction_score").cache()
    summary_row = scored.agg(
        F.count("*").alias("row_count"),
        F.sum(F.col("label")).alias("positive_count"),
        F.sum(F.when(F.col("prediction_score").isNull(), 1).otherwise(0)).alias("null_score_count"),
        F.min("prediction_score").alias("min_score"),
        F.max("prediction_score").alias("max_score"),
        F.avg("prediction_score").alias("avg_score"),
    ).collect()[0]

    row_count = int(summary_row["row_count"])
    positive_count = int(summary_row["positive_count"] or 0)
    null_score_count = int(summary_row["null_score_count"] or 0)
    if not row_count:
        raise ValueError("Validation predictions are empty")
    if null_score_count:
        raise ValueError("Validation predictions contain null scores")

    bucketed = scored.withColumn(
        "bucket_index",
        F.least(F.floor(F.col("prediction_score") * F.lit(BUCKET_COUNT)).cast("int"), F.lit(BUCKET_COUNT - 1)),
    )
    aggregate_rows = {
        int(row["bucket_index"]): row.asDict()
        for row in bucketed.groupBy("bucket_index").agg(
            F.count("*").alias("sample_count"),
            F.avg("prediction_score").alias("predicted_probability_avg"),
            F.avg("label").alias("actual_positive_rate"),
        ).collect()
    }

    curve_rows = []
    for index in range(BUCKET_COUNT):
        row = aggregate_rows.get(index)
        sample_count = int(row["sample_count"]) if row else 0
        predicted_avg = float(row["predicted_probability_avg"]) if row else 0.0
        actual_rate = float(row["actual_positive_rate"]) if row else 0.0
        absolute_gap = abs(predicted_avg - actual_rate) if sample_count else 0.0
        curve_rows.append(
            {
                "score_bucket": bucket_label(index),
                "sample_count": sample_count,
                "predicted_probability_avg": predicted_avg,
                "actual_positive_rate": actual_rate,
                "absolute_gap": absolute_gap,
            }
        )

    base_summary = {
        "validation_rows": row_count,
        "positive_count": positive_count,
        "negative_count": row_count - positive_count,
        "positive_rate": safe_divide(positive_count, row_count),
        "min_score": float(summary_row["min_score"]),
        "max_score": float(summary_row["max_score"]),
        "avg_score": float(summary_row["avg_score"]),
    }
    return curve_rows, base_summary


def summarize_calibration(curve_rows: list[dict[str, Any]], row_count: int) -> dict[str, Any]:
    non_empty_rows = [row for row in curve_rows if row["sample_count"] > 0]
    expected_calibration_error = sum(
        safe_divide(row["sample_count"], row_count) * row["absolute_gap"] for row in non_empty_rows
    )
    maximum_calibration_gap = max((row["absolute_gap"] for row in non_empty_rows), default=0.0)
    average_calibration_gap = safe_divide(
        sum(row["absolute_gap"] for row in non_empty_rows), len(non_empty_rows)
    )
    bucket_gaps_under_threshold = sum(1 for row in non_empty_rows if row["absolute_gap"] < GAP_PASS_THRESHOLD)
    most_bucket_gaps_under_threshold = bucket_gaps_under_threshold > safe_divide(len(non_empty_rows), 2)
    high_score_overconfident_rows = [
        row
        for row in non_empty_rows
        if row["score_bucket"] in HIGH_SCORE_BUCKETS
        and (row["predicted_probability_avg"] - row["actual_positive_rate"]) > HIGH_SCORE_OVERCONFIDENCE_THRESHOLD
    ]
    monotonic_actual_rate = all(
        current["actual_positive_rate"] <= following["actual_positive_rate"]
        for current, following in zip(non_empty_rows, non_empty_rows[1:])
    )

    if expected_calibration_error < GAP_PASS_THRESHOLD and most_bucket_gaps_under_threshold:
        calibration_quality = "PASS"
        recommendation = "Scores are reasonably calibrated and can be interpreted as probabilities."
    elif expected_calibration_error <= ECE_FAIL_THRESHOLD and not high_score_overconfident_rows:
        calibration_quality = "PARTIAL PASS"
        recommendation = "Scores are useful mainly for ranking; probability interpretation should be treated cautiously."
    else:
        calibration_quality = "FAIL"
        recommendation = (
            "Calibration is poor and probability-based decision making should be avoided until calibration is applied."
        )

    return {
        "bucket_count": len(curve_rows),
        "non_empty_bucket_count": len(non_empty_rows),
        "expected_calibration_error": expected_calibration_error,
        "maximum_calibration_gap": maximum_calibration_gap,
        "average_calibration_gap": average_calibration_gap,
        "bucket_gaps_under_0_05": bucket_gaps_under_threshold,
        "most_bucket_gaps_under_0_05": most_bucket_gaps_under_threshold,
        "high_score_overconfident": bool(high_score_overconfident_rows),
        "high_score_overconfidence_bucket_count": len(high_score_overconfident_rows),
        "monotonic_actual_positive_rate": monotonic_actual_rate,
        "calibration_quality": calibration_quality,
        "final_recommendation": recommendation,
    }


def write_review(
    path: Path,
    base_summary: dict[str, Any],
    calibration_summary: dict[str, Any],
    curve_rows: list[dict[str, Any]],
) -> None:
    lines = [
        "# E3 Calibration Analysis",
        "",
        "## Scope",
        "This evaluation uses the existing temporal Logistic Regression model and temporal validation snapshot. It does not retrain the model, fit a calibration model, modify features, change labels, change thresholds, or persist row-level predictions.",
        "",
        "## Validation Population",
        f"- Validation rows: {base_summary['validation_rows']:,}",
        f"- Positives: {base_summary['positive_count']:,}",
        f"- Negatives: {base_summary['negative_count']:,}",
        f"- Positive rate: {base_summary['positive_rate']:.6f}",
        f"- Average predicted score: {base_summary['avg_score']:.6f}",
        "",
        "## Calibration Curve",
        "| Score bucket | Sample count | Predicted probability avg | Actual positive rate | Absolute gap |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in curve_rows:
        lines.append(
            f"| {row['score_bucket']} | {row['sample_count']:,} | "
            f"{row['predicted_probability_avg']:.6f} | {row['actual_positive_rate']:.6f} | "
            f"{row['absolute_gap']:.6f} |"
        )

    lines.extend(
        [
            "",
            "## Calibration Metrics",
            f"- Expected Calibration Error: {calibration_summary['expected_calibration_error']:.6f}",
            f"- Maximum calibration gap: {calibration_summary['maximum_calibration_gap']:.6f}",
            f"- Average calibration gap: {calibration_summary['average_calibration_gap']:.6f}",
            f"- Buckets with gap < 0.05: {calibration_summary['bucket_gaps_under_0_05']} of {calibration_summary['non_empty_bucket_count']}",
            f"- Monotonic actual positive rate: {calibration_summary['monotonic_actual_positive_rate']}",
            f"- High-score overconfident: {calibration_summary['high_score_overconfident']}",
            f"- Calibration quality: {calibration_summary['calibration_quality']}",
            "",
            "## Questions Answered",
            f"1. Can the current LR score be interpreted as a probability? {calibration_summary['final_recommendation']}",
            f"2. Does score increase monotonically with actual purchase rate? {calibration_summary['monotonic_actual_positive_rate']}.",
            "3. Are high-score users actually buying at similar rates? Review the 0.8-0.9 and 0.9-1.0 buckets above; large positive gaps mean the model is overconfident.",
            "4. Is calibration good enough for business probability interpretation? Use the ECE and bucket gaps above as the decision gate.",
            "5. Should future business decisions use raw probability thresholds or TopK? If calibration is not PASS, prefer ranking-only TopK policies from E2.",
            "",
            "## Final Recommendation",
            calibration_summary["final_recommendation"],
            "",
            "## E11 Recommendation",
            "E11 optional calibration layer is recommended if E3 is PARTIAL PASS or FAIL and stakeholders need probability interpretation. It is not needed merely to keep using TopK ranking policies.",
            "",
            "## Privacy",
            "Artifacts are aggregate-only. No raw client IDs, raw query text, product names, row-level scores, or row-level prediction examples are persisted.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    model_input = resolve_repo_path(args.model_input)
    validation_input = resolve_repo_path(args.validation_input)
    artifact_dir = resolve_repo_path(args.artifact_dir)
    calibration_curve_path = artifact_dir / "calibration_curve.csv"
    calibration_summary_path = artifact_dir / "calibration_summary.json"
    calibration_review_path = artifact_dir / "calibration_review.md"

    spark = SparkSession.builder.appName("e3-calibration-analysis-temporal").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        validation = spark.read.parquet(str(validation_input))
        model = PipelineModel.load(str(model_input))
        predictions = score_validation(model, validation).cache()
        curve_rows, base_summary = build_calibration_curve(predictions)
        calibration_summary = summarize_calibration(curve_rows, base_summary["validation_rows"])

        write_csv(
            calibration_curve_path,
            curve_rows,
            [
                "score_bucket",
                "sample_count",
                "predicted_probability_avg",
                "actual_positive_rate",
                "absolute_gap",
            ],
        )

        summary_payload = {
            "generated_at_timestamp": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "experiment": "E3 Calibration Analysis",
            "status": "success",
            "model_input_path": relative_path(model_input),
            "validation_input_path": relative_path(validation_input),
            "calibration_curve_path": relative_path(calibration_curve_path),
            "calibration_review_path": relative_path(calibration_review_path),
            **base_summary,
            **calibration_summary,
            "decision_gates": {
                "pass": "ECE < 0.05 and most bucket gaps < 0.05",
                "partial_pass": "0.05 <= ECE <= 0.10 without severe high-score overconfidence",
                "fail": "ECE > 0.10 or high-score buckets are heavily overconfident",
                "high_score_overconfidence_gap_threshold": HIGH_SCORE_OVERCONFIDENCE_THRESHOLD,
            },
            "privacy": {
                "artifact_level": "aggregate_only",
                "row_level_predictions_persisted": False,
                "raw_client_ids_persisted": False,
                "raw_query_text_persisted": False,
                "product_names_persisted": False,
            },
        }
        write_json(calibration_summary_path, summary_payload)
        write_review(calibration_review_path, base_summary, calibration_summary, curve_rows)

        print("E3 calibration analysis completed.")
        print(f"Validation rows: {base_summary['validation_rows']}")
        print(f"ECE: {calibration_summary['expected_calibration_error']:.6f}")
        print(f"Calibration quality: {calibration_summary['calibration_quality']}")
        print(f"Calibration curve: {relative_path(calibration_curve_path)}")
        print(f"Calibration summary: {relative_path(calibration_summary_path)}")
        print(f"Calibration review: {relative_path(calibration_review_path)}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
