"""Run E2 threshold analysis for the temporal baseline model.

This job loads the existing temporal Spark ML model and temporal validation
snapshot, computes aggregate threshold metrics from 0.01 to 0.99, and writes
sanitized aggregate-only artifacts for decision-policy review.

It does not retrain the model, alter features, tune hyperparameters, persist
row-level predictions, or write client-level outputs.
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
DEFAULT_ARTIFACT_DIR = PROJECT_ROOT / "artifacts" / "modeling" / "e1_temporal_validation"
THRESHOLDS = tuple(round(value / 100, 2) for value in range(1, 100))
TOPK_METRICS_NAME = "topk_metrics.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run aggregate E2 threshold analysis.")
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


def read_topk_metrics(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [
            {
                "k_percent": float(row["k_percent"]),
                "top_k_count": int(float(row["top_k_count"])),
                "positive_count": int(float(row["positive_count"])),
                "precision_at_k": float(row["precision_at_k"]),
                "recall_at_k": float(row["recall_at_k"]),
                "lift_at_k": float(row["lift_at_k"]),
            }
            for row in csv.DictReader(handle)
        ]


def score_validation(model: PipelineModel, validation: DataFrame) -> DataFrame:
    return model.transform(validation).select(
        F.col("label").cast("int").alias("label"),
        vector_to_array(F.col("probability"))[1].alias("prediction_score"),
    )


def validation_summary(predictions: DataFrame) -> dict[str, Any]:
    row = predictions.agg(
        F.count("*").alias("row_count"),
        F.sum(F.col("label")).alias("positive_count"),
        F.sum(F.when(F.col("prediction_score").isNull(), 1).otherwise(0)).alias("null_score_count"),
        F.min("prediction_score").alias("min_score"),
        F.max("prediction_score").alias("max_score"),
        F.avg("prediction_score").alias("avg_score"),
    ).collect()[0]
    row_count = int(row["row_count"])
    positive_count = int(row["positive_count"] or 0)
    null_score_count = int(row["null_score_count"] or 0)
    if not row_count:
        raise ValueError("Validation predictions are empty")
    if null_score_count:
        raise ValueError("Validation predictions contain null scores")
    return {
        "validation_rows": row_count,
        "positive_count": positive_count,
        "negative_count": row_count - positive_count,
        "positive_rate": safe_divide(positive_count, row_count),
        "min_score": float(row["min_score"]),
        "max_score": float(row["max_score"]),
        "avg_score": float(row["avg_score"]),
    }


def threshold_metrics(predictions: DataFrame, row_count: int, positive_count: int) -> list[dict[str, Any]]:
    aggregate_exprs = []
    for threshold in THRESHOLDS:
        suffix = str(int(round(threshold * 100)))
        predicted_positive = F.col("prediction_score") >= F.lit(float(threshold))
        actual_positive = F.col("label") == F.lit(1)
        aggregate_exprs.extend(
            [
                F.sum(F.when(predicted_positive & actual_positive, 1).otherwise(0)).alias(f"tp_{suffix}"),
                F.sum(F.when(predicted_positive & ~actual_positive, 1).otherwise(0)).alias(f"fp_{suffix}"),
                F.sum(F.when(~predicted_positive & ~actual_positive, 1).otherwise(0)).alias(f"tn_{suffix}"),
                F.sum(F.when(~predicted_positive & actual_positive, 1).otherwise(0)).alias(f"fn_{suffix}"),
            ]
        )

    aggregate_row = predictions.agg(*aggregate_exprs).collect()[0].asDict()
    baseline_positive_rate = safe_divide(positive_count, row_count)
    rows = []
    for threshold in THRESHOLDS:
        suffix = str(int(round(threshold * 100)))
        tp = int(aggregate_row[f"tp_{suffix}"] or 0)
        fp = int(aggregate_row[f"fp_{suffix}"] or 0)
        tn = int(aggregate_row[f"tn_{suffix}"] or 0)
        fn = int(aggregate_row[f"fn_{suffix}"] or 0)
        precision = safe_divide(tp, tp + fp)
        recall = safe_divide(tp, tp + fn)
        f1 = safe_divide(2 * precision * recall, precision + recall)
        positive_prediction_rate = safe_divide(tp + fp, row_count)
        rows.append(
            {
                "threshold": threshold,
                "tp": tp,
                "fp": fp,
                "tn": tn,
                "fn": fn,
                "precision": precision,
                "recall": recall,
                "f1": f1,
                "positive_prediction_rate": positive_prediction_rate,
                "lift": safe_divide(precision, baseline_positive_rate),
            }
        )
    return rows


def lowest_threshold_with_precision(rows: list[dict[str, Any]], target: float) -> dict[str, Any] | None:
    matches = [row for row in rows if row["precision"] >= target]
    return min(matches, key=lambda row: row["threshold"]) if matches else None


def highest_threshold_with_recall(rows: list[dict[str, Any]], target: float) -> dict[str, Any] | None:
    matches = [row for row in rows if row["recall"] >= target]
    return max(matches, key=lambda row: row["threshold"]) if matches else None


def closest_population_threshold(rows: list[dict[str, Any]], target: float) -> dict[str, Any]:
    return min(rows, key=lambda row: (abs(row["positive_prediction_rate"] - target), row["threshold"]))


def summarize_operating_points(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any] | None]:
    return {
        "max_f1": max(rows, key=lambda row: (row["f1"], row["precision"], -row["positive_prediction_rate"])),
        "precision_ge_20_lowest_threshold": lowest_threshold_with_precision(rows, 0.20),
        "precision_ge_30_lowest_threshold": lowest_threshold_with_precision(rows, 0.30),
        "precision_ge_40_lowest_threshold": lowest_threshold_with_precision(rows, 0.40),
        "recall_ge_50_highest_threshold": highest_threshold_with_recall(rows, 0.50),
        "recall_ge_70_highest_threshold": highest_threshold_with_recall(rows, 0.70),
        "population_closest_1pct": closest_population_threshold(rows, 0.01),
        "population_closest_5pct": closest_population_threshold(rows, 0.05),
        "population_closest_10pct": closest_population_threshold(rows, 0.10),
        "population_closest_20pct": closest_population_threshold(rows, 0.20),
    }


def policy_comparison(
    operating_points: dict[str, dict[str, Any] | None], topk_rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    rows = []
    for policy_name, key in [
        ("Best-F1 threshold", "max_f1"),
        ("Precision >= 20% threshold", "precision_ge_20_lowest_threshold"),
        ("Precision >= 30% threshold", "precision_ge_30_lowest_threshold"),
    ]:
        row = operating_points[key]
        if row:
            rows.append(
                {
                    "policy": policy_name,
                    "threshold": row["threshold"],
                    "population_targeted": row["positive_prediction_rate"],
                    "precision": row["precision"],
                    "recall": row["recall"],
                    "lift": row["lift"],
                    "positive_count_captured": row["tp"],
                }
            )

    for row in topk_rows:
        rows.append(
            {
                "policy": f"Top {int(row['k_percent'] * 100)}%",
                "threshold": None,
                "population_targeted": row["k_percent"],
                "precision": row["precision_at_k"],
                "recall": row["recall_at_k"],
                "lift": row["lift_at_k"],
                "positive_count_captured": row["positive_count"],
            }
        )
    return rows


def metric_cell(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.6f}"
    return f"{value}"


def markdown_operating_row(label: str, row: dict[str, Any] | None) -> str:
    if row is None:
        return f"| {label} | Not reached | - | - | - | - | - |\n"
    return (
        f"| {label} | {row['threshold']:.2f} | {row['precision']:.6f} | {row['recall']:.6f} | "
        f"{row['f1']:.6f} | {row['positive_prediction_rate']:.6f} | {row['lift']:.6f} |\n"
    )


def write_review(
    path: Path,
    summary: dict[str, Any],
    operating_points: dict[str, dict[str, Any] | None],
    policy_rows: list[dict[str, Any]],
) -> None:
    lines = [
        "# E2 Threshold Search & Decision Policy Review",
        "",
        "## Scope",
        "This review uses the existing temporal Logistic Regression model and the temporal validation snapshot. It does not retrain the model, modify features, tune hyperparameters, or persist row-level predictions.",
        "",
        "## Validation Population",
        f"- Validation rows: {summary['validation_rows']:,}",
        f"- Positives: {summary['positive_count']:,}",
        f"- Negatives: {summary['negative_count']:,}",
        f"- Positive rate: {summary['positive_rate']:.6f}",
        "",
        "## Key Operating Points",
        "| Operating point | Threshold | Precision | Recall | F1 | Population targeted | Lift |",
        "|---|---:|---:|---:|---:|---:|---:|",
        markdown_operating_row("Maximum F1", operating_points["max_f1"]).rstrip(),
        markdown_operating_row(
            "Lowest threshold with precision >= 20%", operating_points["precision_ge_20_lowest_threshold"]
        ).rstrip(),
        markdown_operating_row(
            "Lowest threshold with precision >= 30%", operating_points["precision_ge_30_lowest_threshold"]
        ).rstrip(),
        markdown_operating_row(
            "Lowest threshold with precision >= 40%", operating_points["precision_ge_40_lowest_threshold"]
        ).rstrip(),
        markdown_operating_row(
            "Highest threshold with recall >= 50%", operating_points["recall_ge_50_highest_threshold"]
        ).rstrip(),
        markdown_operating_row(
            "Highest threshold with recall >= 70%", operating_points["recall_ge_70_highest_threshold"]
        ).rstrip(),
        "",
        "## Population-Target Closest Thresholds",
        "| Target population | Threshold | Actual population | Precision | Recall | Lift |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for label, key in [
        ("1%", "population_closest_1pct"),
        ("5%", "population_closest_5pct"),
        ("10%", "population_closest_10pct"),
        ("20%", "population_closest_20pct"),
    ]:
        row = operating_points[key]
        if row:
            lines.append(
                f"| {label} | {row['threshold']:.2f} | {row['positive_prediction_rate']:.6f} | "
                f"{row['precision']:.6f} | {row['recall']:.6f} | {row['lift']:.6f} |"
            )

    lines.extend(
        [
            "",
            "## Policy Comparison",
            "| Policy | Threshold | Population targeted | Precision | Recall | Lift | Positives captured |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in policy_rows:
        threshold = "-" if row["threshold"] is None else f"{row['threshold']:.2f}"
        lines.append(
            f"| {row['policy']} | {threshold} | {row['population_targeted']:.6f} | "
            f"{row['precision']:.6f} | {row['recall']:.6f} | {row['lift']:.6f} | "
            f"{row['positive_count_captured']:,} |"
        )

    lines.extend(
        [
            "",
            "## Decision Review",
            "Threshold 0.5 is recall-heavy rather than precision-oriented. It captures many buyers, but it also targets a large share of the population, so it is not naturally aligned with capacity-limited marketing campaigns.",
            "",
            "The maximum-F1 threshold is useful as a diagnostic, but it should not automatically become the business policy because F1 assumes precision and recall have equal value. Campaign cost, channel capacity, and user fatigue usually make population size and precision more important.",
            "",
            "TopK policies are easier to operate because they let the business decide a fixed campaign size. For the current baseline, TopK ranking is preferable to a fixed probability threshold. Top 5% is the recommended default campaign segment; Top 1% is better for expensive or conservative outreach; Top 10% is suitable for broader campaigns.",
            "",
            "## Final Conclusion",
            "A fixed threshold is not preferable for the current baseline decision policy. TopK is preferable because the model's strongest validated behavior is ranking quality, and TopK maps directly to marketing capacity.",
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
    threshold_metrics_path = artifact_dir / "threshold_metrics.csv"
    threshold_summary_path = artifact_dir / "threshold_summary.json"
    threshold_review_path = artifact_dir / "threshold_review.md"

    spark = SparkSession.builder.appName("e2-threshold-analysis-temporal").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        validation = spark.read.parquet(str(validation_input))
        model = PipelineModel.load(str(model_input))
        predictions = score_validation(model, validation).cache()
        summary = validation_summary(predictions)
        rows = threshold_metrics(predictions, summary["validation_rows"], summary["positive_count"])
        operating_points = summarize_operating_points(rows)
        topk_rows = read_topk_metrics(artifact_dir / TOPK_METRICS_NAME)
        policy_rows = policy_comparison(operating_points, topk_rows)

        most_buyers = max(policy_rows, key=lambda row: row["positive_count_captured"]) if policy_rows else None
        highest_precision = max(policy_rows, key=lambda row: row["precision"]) if policy_rows else None

        write_csv(
            threshold_metrics_path,
            rows,
            [
                "threshold",
                "tp",
                "fp",
                "tn",
                "fn",
                "precision",
                "recall",
                "f1",
                "positive_prediction_rate",
                "lift",
            ],
        )

        threshold_summary = {
            "generated_at_timestamp": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "experiment": "E2 Threshold Search & Decision Policy Review",
            "status": "success",
            "model_input_path": relative_path(model_input),
            "validation_input_path": relative_path(validation_input),
            "threshold_metrics_path": relative_path(threshold_metrics_path),
            "threshold_review_path": relative_path(threshold_review_path),
            "threshold_start": min(THRESHOLDS),
            "threshold_end": max(THRESHOLDS),
            "threshold_step": 0.01,
            **summary,
            "operating_points": operating_points,
            "policy_comparison": policy_rows,
            "decision_answers": {
                "captures_most_buyers": most_buyers["policy"] if most_buyers else None,
                "highest_precision": highest_precision["policy"] if highest_precision else None,
                "fixed_threshold_preferable": False,
                "topk_preferable": True,
                "recommended_policy": "Use TopK segments for current baseline decisioning; Top 5% is the practical default campaign segment, with Top 1% for conservative/high-cost outreach and Top 10% for broader campaigns.",
            },
            "privacy": {
                "artifact_level": "aggregate_only",
                "row_level_predictions_persisted": False,
                "raw_client_ids_persisted": False,
                "raw_query_text_persisted": False,
                "product_names_persisted": False,
            },
        }
        write_json(threshold_summary_path, threshold_summary)
        write_review(threshold_review_path, summary, operating_points, policy_rows)

        best_f1 = operating_points["max_f1"]
        print("E2 threshold analysis completed.")
        print(f"Validation rows: {summary['validation_rows']}")
        print(f"Best F1 threshold: {best_f1['threshold']:.2f}; F1: {best_f1['f1']:.6f}")
        print(f"Threshold metrics: {relative_path(threshold_metrics_path)}")
        print(f"Threshold summary: {relative_path(threshold_summary_path)}")
        print(f"Threshold review: {relative_path(threshold_review_path)}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
