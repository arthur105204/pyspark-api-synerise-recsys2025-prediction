"""Run E9 final benchmark evaluation for V2-2 vs V2-4.

This job loads existing trained Spark ML models, scores the temporal validation
snapshot in memory, and writes aggregate-only benchmark artifacts. It does not
retrain models, change features, change preprocessing, change labels, or persist
row-level predictions.
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
E6_JOB_PATH = PROJECT_ROOT / "jobs" / "05l_train_e6_velocity.py"
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "baseline_v24_features.json"
DEFAULT_VALIDATION_INPUT = (
    PROJECT_ROOT / "data" / "processed" / "training" / "e1_valid_2022_11_09" / "purchase_propensity_30d"
)
DEFAULT_EVENTS_BASE = PROJECT_ROOT / "data" / "processed" / "events"
DEFAULT_V22_MODEL = PROJECT_ROOT / "data" / "models" / "purchase_propensity_baseline_v22"
DEFAULT_V24_MODEL = PROJECT_ROOT / "data" / "models" / "purchase_propensity_baseline_v24"
DEFAULT_ARTIFACT_DIR = PROJECT_ROOT / "artifacts" / "modeling" / "baseline_v2"
TOPK_PERCENTS = (0.01, 0.05, 0.10)


def load_e6_module() -> Any:
    spec = importlib.util.spec_from_file_location("e6_velocity_job", E6_JOB_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load E6 helper module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


E6 = load_e6_module()
V21 = E6.V21


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run E9 final V2-2 vs V2-4 benchmark.")
    parser.add_argument("--feature-config", default=DEFAULT_CONFIG_PATH.relative_to(PROJECT_ROOT).as_posix())
    parser.add_argument("--validation-input", default=DEFAULT_VALIDATION_INPUT.relative_to(PROJECT_ROOT).as_posix())
    parser.add_argument("--events-base", default=DEFAULT_EVENTS_BASE.relative_to(PROJECT_ROOT).as_posix())
    parser.add_argument("--v22-model", default=DEFAULT_V22_MODEL.relative_to(PROJECT_ROOT).as_posix())
    parser.add_argument("--v24-model", default=DEFAULT_V24_MODEL.relative_to(PROJECT_ROOT).as_posix())
    parser.add_argument("--artifact-dir", default=DEFAULT_ARTIFACT_DIR.relative_to(PROJECT_ROOT).as_posix())
    return parser.parse_args()


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


def safe_divide(numerator: float, denominator: float) -> float:
    return float(numerator) / float(denominator) if denominator else 0.0


def metric_delta(candidate: float, baseline: float) -> float:
    return safe_divide(candidate - baseline, baseline)


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def score_model(model_path: Path, df: DataFrame, score_col: str) -> DataFrame:
    model = PipelineModel.load(str(model_path))
    return model.transform(df).select(
        "client_id",
        "label",
        vector_to_array(F.col("probability"))[1].alias(score_col),
    )


def evaluator_metric(df: DataFrame, score_col: str, metric_name: str) -> float | None:
    row = df.agg(
        F.count("*").alias("row_count"),
        F.sum(F.col("label")).alias("positive_count"),
    ).collect()[0]
    row_count = int(row["row_count"] or 0)
    positive_count = int(row["positive_count"] or 0)
    if row_count <= 1 or positive_count == 0 or positive_count == row_count:
        return None
    return BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol=score_col,
        metricName=metric_name,
    ).evaluate(df.select("label", score_col))


def topk_for_scored(df: DataFrame, score_col: str, row_count: int, positive_count: int, positive_rate: float) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    for percent in TOPK_PERCENTS:
        k_count = max(1, int(row_count * percent + 0.999999))
        row = df.orderBy(F.desc(score_col)).limit(k_count).agg(
            F.count(F.lit(1)).alias("top_k_count"),
            F.sum(F.col("label").cast("long")).alias("positive_count"),
        ).collect()[0]
        positives = int(row["positive_count"] or 0)
        top_count = int(row["top_k_count"] or 0)
        precision = safe_divide(positives, top_count)
        recall = safe_divide(positives, positive_count)
        lift = safe_divide(precision, positive_rate)
        suffix = f"{int(percent * 100)}pct"
        metrics[f"precision_at_{suffix}"] = precision
        metrics[f"recall_at_{suffix}"] = recall
        metrics[f"lift_at_{suffix}"] = lift
    return metrics


def model_metrics(scored: DataFrame, score_col: str) -> dict[str, Any]:
    row = scored.agg(
        F.count("*").alias("row_count"),
        F.sum(F.col("label")).alias("positive_count"),
    ).collect()[0]
    row_count = int(row["row_count"] or 0)
    positive_count = int(row["positive_count"] or 0)
    negative_count = row_count - positive_count
    positive_rate = safe_divide(positive_count, row_count)
    metrics = {
        "row_count": row_count,
        "positive_count": positive_count,
        "negative_count": negative_count,
        "positive_rate": positive_rate,
        "roc_auc": evaluator_metric(scored, score_col, "areaUnderROC"),
        "pr_auc": evaluator_metric(scored, score_col, "areaUnderPR"),
    }
    metrics.update(topk_for_scored(scored, score_col, row_count, positive_count, positive_rate))
    return metrics


def segment_definitions(df: DataFrame) -> tuple[DataFrame, dict[str, Any]]:
    active_median = df.approxQuantile("active_days_count", [0.5], 0.01)[0]
    with_segments = (
        df.withColumn(
            "activity_segment",
            F.when(F.col("active_days_count") > F.lit(float(active_median)), F.lit("high_activity")).otherwise(F.lit("low_activity")),
        )
        .withColumn(
            "lifecycle_segment",
            F.when(F.col("product_buy_count") > F.lit(0), F.lit("returning_users")).otherwise(F.lit("new_users")),
        )
    )
    metadata = {
        "activity_segment_rule": f"high_activity if active_days_count > validation median ({active_median:.6f}); otherwise low_activity",
        "lifecycle_segment_rule": "returning_users if pre-cutoff product_buy_count > 0; otherwise new_users",
        "time_slice_available": False,
        "time_slice_note": "The temporal validation dataset is a single cutoff snapshot, so row-level prediction time slices are not available without generating additional cutoffs.",
    }
    return with_segments, metadata


def segment_metrics(scored: DataFrame, score_col: str, model_name: str, segment_col: str) -> list[dict[str, Any]]:
    rows = []
    segment_values = [row[segment_col] for row in scored.select(segment_col).distinct().collect()]
    for segment_value in sorted(segment_values):
        segment_df = scored.where(F.col(segment_col) == F.lit(segment_value)).cache()
        metrics = model_metrics(segment_df, score_col)
        rows.append(
            {
                "model": model_name,
                "segment_type": segment_col,
                "segment_name": segment_value,
                **metrics,
            }
        )
        segment_df.unpersist()
    return rows


def topk_overlap(joined: DataFrame, row_count: int) -> list[dict[str, Any]]:
    rows = []
    for percent in TOPK_PERCENTS:
        k_count = max(1, int(row_count * percent + 0.999999))
        v22_top = joined.orderBy(F.desc("score_v22")).limit(k_count).select("client_id").withColumn("in_v22_top", F.lit(1))
        v24_top = joined.orderBy(F.desc("score_v24")).limit(k_count).select("client_id").withColumn("in_v24_top", F.lit(1))
        overlap = v22_top.join(v24_top, "client_id", "inner").count()
        rows.append(
            {
                "k": f"{int(percent * 100)}%",
                "top_k_count_v22": k_count,
                "top_k_count_v24": k_count,
                "overlap_count": overlap,
                "overlap_rate": safe_divide(overlap, k_count),
                "v22_only_count": k_count - overlap,
                "v24_only_count": k_count - overlap,
            }
        )
    return rows


def distribution_for_score(df: DataFrame, score_col: str) -> dict[str, Any]:
    row = df.agg(
        F.count(F.col(score_col)).alias("count"),
        F.mean(F.col(score_col)).alias("mean"),
        F.variance(F.col(score_col)).alias("variance"),
        F.stddev(F.col(score_col)).alias("stddev"),
        F.min(F.col(score_col)).alias("min"),
        F.max(F.col(score_col)).alias("max"),
    ).collect()[0].asDict()
    quantiles = df.approxQuantile(score_col, [0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99], 0.001)
    return {
        "count": int(row["count"] or 0),
        "mean": row["mean"],
        "variance": row["variance"],
        "stddev": row["stddev"],
        "min": row["min"],
        "max": row["max"],
        "p01": quantiles[0],
        "p05": quantiles[1],
        "p10": quantiles[2],
        "p25": quantiles[3],
        "p50": quantiles[4],
        "p75": quantiles[5],
        "p90": quantiles[6],
        "p95": quantiles[7],
        "p99": quantiles[8],
    }


def score_distribution_summary(joined: DataFrame) -> dict[str, Any]:
    delta_df = joined.withColumn("score_delta_v24_minus_v22", F.col("score_v24") - F.col("score_v22")).cache()
    summary = {
        "v2_2": distribution_for_score(delta_df, "score_v22"),
        "v2_4": distribution_for_score(delta_df, "score_v24"),
        "score_delta_v24_minus_v22": distribution_for_score(delta_df, "score_delta_v24_minus_v22"),
    }
    delta_df.unpersist()
    return summary


def recommendation(overall_rows: list[dict[str, Any]], segment_rows: list[dict[str, Any]]) -> tuple[str, str]:
    v22 = next(row for row in overall_rows if row["model"] == "baseline_v2_2")
    v24 = next(row for row in overall_rows if row["model"] == "baseline_v2_4")
    improves_core = v24["pr_auc"] > v22["pr_auc"] and v24["lift_at_5pct"] > v22["lift_at_5pct"]
    important_segments = {"high_activity", "low_activity", "new_users", "returning_users"}
    regressions = []
    for segment in important_segments:
        v22_seg = next(row for row in segment_rows if row["model"] == "baseline_v2_2" and row["segment_name"] == segment)
        v24_seg = next(row for row in segment_rows if row["model"] == "baseline_v2_4" and row["segment_name"] == segment)
        if v24_seg["pr_auc"] is not None and v22_seg["pr_auc"] is not None and v24_seg["lift_at_5pct"] < v22_seg["lift_at_5pct"]:
            regressions.append(segment)
    if improves_core and not regressions:
        return "PROMOTE V2-4", "V2-4 improves both PR-AUC and Lift@5 versus V2-2 with no Lift@5 regression across the required user segments."
    if improves_core:
        return "PROMOTE V2-4", f"V2-4 improves PR-AUC and Lift@5 overall; segment Lift@5 regressions require monitoring: {', '.join(regressions)}."
    return "KEEP V2-2", "V2-4 does not improve both PR-AUC and Lift@5 versus V2-2."


def write_report(
    path: Path,
    overall_rows: list[dict[str, Any]],
    segment_rows: list[dict[str, Any]],
    overlap_rows: list[dict[str, Any]],
    distribution: dict[str, Any],
    segment_metadata: dict[str, Any],
    decision_text: str,
    decision_reason: str,
) -> None:
    v22 = next(row for row in overall_rows if row["model"] == "baseline_v2_2")
    v24 = next(row for row in overall_rows if row["model"] == "baseline_v2_4")
    lines = [
        "# E9 Final Benchmark Report",
        "",
        "## Scope",
        "",
        "E9 compares existing trained V2-2 and V2-4 models on the temporal validation snapshot. No model retraining, feature redesign, preprocessing change, or model architecture change is performed.",
        "",
        "## Overall Metrics",
        "",
        "| Model | ROC-AUC | PR-AUC | Precision@1% | Precision@5% | Precision@10% | Lift@1% | Lift@5% | Lift@10% |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in overall_rows:
        lines.append(
            f"| `{row['model']}` | {row['roc_auc']:.6f} | {row['pr_auc']:.6f} | "
            f"{row['precision_at_1pct']:.6f} | {row['precision_at_5pct']:.6f} | {row['precision_at_10pct']:.6f} | "
            f"{row['lift_at_1pct']:.6f} | {row['lift_at_5pct']:.6f} | {row['lift_at_10pct']:.6f} |"
        )
    lines.extend(
        [
            "",
            "## Overall Delta",
            "",
            f"- PR-AUC change: {metric_delta(v24['pr_auc'], v22['pr_auc']):.2%}",
            f"- Lift@5 change: {metric_delta(v24['lift_at_5pct'], v22['lift_at_5pct']):.2%}",
            "",
            "## Segment Stability",
            "",
            f"- Activity segmentation: {segment_metadata['activity_segment_rule']}",
            f"- Lifecycle segmentation: {segment_metadata['lifecycle_segment_rule']}",
            f"- Time-slice analysis: {segment_metadata['time_slice_note']}",
            "",
            "| Segment type | Segment | V2-2 PR-AUC | V2-4 PR-AUC | V2-2 Lift@5 | V2-4 Lift@5 | Lift@5 delta |",
            "|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    for segment_name in ["high_activity", "low_activity", "new_users", "returning_users"]:
        v22_seg = next(row for row in segment_rows if row["model"] == "baseline_v2_2" and row["segment_name"] == segment_name)
        v24_seg = next(row for row in segment_rows if row["model"] == "baseline_v2_4" and row["segment_name"] == segment_name)
        lines.append(
            f"| `{v22_seg['segment_type']}` | `{segment_name}` | "
            f"{v22_seg['pr_auc'] if v22_seg['pr_auc'] is not None else 'NA'} | "
            f"{v24_seg['pr_auc'] if v24_seg['pr_auc'] is not None else 'NA'} | "
            f"{v22_seg['lift_at_5pct']:.6f} | {v24_seg['lift_at_5pct']:.6f} | "
            f"{metric_delta(v24_seg['lift_at_5pct'], v22_seg['lift_at_5pct']):.2%} |"
        )
    lines.extend(
        [
            "",
            "## Ranking Stability",
            "",
            "| K | Overlap count | Overlap rate | V2-2 only | V2-4 only |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for row in overlap_rows:
        lines.append(
            f"| {row['k']} | {row['overlap_count']} | {row['overlap_rate']:.6f} | {row['v22_only_count']} | {row['v24_only_count']} |"
        )
    lines.extend(
        [
            "",
            "## Score Distribution",
            "",
            f"- V2-2 score mean: {distribution['v2_2']['mean']:.6f}",
            f"- V2-4 score mean: {distribution['v2_4']['mean']:.6f}",
            f"- Mean score delta, V2-4 minus V2-2: {distribution['score_delta_v24_minus_v22']['mean']:.6f}",
            "",
            "## Risk Review",
            "",
            "The final decision is based only on PR-AUC, Lift@5, and segment stability. ROC-AUC is reported but is secondary for this imbalanced ranking use case.",
            "",
            "## Decision",
            "",
            decision_text,
            "",
            decision_reason,
            "",
            "## Privacy",
            "",
            "Artifacts contain aggregate metrics only. No raw client IDs, raw query text, product names, row-level examples, or row-level prediction files are written.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    config_path = V21.resolve_repo_path(args.feature_config)
    validation_input = V21.resolve_repo_path(args.validation_input)
    events_base = V21.resolve_repo_path(args.events_base)
    v22_model = V21.resolve_repo_path(args.v22_model)
    v24_model = V21.resolve_repo_path(args.v24_model)
    artifact_dir = V21.resolve_repo_path(args.artifact_dir)
    report_path = artifact_dir / "v2_e9_final_benchmark_report.md"
    segment_path = artifact_dir / "v2_e9_segment_analysis.csv"
    overlap_path = artifact_dir / "v2_e9_topk_overlap_analysis.csv"
    distribution_path = artifact_dir / "v2_e9_score_distribution_summary.json"

    _ = load_json(config_path)
    spark = E6.start_spark()
    try:
        validation_base = spark.read.parquet(str(validation_input)).cache()
        events_by_name = {
            name: spark.read.parquet(str(events_base / name)).cache()
            for name in E6.EVENT_TABLES_FOR_ACTIVITY
        }
        validation_activity_proxy = E6.active_days_30d_proxy(events_by_name, E6.VALIDATION_CUTOFF_DATE).cache()
        validation_augmented = E6.add_e6_features(validation_base, validation_activity_proxy).cache()
        segmented, segment_metadata = segment_definitions(validation_augmented)
        segmented = segmented.cache()

        scored_v22 = score_model(v22_model, segmented, "score_v22").cache()
        scored_v24 = score_model(v24_model, segmented, "score_v24").cache()
        joined_scores = scored_v22.join(scored_v24.select("client_id", "score_v24"), "client_id", "inner").cache()
        joined_segmented = (
            joined_scores.join(segmented.select("client_id", "activity_segment", "lifecycle_segment"), "client_id", "left")
            .cache()
        )

        overall_rows = [
            {"model": "baseline_v2_2", **model_metrics(scored_v22, "score_v22")},
            {"model": "baseline_v2_4", **model_metrics(scored_v24, "score_v24")},
        ]
        segment_rows = []
        segment_rows.extend(segment_metrics(joined_segmented.select("label", "score_v22", "activity_segment"), "score_v22", "baseline_v2_2", "activity_segment"))
        segment_rows.extend(segment_metrics(joined_segmented.select("label", "score_v24", "activity_segment"), "score_v24", "baseline_v2_4", "activity_segment"))
        segment_rows.extend(segment_metrics(joined_segmented.select("label", "score_v22", "lifecycle_segment"), "score_v22", "baseline_v2_2", "lifecycle_segment"))
        segment_rows.extend(segment_metrics(joined_segmented.select("label", "score_v24", "lifecycle_segment"), "score_v24", "baseline_v2_4", "lifecycle_segment"))

        overlap_rows = topk_overlap(joined_scores, overall_rows[0]["row_count"])
        distribution = score_distribution_summary(joined_scores)
        distribution["created_at_utc"] = datetime.now(timezone.utc).isoformat()
        distribution["segment_metadata"] = segment_metadata
        distribution_path.parent.mkdir(parents=True, exist_ok=True)
        distribution_path.write_text(
            json.dumps({key: normalize_value(value) if not isinstance(value, dict) else value for key, value in distribution.items()}, indent=2),
            encoding="utf-8",
        )
        write_csv(
            segment_path,
            segment_rows,
            [
                "model",
                "segment_type",
                "segment_name",
                "row_count",
                "positive_count",
                "negative_count",
                "positive_rate",
                "roc_auc",
                "pr_auc",
                "precision_at_1pct",
                "recall_at_1pct",
                "lift_at_1pct",
                "precision_at_5pct",
                "recall_at_5pct",
                "lift_at_5pct",
                "precision_at_10pct",
                "recall_at_10pct",
                "lift_at_10pct",
            ],
        )
        write_csv(
            overlap_path,
            overlap_rows,
            ["k", "top_k_count_v22", "top_k_count_v24", "overlap_count", "overlap_rate", "v22_only_count", "v24_only_count"],
        )
        decision_text, decision_reason = recommendation(overall_rows, segment_rows)
        write_report(
            report_path,
            overall_rows,
            segment_rows,
            overlap_rows,
            distribution,
            segment_metadata,
            decision_text,
            decision_reason,
        )

        print("E9 final benchmark completed.")
        print(f"V2-2 PR-AUC: {overall_rows[0]['pr_auc']:.6f}; Lift@5%: {overall_rows[0]['lift_at_5pct']:.6f}")
        print(f"V2-4 PR-AUC: {overall_rows[1]['pr_auc']:.6f}; Lift@5%: {overall_rows[1]['lift_at_5pct']:.6f}")
        print(f"Decision: {decision_text}")
        print(f"Report: {V21.relative_path(report_path)}")
        print(f"Segment analysis: {V21.relative_path(segment_path)}")
        print(f"TopK overlap: {V21.relative_path(overlap_path)}")
        print(f"Score distribution: {V21.relative_path(distribution_path)}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
