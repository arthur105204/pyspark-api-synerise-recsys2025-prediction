"""Run E4 feature family ablation for the temporal baseline setup.

This job trains one Logistic Regression model per ablation, each time removing
one predefined feature family, then evaluates temporal validation metrics. It
uses the existing E1 temporal train/validation snapshots and writes only
aggregate experiment artifacts.

It does not add features, change labels, change cohorts, tune hyperparameters,
calibrate scores, benchmark new model classes, persist row-level predictions,
or write model binaries.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
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
DEFAULT_TRAIN_INPUT = (
    PROJECT_ROOT / "data" / "processed" / "training" / "e1_train_2022_10_10" / "purchase_propensity_30d"
)
DEFAULT_VALIDATION_INPUT = (
    PROJECT_ROOT / "data" / "processed" / "training" / "e1_valid_2022_11_09" / "purchase_propensity_30d"
)
DEFAULT_ARTIFACT_DIR = PROJECT_ROOT / "artifacts" / "modeling" / "e4_feature_ablation"
DEFAULT_MAX_ITER = 20
TOPK_PERCENTS = (0.01, 0.05, 0.10)
BASELINE_METRICS = {
    "roc_auc": 0.835559,
    "pr_auc": 0.253155,
    "precision_at_1pct": 0.478742,
    "lift_at_1pct": 10.994062,
    "precision_at_5pct": 0.294837,
    "lift_at_5pct": 6.770770,
    "precision_at_10pct": 0.213922,
    "lift_at_10pct": 4.912611,
}
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
FEATURE_FAMILIES = {
    "add_to_cart_activity": [
        "add_to_cart_count",
        "distinct_add_to_cart_sku_count",
        "add_to_cart_count_30d",
        "add_to_cart_count_60d",
        "add_to_cart_count_90d",
    ],
    "product_buy_activity": [
        "product_buy_count",
        "distinct_product_buy_sku_count",
        "product_buy_count_30d",
        "product_buy_count_60d",
        "product_buy_count_90d",
    ],
    "remove_from_cart_activity": [
        "remove_from_cart_count",
        "distinct_remove_from_cart_sku_count",
        "remove_from_cart_count_30d",
        "remove_from_cart_count_60d",
        "remove_from_cart_count_90d",
    ],
    "search_activity": [
        "search_query_count",
        "distinct_search_days",
        "search_query_count_30d",
        "search_query_count_60d",
        "search_query_count_90d",
    ],
    "recency_features": [
        "days_since_last_add_to_cart",
        "days_since_last_remove_from_cart",
        "days_since_last_product_buy",
        "days_since_last_search_query",
    ],
    "ratio_features": [
        "buy_to_cart_ratio",
        "remove_to_cart_ratio",
        "cart_minus_remove_count",
        "search_to_cart_ratio",
    ],
    "product_metadata_features": [
        "distinct_cart_category_count",
        "avg_cart_price",
        "max_cart_price",
        "distinct_bought_category_count",
        "avg_bought_price",
        "max_bought_price",
    ],
    "cohort_indicator": [
        "is_eligible_purchase_propensity",
    ],
    "overall_activity": [
        "active_days_count",
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run temporal feature family ablation analysis.")
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
        "--artifact-dir",
        default=DEFAULT_ARTIFACT_DIR.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative output artifact directory.",
    )
    parser.add_argument("--max-iter", type=int, default=DEFAULT_MAX_ITER, help="Logistic Regression max iterations.")
    parser.add_argument(
        "--no-class-weights",
        action="store_true",
        help="Disable class weights. Defaults to the same class-weighted setup as baseline.",
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


def label_like_columns(columns: list[str]) -> list[str]:
    flagged = []
    for column in columns:
        lowered = column.lower()
        if lowered in {"target", "y"} or lowered.startswith(LABEL_LIKE_PREFIXES):
            flagged.append(column)
    return flagged


def numeric_feature_columns(df: DataFrame) -> list[str]:
    numeric_types = (
        T.ByteType,
        T.ShortType,
        T.IntegerType,
        T.LongType,
        T.FloatType,
        T.DoubleType,
        T.DecimalType,
    )
    blocked = EXCLUDED_COLUMNS.union(label_like_columns(df.columns))
    return [
        field.name
        for field in df.schema.fields
        if field.name not in blocked and isinstance(field.dataType, numeric_types)
    ]


def validate_feature_family_mapping(feature_columns: list[str]) -> dict[str, Any]:
    feature_to_family: dict[str, str] = {}
    duplicate_features = []
    for family, features in FEATURE_FAMILIES.items():
        for feature in features:
            if feature in feature_to_family:
                duplicate_features.append(feature)
            feature_to_family[feature] = family

    missing_from_dataset = sorted(set(feature_to_family).difference(feature_columns))
    unmapped_features = sorted(set(feature_columns).difference(feature_to_family))
    if duplicate_features or missing_from_dataset or unmapped_features:
        raise ValueError(
            "Feature family mapping must be one-to-one. "
            f"duplicates={duplicate_features}; missing_from_dataset={missing_from_dataset}; "
            f"unmapped_features={unmapped_features}"
        )
    return {
        "feature_to_family": feature_to_family,
        "family_to_features": FEATURE_FAMILIES,
    }


def dataset_summary(df: DataFrame, name: str) -> dict[str, Any]:
    row = df.agg(
        F.count("*").alias("row_count"),
        F.sum(F.col("label")).alias("positive_count"),
    ).collect()[0]
    row_count = int(row["row_count"])
    positive_count = int(row["positive_count"] or 0)
    if not row_count or not positive_count or positive_count == row_count:
        raise ValueError(f"{name} dataset must contain nonzero positive and negative rows")
    return {
        "row_count": row_count,
        "positive_count": positive_count,
        "negative_count": row_count - positive_count,
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
    use_class_weights: bool,
    validation_summary: dict[str, Any],
) -> dict[str, Any]:
    imputed_columns = [f"{column}__imputed" for column in feature_columns]
    imputer = Imputer(inputCols=feature_columns, outputCols=imputed_columns, strategy="median")
    assembler = VectorAssembler(inputCols=imputed_columns, outputCol="features")
    lr_kwargs: dict[str, Any] = {
        "featuresCol": "features",
        "labelCol": "label",
        "maxIter": int(max_iter),
        "standardization": True,
    }
    if use_class_weights:
        lr_kwargs["weightCol"] = "class_weight"

    pipeline = Pipeline(stages=[imputer, assembler, LogisticRegression(**lr_kwargs)])
    model = pipeline.fit(train_df)
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
    return {
        "roc_auc": roc_auc,
        "pr_auc": pr_auc,
        **topk,
    }


def relative_drop(baseline_value: float, ablation_value: float) -> float:
    return safe_divide(baseline_value - ablation_value, baseline_value)


def classify_family(pr_auc_drop_relative: float, lift_5pct_drop_relative: float) -> str:
    max_drop = max(pr_auc_drop_relative, lift_5pct_drop_relative)
    if max_drop > 0.03:
        return "KEEP"
    if max_drop >= 0.01:
        return "KEEP_LOWER_PRIORITY"
    return "REMOVE_CANDIDATE"


def enrich_results(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    enriched = []
    for row in rows:
        pr_auc_drop_relative = relative_drop(BASELINE_METRICS["pr_auc"], row["pr_auc"])
        lift_5pct_drop_relative = relative_drop(BASELINE_METRICS["lift_at_5pct"], row["lift_at_5pct"])
        enriched.append(
            {
                **row,
                "roc_auc_drop_relative": relative_drop(BASELINE_METRICS["roc_auc"], row["roc_auc"]),
                "pr_auc_drop_relative": pr_auc_drop_relative,
                "lift_5pct_drop_relative": lift_5pct_drop_relative,
                "recommendation": classify_family(pr_auc_drop_relative, lift_5pct_drop_relative),
            }
        )
    return enriched


def write_review(path: Path, summary: dict[str, Any], enriched_rows: list[dict[str, Any]]) -> None:
    ranked_rows = sorted(enriched_rows, key=lambda row: max(row["pr_auc_drop_relative"], row["lift_5pct_drop_relative"]), reverse=True)
    lines = [
        "# E4 Feature Ablation Analysis",
        "",
        "## Scope",
        "This experiment removes one existing feature family at a time and evaluates temporal validation performance using the same Logistic Regression setup as E1. It does not add features, change labels, modify cohorts, tune hyperparameters, calibrate scores, benchmark new model classes, persist row-level predictions, or write model binaries.",
        "",
        "## Baseline Reference",
        f"- ROC-AUC: {BASELINE_METRICS['roc_auc']:.6f}",
        f"- PR-AUC: {BASELINE_METRICS['pr_auc']:.6f}",
        f"- Lift@5%: {BASELINE_METRICS['lift_at_5pct']:.6f}",
        "",
        "## Feature Family Mapping",
    ]
    for family, features in FEATURE_FAMILIES.items():
        lines.append(f"- {family}: {', '.join(features)}")

    lines.extend(
        [
            "",
            "## Ablation Results",
            "| Removed family | Removed features | ROC-AUC | PR-AUC | Precision@5% | Lift@5% | PR-AUC drop | Lift@5% drop | Recommendation |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    for row in ranked_rows:
        lines.append(
            f"| {row['feature_family_removed']} | {row['feature_count_removed']} | "
            f"{row['roc_auc']:.6f} | {row['pr_auc']:.6f} | {row['precision_at_5pct']:.6f} | "
            f"{row['lift_at_5pct']:.6f} | {row['pr_auc_drop_relative']:.2%} | "
            f"{row['lift_5pct_drop_relative']:.2%} | {row['recommendation']} |"
        )

    lines.extend(
        [
            "",
            "## Key Findings",
            f"- Most signal by PR-AUC drop: {summary['largest_pr_auc_drop']['feature_family_removed']}",
            f"- Most important for TopK Lift@5%: {summary['largest_lift_5pct_drop']['feature_family_removed']}",
            f"- Least disruptive ablation: {summary['best_performing_ablation']['feature_family_removed']}",
            f"- Cohort indicator recommendation: {summary['cohort_indicator_recommendation']}",
            "",
            "## Final Recommendations",
        ]
    )
    for row in ranked_rows:
        lines.append(f"- {row['feature_family_removed']}: {row['recommendation']}")

    lines.extend(
        [
            "",
            "## Privacy",
            "Artifacts are aggregate-only. No raw client IDs, raw query text, product names, row-level scores, row-level prediction examples, or model binaries are persisted.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    train_input = resolve_repo_path(args.train_input)
    validation_input = resolve_repo_path(args.validation_input)
    artifact_dir = resolve_repo_path(args.artifact_dir)
    results_path = artifact_dir / "feature_ablation_results.csv"
    summary_path = artifact_dir / "feature_ablation_summary.json"
    review_path = artifact_dir / "feature_ablation_review.md"
    use_class_weights = not args.no_class_weights

    spark = SparkSession.builder.appName("e4-feature-ablation-temporal").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        train_base = spark.read.parquet(str(train_input)).cache()
        validation_base = spark.read.parquet(str(validation_input)).cache()
        train_summary = dataset_summary(train_base, "train")
        validation_summary = dataset_summary(validation_base, "validation")
        feature_columns = numeric_feature_columns(train_base)
        mapping = validate_feature_family_mapping(feature_columns)

        if numeric_feature_columns(validation_base) != feature_columns:
            raise ValueError("Train and validation feature columns differ")

        positive_weight = train_summary["row_count"] / (2.0 * train_summary["positive_count"])
        negative_weight = train_summary["row_count"] / (2.0 * train_summary["negative_count"])
        train_df = add_class_weight(train_base, positive_weight, negative_weight).cache() if use_class_weights else train_base

        rows = []
        for family, removed_features in FEATURE_FAMILIES.items():
            selected_features = [column for column in feature_columns if column not in set(removed_features)]
            print(f"Running ablation: remove {family} ({len(removed_features)} features)")
            metrics = train_and_evaluate(
                train_df=train_df,
                validation_df=validation_base,
                feature_columns=selected_features,
                max_iter=args.max_iter,
                use_class_weights=use_class_weights,
                validation_summary=validation_summary,
            )
            rows.append(
                {
                    "feature_family_removed": family,
                    "feature_count_removed": len(removed_features),
                    **metrics,
                }
            )

        enriched_rows = enrich_results(rows)
        best_performing = max(enriched_rows, key=lambda row: row["pr_auc"])
        worst_performing = min(enriched_rows, key=lambda row: row["pr_auc"])
        largest_pr_auc_drop = max(enriched_rows, key=lambda row: row["pr_auc_drop_relative"])
        largest_lift_5pct_drop = max(enriched_rows, key=lambda row: row["lift_5pct_drop_relative"])
        essential = [row["feature_family_removed"] for row in enriched_rows if row["recommendation"] == "KEEP"]
        low_value = [row["feature_family_removed"] for row in enriched_rows if row["recommendation"] == "REMOVE_CANDIDATE"]
        cohort_row = next(row for row in enriched_rows if row["feature_family_removed"] == "cohort_indicator")
        cohort_recommendation = (
            "Recommend excluding from future modeling"
            if abs(cohort_row["pr_auc_drop_relative"]) < 0.01
            else "Keep for now; ablation changed PR-AUC by at least 1% relative"
        )

        write_csv(
            results_path,
            rows,
            [
                "feature_family_removed",
                "feature_count_removed",
                "roc_auc",
                "pr_auc",
                "precision_at_1pct",
                "lift_at_1pct",
                "precision_at_5pct",
                "lift_at_5pct",
                "precision_at_10pct",
                "lift_at_10pct",
            ],
        )

        summary = {
            "generated_at_timestamp": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "experiment": "E4 Feature Ablation Analysis",
            "status": "success",
            "train_input_path": relative_path(train_input),
            "validation_input_path": relative_path(validation_input),
            "results_path": relative_path(results_path),
            "review_path": relative_path(review_path),
            "train_rows": train_summary["row_count"],
            "validation_rows": validation_summary["row_count"],
            "feature_count_baseline": len(feature_columns),
            "class_weighting_enabled": use_class_weights,
            "positive_class_weight": positive_weight if use_class_weights else None,
            "negative_class_weight": negative_weight if use_class_weights else None,
            "baseline_metrics": BASELINE_METRICS,
            "feature_family_mapping": mapping["family_to_features"],
            "best_performing_ablation": best_performing,
            "worst_performing_ablation": worst_performing,
            "largest_pr_auc_drop": largest_pr_auc_drop,
            "largest_lift_5pct_drop": largest_lift_5pct_drop,
            "families_marked_essential": essential,
            "families_marked_low_value": low_value,
            "cohort_indicator_recommendation": cohort_recommendation,
            "all_ablation_results_with_drops": enriched_rows,
            "decision_gates": {
                "essential": "PR-AUC drop > 3% relative OR Lift@5% drop > 3% relative",
                "moderately_useful": "1% to 3% relative drop",
                "low_value": "drop < 1% relative",
                "cohort_indicator_special_test": "if removing changes PR-AUC by less than 1% relative, recommend excluding from future modeling",
            },
            "privacy": {
                "artifact_level": "aggregate_only",
                "row_level_predictions_persisted": False,
                "raw_client_ids_persisted": False,
                "raw_query_text_persisted": False,
                "product_names_persisted": False,
                "model_binaries_persisted": False,
            },
        }
        write_json(summary_path, summary)
        write_review(review_path, summary, enriched_rows)

        print("E4 feature ablation completed.")
        print(f"Ablations run: {len(rows)}")
        print(f"Largest PR-AUC drop: {largest_pr_auc_drop['feature_family_removed']}")
        print(f"Largest Lift@5% drop: {largest_lift_5pct_drop['feature_family_removed']}")
        print(f"Results: {relative_path(results_path)}")
        print(f"Summary: {relative_path(summary_path)}")
        print(f"Review: {relative_path(review_path)}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
