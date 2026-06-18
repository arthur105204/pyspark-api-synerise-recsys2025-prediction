"""Run E4 follow-up combined ablation and redundancy audit.

This job investigates whether active_days_count absorbs signal from cart/buy
activity features. It uses the same temporal train/validation snapshots and
class-weighted Logistic Regression setup as E1/E4, then writes aggregate-only
follow-up artifacts.

It does not add features, tune hyperparameters, change labels, change cohorts,
calibrate scores, benchmark new model classes, persist row-level predictions,
or write model binaries.
"""

from __future__ import annotations

import argparse
import csv
import math
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
DEFAULT_ARTIFACT_DIR = PROJECT_ROOT / "artifacts" / "modeling" / "e4_feature_ablation_followup"
DEFAULT_MAX_ITER = 20
TOPK_PERCENTS = (0.01, 0.05, 0.10)
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
    "cohort_indicator": ["is_eligible_purchase_propensity"],
    "overall_activity": ["active_days_count"],
}
EXPERIMENTS = [
    ("baseline", []),
    ("remove_overall_activity", ["overall_activity"]),
    ("remove_overall_plus_add_to_cart", ["overall_activity", "add_to_cart_activity"]),
    ("remove_overall_plus_product_buy", ["overall_activity", "product_buy_activity"]),
    (
        "remove_overall_plus_add_to_cart_plus_product_buy",
        ["overall_activity", "add_to_cart_activity", "product_buy_activity"],
    ),
    ("remove_overall_plus_recency", ["overall_activity", "recency_features"]),
    (
        "remove_overall_plus_recency_plus_add_to_cart_plus_product_buy",
        ["overall_activity", "recency_features", "add_to_cart_activity", "product_buy_activity"],
    ),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run E4 follow-up feature redundancy audit.")
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


def validate_feature_mapping(feature_columns: list[str]) -> None:
    mapped = [feature for features in FEATURE_FAMILIES.values() for feature in features]
    duplicate_features = sorted({feature for feature in mapped if mapped.count(feature) > 1})
    missing = sorted(set(mapped).difference(feature_columns))
    unmapped = sorted(set(feature_columns).difference(mapped))
    if duplicate_features or missing or unmapped:
        raise ValueError(
            "Feature family mapping must be exact. "
            f"duplicates={duplicate_features}; missing={missing}; unmapped={unmapped}"
        )


def dataset_summary(df: DataFrame, name: str) -> dict[str, Any]:
    row = df.agg(F.count("*").alias("row_count"), F.sum(F.col("label")).alias("positive_count")).collect()[0]
    row_count = int(row["row_count"])
    positive_count = int(row["positive_count"] or 0)
    if not row_count or not positive_count or positive_count == row_count:
        raise ValueError(f"{name} dataset must contain positive and negative labels")
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


def removed_feature_set(families: list[str]) -> set[str]:
    removed: set[str] = set()
    for family in families:
        removed.update(FEATURE_FAMILIES[family])
    return removed


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
        key = int(percent * 100)
        precision_at_k = safe_divide(observed_positive_count, observed_top_k_count)
        metrics[f"precision_at_{key}pct"] = precision_at_k
        metrics[f"lift_at_{key}pct"] = safe_divide(precision_at_k, positive_rate)
    return metrics


def train_and_evaluate(
    train_df: DataFrame,
    validation_df: DataFrame,
    feature_columns: list[str],
    max_iter: int,
    validation_summary: dict[str, Any],
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


def finite_abs(value: float | None) -> float | None:
    if value is None or math.isnan(value) or math.isinf(value):
        return None
    return abs(float(value))


def average(values: list[float]) -> float | None:
    return safe_divide(sum(values), len(values)) if values else None


def correlation(df: DataFrame, left: str, right: str, variances: dict[str, float | None]) -> float | None:
    if not variances.get(left) or not variances.get(right):
        return None
    return finite_abs(df.stat.corr(left, right))


def redundancy_audit(df: DataFrame, row_count: int) -> list[dict[str, Any]]:
    audit_rows = []
    variance_exprs = [F.variance(F.col(feature).cast("double")).alias(feature) for features in FEATURE_FAMILIES.values() for feature in features]
    variance_row = df.agg(*variance_exprs).collect()[0].asDict()
    variances_by_feature = {
        feature: (
            float(variance_row[feature])
            if variance_row.get(feature) is not None and not math.isnan(float(variance_row[feature]))
            else None
        )
        for features in FEATURE_FAMILIES.values()
        for feature in features
    }
    for family, features in FEATURE_FAMILIES.items():
        pairwise_corrs = []
        for index, left in enumerate(features):
            for right in features[index + 1 :]:
                corr = correlation(df, left, right, variances_by_feature)
                if corr is not None:
                    pairwise_corrs.append(corr)

        active_corrs = []
        for feature in features:
            if feature == "active_days_count":
                continue
            corr = correlation(df, feature, "active_days_count", variances_by_feature)
            if corr is not None:
                active_corrs.append(corr)

        aggregate_exprs = []
        for feature in features:
            aggregate_exprs.extend(
                [
                    F.sum(F.when(F.col(feature).isNull() | F.isnan(F.col(feature).cast("double")), 1).otherwise(0)).alias(
                        f"{feature}__nulls"
                    ),
                    F.variance(F.col(feature).cast("double")).alias(f"{feature}__variance"),
                ]
            )
        stats = df.agg(*aggregate_exprs).collect()[0].asDict()
        null_rates = [safe_divide(int(stats.get(f"{feature}__nulls") or 0), row_count) for feature in features]
        variances = [
            float(stats[f"{feature}__variance"])
            for feature in features
            if stats.get(f"{feature}__variance") is not None
            and not math.isnan(float(stats[f"{feature}__variance"]))
        ]
        constant_features = [feature for feature in features if not variances_by_feature.get(feature)]
        audit_rows.append(
            {
                "feature_family": family,
                "feature_count": len(features),
                "constant_feature_count": len(constant_features),
                "constant_features": ",".join(constant_features),
                "average_pairwise_correlation": average(pairwise_corrs),
                "correlation_with_active_days_count": average(active_corrs),
                "max_correlation_with_active_days_count": max(active_corrs) if active_corrs else None,
                "average_null_rate": average(null_rates),
                "max_null_rate": max(null_rates) if null_rates else None,
                "average_variance": average(variances),
                "max_variance": max(variances) if variances else None,
            }
        )
    return audit_rows


def relative_drop(baseline: float, value: float) -> float:
    return safe_divide(baseline - value, baseline)


def write_review(path: Path, combined_rows: list[dict[str, Any]], redundancy_rows: list[dict[str, Any]]) -> None:
    baseline = next(row for row in combined_rows if row["experiment_name"] == "baseline")
    by_name = {row["experiment_name"]: row for row in combined_rows}
    remove_overall = by_name["remove_overall_activity"]

    def extra_drop(name: str, metric: str) -> float:
        return relative_drop(remove_overall[metric], by_name[name][metric])

    add_extra_pr = extra_drop("remove_overall_plus_add_to_cart", "pr_auc")
    buy_extra_pr = extra_drop("remove_overall_plus_product_buy", "pr_auc")
    recency_extra_pr = extra_drop("remove_overall_plus_recency", "pr_auc")
    active_proxy_families = [
        row["feature_family"]
        for row in redundancy_rows
        if row["correlation_with_active_days_count"] is not None
        and row["correlation_with_active_days_count"] >= 0.30
    ]

    lines = [
        "# E4 Follow-up: Active Days Redundancy and Combined Ablation",
        "",
        "## Scope",
        "This follow-up investigates whether active_days_count absorbs signal from cart, buy, search, and recency features. It uses the same E1/E4 temporal snapshots and class-weighted Logistic Regression setup. It does not add features, tune hyperparameters, change labels, change cohorts, calibrate scores, benchmark new models, persist row-level predictions, or write model binaries.",
        "",
        "## Combined Ablation Results",
        "| Experiment | Removed families | ROC-AUC | PR-AUC | Precision@5% | Lift@5% | PR-AUC drop vs baseline | Lift@5% drop vs baseline |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in combined_rows:
        removed = row["removed_families"] or "none"
        lines.append(
            f"| {row['experiment_name']} | {removed} | {row['roc_auc']:.6f} | {row['pr_auc']:.6f} | "
            f"{row['precision_at_5pct']:.6f} | {row['lift_at_5pct']:.6f} | "
            f"{relative_drop(baseline['pr_auc'], row['pr_auc']):.2%} | "
            f"{relative_drop(baseline['lift_at_5pct'], row['lift_at_5pct']):.2%} |"
        )

    lines.extend(
        [
            "",
            "## Redundancy Audit",
            "| Family | Features | Avg pairwise corr | Avg corr with active_days_count | Avg null rate | Avg variance |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for row in redundancy_rows:
        pair = "-" if row["average_pairwise_correlation"] is None else f"{row['average_pairwise_correlation']:.6f}"
        active = (
            "-"
            if row["correlation_with_active_days_count"] is None
            else f"{row['correlation_with_active_days_count']:.6f}"
        )
        variance = "-" if row["average_variance"] is None else f"{row['average_variance']:.6f}"
        lines.append(
            f"| {row['feature_family']} | {row['feature_count']} | {pair} | {active} | "
            f"{row['average_null_rate']:.6f} | {variance} |"
        )

    lines.extend(
        [
            "",
            "## Questions Answered",
            f"1. Is active_days_count acting as a proxy for activity? Families with average absolute correlation >= 0.30: {', '.join(active_proxy_families) if active_proxy_families else 'none'}.",
            f"2. Does cart activity become important once overall_activity is removed? Additional PR-AUC drop after removing cart on top of overall_activity: {add_extra_pr:.2%}.",
            f"3. Does buy activity become important once overall_activity is removed? Additional PR-AUC drop after removing buy on top of overall_activity: {buy_extra_pr:.2%}.",
            f"4. Are recency and activity signals overlapping? Additional PR-AUC drop after removing recency on top of overall_activity: {recency_extra_pr:.2%}.",
            "5. Unique versus redundant signal should be judged by combined ablation drops together with the correlation audit above.",
            "6. Do not delete feature families unless the combined-ablation evidence supports it; this follow-up is diagnostic, not a cleanup implementation.",
            "",
            "## Privacy",
            "Artifacts are aggregate-only. No raw client IDs, raw query text, product names, row-level predictions, row-level scores, or model binaries are persisted.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    train_input = resolve_repo_path(args.train_input)
    validation_input = resolve_repo_path(args.validation_input)
    artifact_dir = resolve_repo_path(args.artifact_dir)
    combined_path = artifact_dir / "combined_ablation_results.csv"
    redundancy_path = artifact_dir / "feature_redundancy_audit.csv"
    review_path = artifact_dir / "combined_ablation_review.md"

    spark = SparkSession.builder.appName("e4-followup-active-days-redundancy").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        train_base = spark.read.parquet(str(train_input)).cache()
        validation_base = spark.read.parquet(str(validation_input)).cache()
        train_summary = dataset_summary(train_base, "train")
        validation_summary = dataset_summary(validation_base, "validation")
        feature_columns = numeric_feature_columns(train_base)
        validate_feature_mapping(feature_columns)

        positive_weight = train_summary["row_count"] / (2.0 * train_summary["positive_count"])
        negative_weight = train_summary["row_count"] / (2.0 * train_summary["negative_count"])
        train_df = add_class_weight(train_base, positive_weight, negative_weight).cache()

        combined_rows = []
        for experiment_name, removed_families in EXPERIMENTS:
            removed_features = removed_feature_set(removed_families)
            selected_features = [feature for feature in feature_columns if feature not in removed_features]
            print(f"Running combined ablation: {experiment_name}")
            metrics = train_and_evaluate(
                train_df=train_df,
                validation_df=validation_base,
                feature_columns=selected_features,
                max_iter=args.max_iter,
                validation_summary=validation_summary,
            )
            combined_rows.append(
                {
                    "experiment_name": experiment_name,
                    "removed_families": "+".join(removed_families),
                    "feature_count_removed": len(removed_features),
                    **metrics,
                }
            )

        baseline = next(row for row in combined_rows if row["experiment_name"] == "baseline")
        for row in combined_rows:
            row["pr_auc_drop_vs_baseline"] = relative_drop(baseline["pr_auc"], row["pr_auc"])
            row["lift_5pct_drop_vs_baseline"] = relative_drop(baseline["lift_at_5pct"], row["lift_at_5pct"])

        redundancy_rows = redundancy_audit(train_base, train_summary["row_count"])

        write_csv(
            combined_path,
            combined_rows,
            [
                "experiment_name",
                "removed_families",
                "feature_count_removed",
                "roc_auc",
                "pr_auc",
                "precision_at_1pct",
                "precision_at_5pct",
                "precision_at_10pct",
                "lift_at_1pct",
                "lift_at_5pct",
                "lift_at_10pct",
                "pr_auc_drop_vs_baseline",
                "lift_5pct_drop_vs_baseline",
            ],
        )
        write_csv(
            redundancy_path,
            redundancy_rows,
            [
                "feature_family",
                "feature_count",
                "constant_feature_count",
                "constant_features",
                "average_pairwise_correlation",
                "correlation_with_active_days_count",
                "max_correlation_with_active_days_count",
                "average_null_rate",
                "max_null_rate",
                "average_variance",
                "max_variance",
            ],
        )
        write_review(review_path, combined_rows, redundancy_rows)

        print("E4 follow-up redundancy audit completed.")
        print(f"Combined ablation results: {relative_path(combined_path)}")
        print(f"Feature redundancy audit: {relative_path(redundancy_path)}")
        print(f"Review: {relative_path(review_path)}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
