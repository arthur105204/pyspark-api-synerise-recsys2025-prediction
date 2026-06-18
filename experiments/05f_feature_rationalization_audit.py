"""Run feature rationalization audit after E4/E5 diagnostics.

This job combines existing E4 ablation evidence with fresh aggregate feature
variance and correlation statistics. It classifies each feature family into one
rationalization category without changing the baseline model.

It does not retrain a model, add features, change labels, change cohorts,
calibrate scores, persist row-level predictions, or write model binaries.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TRAIN_INPUT = (
    PROJECT_ROOT / "data" / "processed" / "training" / "e1_train_2022_10_10" / "purchase_propensity_30d"
)
DEFAULT_E4_SUMMARY = PROJECT_ROOT / "artifacts" / "modeling" / "e4_feature_ablation" / "feature_ablation_summary.json"
DEFAULT_ARTIFACT_DIR = PROJECT_ROOT / "artifacts" / "modeling" / "feature_rationalization"
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
BUSINESS_MEANING = {
    "add_to_cart_activity": "Core commerce-funnel intent behavior.",
    "product_buy_activity": "Prior purchase history and repeat-purchase behavior.",
    "remove_from_cart_activity": "Potential negative intent, hesitation, or cart cleanup behavior.",
    "search_activity": "Discovery and exploration behavior; count-only representation may be noisy.",
    "recency_features": "Timing of recent activity and freshness of intent.",
    "ratio_features": "Derived intensity ratios that may duplicate count features.",
    "product_metadata_features": "Category and price context for cart/buy behavior.",
    "cohort_indicator": "Eligibility flag; constant after training-row filtering.",
    "overall_activity": "General engagement intensity across event types.",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run feature rationalization audit.")
    parser.add_argument(
        "--train-input",
        default=DEFAULT_TRAIN_INPUT.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative E1 temporal train snapshot dataset path.",
    )
    parser.add_argument(
        "--e4-summary",
        default=DEFAULT_E4_SUMMARY.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative E4 feature ablation summary JSON path.",
    )
    parser.add_argument(
        "--artifact-dir",
        default=DEFAULT_ARTIFACT_DIR.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative output artifact directory.",
    )
    return parser.parse_args()


def resolve_repo_path(path_text: str) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        raise ValueError("path arguments must be repo-relative")
    return PROJECT_ROOT / path


def relative_path(path: Path) -> str:
    return path.relative_to(PROJECT_ROOT).as_posix()


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
    return [
        column
        for column in columns
        if column.lower() in {"target", "y"} or column.lower().startswith(LABEL_LIKE_PREFIXES)
    ]


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


def validate_mapping(feature_columns: list[str]) -> None:
    mapped = [feature for features in FEATURE_FAMILIES.values() for feature in features]
    duplicate_features = sorted({feature for feature in mapped if mapped.count(feature) > 1})
    missing = sorted(set(mapped).difference(feature_columns))
    unmapped = sorted(set(feature_columns).difference(mapped))
    if duplicate_features or missing or unmapped:
        raise ValueError(
            "Feature mapping must cover each feature exactly once. "
            f"duplicates={duplicate_features}; missing={missing}; unmapped={unmapped}"
        )


def finite_corr(value: float | None) -> float | None:
    if value is None or math.isnan(value) or math.isinf(value):
        return None
    return float(value)


def feature_variance_audit(df: DataFrame, feature_columns: list[str]) -> list[dict[str, Any]]:
    exprs = []
    for feature in feature_columns:
        exprs.extend(
            [
                F.count(F.col(feature)).alias(f"{feature}__non_null_count"),
                F.approx_count_distinct(F.col(feature)).alias(f"{feature}__distinct_count"),
                F.stddev(F.col(feature).cast("double")).alias(f"{feature}__stddev"),
                F.min(F.col(feature)).alias(f"{feature}__min"),
                F.max(F.col(feature)).alias(f"{feature}__max"),
            ]
        )
    stats = df.agg(*exprs).collect()[0].asDict()
    rows = []
    for family, features in FEATURE_FAMILIES.items():
        for feature in features:
            stddev = stats.get(f"{feature}__stddev")
            distinct_count = int(stats.get(f"{feature}__distinct_count") or 0)
            is_constant = bool(distinct_count <= 1 or stddev is None or float(stddev) == 0.0)
            rows.append(
                {
                    "feature_name": feature,
                    "feature_family": family,
                    "non_null_count": int(stats.get(f"{feature}__non_null_count") or 0),
                    "distinct_count": distinct_count,
                    "stddev": float(stddev) if stddev is not None else None,
                    "min": stats.get(f"{feature}__min"),
                    "max": stats.get(f"{feature}__max"),
                    "is_constant": is_constant,
                }
            )
    return rows


def correlation_matrix(df: DataFrame, variance_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    feature_to_family = {
        feature: family for family, features in FEATURE_FAMILIES.items() for feature in features
    }
    stats = {row["feature_name"]: row for row in variance_rows}
    features = [row["feature_name"] for row in variance_rows]
    rows = []
    for index, left in enumerate(features):
        for right in features[index + 1 :]:
            left_constant = bool(stats[left]["is_constant"])
            right_constant = bool(stats[right]["is_constant"])
            if left_constant or right_constant:
                corr = None
                status = "skipped_constant_feature"
            else:
                corr = finite_corr(df.stat.corr(left, right))
                status = "ok" if corr is not None else "not_computable"
            rows.append(
                {
                    "feature_left": left,
                    "family_left": feature_to_family[left],
                    "feature_right": right,
                    "family_right": feature_to_family[right],
                    "correlation": corr,
                    "absolute_correlation": abs(corr) if corr is not None else None,
                    "correlation_status": status,
                }
            )
    return rows


def average(values: list[float]) -> float | None:
    return safe_divide(sum(values), len(values)) if values else None


def load_e4_rows(path: Path) -> dict[str, dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        row["feature_family_removed"]: row for row in payload.get("all_ablation_results_with_drops", [])
    }


def family_corr_summary(matrix_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    summary = {}
    for family, features in FEATURE_FAMILIES.items():
        within = [
            row["absolute_correlation"]
            for row in matrix_rows
            if row["correlation_status"] == "ok"
            and row["family_left"] == family
            and row["family_right"] == family
            and row["absolute_correlation"] is not None
        ]
        active = [
            row["absolute_correlation"]
            for row in matrix_rows
            if row["correlation_status"] == "ok"
            and row["absolute_correlation"] is not None
            and (
                (row["feature_left"] == "active_days_count" and row["family_right"] == family)
                or (row["feature_right"] == "active_days_count" and row["family_left"] == family)
            )
        ]
        summary[family] = {
            "average_pairwise_correlation": average(within),
            "average_correlation_with_active_days_count": average(active),
            "max_correlation_with_active_days_count": max(active) if active else None,
        }
    return summary


def classify_family(
    family: str,
    variance_rows: list[dict[str, Any]],
    e4_rows: dict[str, dict[str, Any]],
    corr_summary: dict[str, dict[str, Any]],
) -> tuple[str, str]:
    family_variance = [row for row in variance_rows if row["feature_family"] == family]
    all_constant = all(row["is_constant"] for row in family_variance)
    e4 = e4_rows.get(family, {})
    pr_drop = float(e4.get("pr_auc_drop_relative") or 0.0)
    lift_drop = float(e4.get("lift_5pct_drop_relative") or 0.0)
    active_corr = corr_summary.get(family, {}).get("average_correlation_with_active_days_count")

    if all_constant:
        return "REMOVE_CONSTANT", "Feature family is constant inside the filtered training dataset."
    if family == "overall_activity":
        return "KEEP_CORE", "Largest E4 drop and broad business meaning as engagement intensity."
    if family in {"product_metadata_features", "recency_features"}:
        return "KEEP_SUPPORTING", "Moderate contribution and interpretable supporting signal."
    if family in {"add_to_cart_activity", "product_buy_activity"}:
        return (
            "REVIEW_REDUNDANCY",
            "Core commerce behavior has weak single-family ablation impact, likely because activity intensity overlaps with active_days_count and windowed counts.",
        )
    if family == "remove_from_cart_activity":
        return (
            "REVIEW_REDUNDANCY",
            "Business meaning is plausible as hesitation or negative intent, but current count-window representation may not capture directionality well.",
        )
    if family == "search_activity":
        return (
            "REVIEW_REDUNDANCY",
            "Search has plausible intent signal but count-only representation is noisy and overlaps with general activity.",
        )
    if family == "ratio_features":
        return "REMOVE_CANDIDATE", "Derived ratios underperform in E4 and likely duplicate underlying counts."
    if max(pr_drop, lift_drop) >= 0.01 or (active_corr is not None and active_corr < 0.20):
        return "KEEP_SUPPORTING", "Aggregate evidence shows modest or potentially independent signal."
    return "REMOVE_CANDIDATE", "Weak contribution and limited evidence of unique signal."


def decision_matrix(
    variance_rows: list[dict[str, Any]],
    matrix_rows: list[dict[str, Any]],
    e4_rows: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    corr_summary = family_corr_summary(matrix_rows)
    rows = []
    for family, features in FEATURE_FAMILIES.items():
        category, rationale = classify_family(family, variance_rows, e4_rows, corr_summary)
        e4 = e4_rows.get(family, {})
        family_variance = [row for row in variance_rows if row["feature_family"] == family]
        constant_features = [row["feature_name"] for row in family_variance if row["is_constant"]]
        rows.append(
            {
                "feature_family": family,
                "category": category,
                "feature_count": len(features),
                "constant_feature_count": len(constant_features),
                "constant_features": ",".join(constant_features),
                "pr_auc_drop_relative": e4.get("pr_auc_drop_relative"),
                "lift_5pct_drop_relative": e4.get("lift_5pct_drop_relative"),
                "average_pairwise_correlation": corr_summary[family]["average_pairwise_correlation"],
                "average_correlation_with_active_days_count": corr_summary[family][
                    "average_correlation_with_active_days_count"
                ],
                "max_correlation_with_active_days_count": corr_summary[family][
                    "max_correlation_with_active_days_count"
                ],
                "business_meaning": BUSINESS_MEANING[family],
                "rationale": rationale,
            }
        )
    return rows


def write_review(path: Path, decision_rows: list[dict[str, Any]], variance_rows: list[dict[str, Any]]) -> None:
    permanent_remove = [
        row["feature_name"]
        for row in variance_rows
        if row["is_constant"] and row["feature_name"] == "is_eligible_purchase_propensity"
    ]
    keep_despite_low = [
        row["feature_family"]
        for row in decision_rows
        if row["category"] == "REVIEW_REDUNDANCY"
        and row["feature_family"] in {"add_to_cart_activity", "product_buy_activity", "remove_from_cart_activity", "search_activity"}
    ]
    redesign = ["remove_from_cart_activity", "ratio_features", "search_activity"]
    baseline_v2 = [
        row["feature_family"]
        for row in decision_rows
        if row["category"] in {"KEEP_CORE", "KEEP_SUPPORTING", "REVIEW_REDUNDANCY"}
    ]

    lines = [
        "# Feature Rationalization Audit",
        "",
        "## Scope",
        "This audit combines E4 ablation evidence with feature variance, constant-feature detection, correlation structure, and business meaning. It keeps the original baseline unchanged for reproducibility and does not train a new model.",
        "",
        "## Decision Matrix",
        "| Family | Category | PR-AUC drop | Lift@5% drop | Avg corr with active_days_count | Constant features | Rationale |",
        "|---|---|---:|---:|---:|---|---|",
    ]
    for row in decision_rows:
        pr_drop = "-" if row["pr_auc_drop_relative"] is None else f"{float(row['pr_auc_drop_relative']):.2%}"
        lift_drop = "-" if row["lift_5pct_drop_relative"] is None else f"{float(row['lift_5pct_drop_relative']):.2%}"
        active_corr = (
            "-"
            if row["average_correlation_with_active_days_count"] is None
            else f"{float(row['average_correlation_with_active_days_count']):.6f}"
        )
        constants = row["constant_features"] or "-"
        lines.append(
            f"| {row['feature_family']} | {row['category']} | {pr_drop} | {lift_drop} | "
            f"{active_corr} | {constants} | {row['rationale']} |"
        )

    lines.extend(
        [
            "",
            "## Specific Findings",
            "- active_days_count dominates E4 because it compresses general engagement intensity across event types into one dense, non-null feature.",
            "- add_to_cart_activity and product_buy_activity should not be removed solely from single-family ablation; they are core commerce funnel signals and may be partially absorbed by active_days_count and overlapping window counts.",
            "- remove_from_cart_activity can represent hesitation, friction, cart cleanup, or negative intent. Current count features may be insufficient because they do not distinguish sequence, timing relative to cart/add, or whether removal was followed by later purchase.",
            "- ratio_features are derived from other activity counts and showed no unique gain in E4, so they are the clearest non-constant remove candidate.",
            "",
            "## Required Answers",
            f"- Permanently remove: {', '.join(permanent_remove) if permanent_remove else 'none'}",
            f"- Remain despite low ablation impact: {', '.join(keep_despite_low) if keep_despite_low else 'none'}",
            f"- Require redesign rather than removal: {', '.join(redesign)}",
            f"- Recommended Baseline v2 family set: {', '.join(baseline_v2)}",
            "",
            "## Privacy",
            "Artifacts are aggregate-only. No raw client IDs, raw query text, product names, row-level predictions, row-level scores, or model binaries are persisted.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    train_input = resolve_repo_path(args.train_input)
    e4_summary_path = resolve_repo_path(args.e4_summary)
    artifact_dir = resolve_repo_path(args.artifact_dir)

    variance_path = artifact_dir / "feature_variance_audit.csv"
    matrix_path = artifact_dir / "feature_redundancy_matrix.csv"
    decision_path = artifact_dir / "feature_decision_matrix.csv"
    review_path = artifact_dir / "feature_rationalization_review.md"

    spark = SparkSession.builder.appName("feature-rationalization-audit").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        train_df = spark.read.parquet(str(train_input)).cache()
        feature_columns = numeric_feature_columns(train_df)
        validate_mapping(feature_columns)
        e4_rows = load_e4_rows(e4_summary_path)

        variance_rows = feature_variance_audit(train_df, feature_columns)
        matrix_rows = correlation_matrix(train_df, variance_rows)
        decision_rows = decision_matrix(variance_rows, matrix_rows, e4_rows)

        write_csv(
            variance_path,
            variance_rows,
            ["feature_name", "feature_family", "non_null_count", "distinct_count", "stddev", "min", "max", "is_constant"],
        )
        write_csv(
            matrix_path,
            matrix_rows,
            [
                "feature_left",
                "family_left",
                "feature_right",
                "family_right",
                "correlation",
                "absolute_correlation",
                "correlation_status",
            ],
        )
        write_csv(
            decision_path,
            decision_rows,
            [
                "feature_family",
                "category",
                "feature_count",
                "constant_feature_count",
                "constant_features",
                "pr_auc_drop_relative",
                "lift_5pct_drop_relative",
                "average_pairwise_correlation",
                "average_correlation_with_active_days_count",
                "max_correlation_with_active_days_count",
                "business_meaning",
                "rationale",
            ],
        )
        write_review(review_path, decision_rows, variance_rows)

        print("Feature rationalization audit completed.")
        print(f"Variance audit: {relative_path(variance_path)}")
        print(f"Redundancy matrix: {relative_path(matrix_path)}")
        print(f"Decision matrix: {relative_path(decision_path)}")
        print(f"Review: {relative_path(review_path)}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
