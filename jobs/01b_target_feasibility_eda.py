"""Run aggregate-only business target selection EDA for MVP planning.

This job compares purchase propensity, cart conversion, and purchase-based
churn as possible MVP targets. It does not write client-level labels, row
samples, raw client ids, query text, or product names.
"""

from __future__ import annotations

import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_BASE_DIR = PROJECT_ROOT / "data" / "raw" / "synerise_dataset"
OUTPUT_DIR = PROJECT_ROOT / "artifacts" / "eda"
SUMMARY_PATH = OUTPUT_DIR / "target_feasibility_summary.json"
BUSINESS_SELECTION_SUMMARY_PATH = OUTPUT_DIR / "business_target_selection_summary.json"
BUSINESS_TARGET_COMPARISON_PATH = OUTPUT_DIR / "business_target_comparison.csv"
WINDOW_BALANCE_PATH = OUTPUT_DIR / "churn_window_balance.csv"
PURCHASE_FREQUENCY_PATH = OUTPUT_DIR / "purchase_frequency_summary.csv"
ACTIVE_COHORT_PATH = OUTPUT_DIR / "active_cohort_comparison.csv"
NOTES_PATH = OUTPUT_DIR / "target_feasibility_notes.md"

PRODUCT_BUY_PATH = RAW_BASE_DIR / "product_buy.parquet"
ADD_TO_CART_PATH = RAW_BASE_DIR / "add_to_cart.parquet"
PAGE_VISIT_PATH = RAW_BASE_DIR / "page_visit.parquet"
SEARCH_QUERY_PATH = RAW_BASE_DIR / "search_query.parquet"

TARGET_WINDOWS = [14, 30, 45]
LEAKAGE_RULE = (
    "Features must only use events before cutoff_date. Labels must only use purchase events from "
    "cutoff_date to target_end. No target-window behavior should be used in feature calculations."
)


def relative_path(path: Path) -> str:
    return path.relative_to(PROJECT_ROOT).as_posix()


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


def short_error_summary(context: str, exc: Exception) -> str:
    return f"{context} ({exc.__class__.__name__})"


def start_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("target-feasibility-eda")
        .master("local[*]")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_event_table(spark: SparkSession, path: Path, event_name: str) -> DataFrame:
    return (
        spark.read.parquet(str(path))
        .select(F.col("client_id"), F.to_date(F.to_timestamp("timestamp")).alias("event_date"))
        .where(F.col("client_id").isNotNull() & F.col("event_date").isNotNull())
        .withColumn("event_source", F.lit(event_name))
    )


def collect_single_row(df: DataFrame) -> dict[str, Any]:
    return {key: normalize_value(value) for key, value in df.collect()[0].asDict().items()}


def build_windows(min_date: str, max_date: str) -> list[dict[str, Any]]:
    max_date_col = F.to_date(F.lit(max_date))
    rows = []
    for target_days in TARGET_WINDOWS:
        row = (
            spark_for_dates.range(1)
            .select(
                F.to_date(F.lit(min_date)).cast("string").alias("history_start"),
                F.date_sub(max_date_col, target_days - 1).cast("string").alias("cutoff_date"),
                max_date_col.cast("string").alias("target_end"),
                F.datediff(F.date_sub(max_date_col, target_days - 1), F.to_date(F.lit(min_date))).alias(
                    "history_days"
                ),
                F.lit(target_days).alias("target_days"),
            )
            .collect()[0]
            .asDict()
        )
        rows.append({key: normalize_value(value) for key, value in row.items()})
    return rows


def purchase_frequency_summary(product_buy: DataFrame) -> dict[str, Any]:
    purchase_counts = product_buy.groupBy("client_id").agg(F.count(F.lit(1)).alias("purchase_count")).cache()
    base = collect_single_row(
        purchase_counts.agg(
            F.count(F.lit(1)).alias("total_purchasing_clients"),
            F.sum("purchase_count").alias("total_purchase_events"),
            F.avg("purchase_count").alias("average_purchases_per_purchasing_client"),
            F.sum(F.when(F.col("purchase_count") == 1, 1).otherwise(0)).alias("clients_with_exactly_1_purchase"),
            F.sum(F.when(F.col("purchase_count") >= 2, 1).otherwise(0)).alias("clients_with_2_plus_purchases"),
            F.sum(F.when(F.col("purchase_count") >= 3, 1).otherwise(0)).alias("clients_with_3_plus_purchases"),
            F.sum(F.when(F.col("purchase_count") >= 5, 1).otherwise(0)).alias("clients_with_5_plus_purchases"),
        )
    )
    quantiles = purchase_counts.approxQuantile("purchase_count", [0.25, 0.5, 0.75, 0.9, 0.95, 0.99], 0.01)
    total = base["total_purchasing_clients"] or 0
    for key in [
        "clients_with_exactly_1_purchase",
        "clients_with_2_plus_purchases",
        "clients_with_3_plus_purchases",
        "clients_with_5_plus_purchases",
    ]:
        base[f"{key}_pct"] = round((base[key] / total) * 100, 4) if total else None
    base.update(
        {
            "purchase_count_q25": normalize_value(quantiles[0]) if len(quantiles) == 6 else None,
            "purchase_count_median": normalize_value(quantiles[1]) if len(quantiles) == 6 else None,
            "purchase_count_q75": normalize_value(quantiles[2]) if len(quantiles) == 6 else None,
            "purchase_count_q90": normalize_value(quantiles[3]) if len(quantiles) == 6 else None,
            "purchase_count_q95": normalize_value(quantiles[4]) if len(quantiles) == 6 else None,
            "purchase_count_q99": normalize_value(quantiles[5]) if len(quantiles) == 6 else None,
        }
    )
    purchase_counts.unpersist()
    return base


def clients_in_window(df: DataFrame, start_date: str, end_date: str, include_start: bool, include_end: bool) -> DataFrame:
    start_op = F.col("event_date") >= F.to_date(F.lit(start_date)) if include_start else F.col("event_date") < F.to_date(F.lit(start_date))
    end_op = F.col("event_date") <= F.to_date(F.lit(end_date)) if include_end else F.col("event_date") < F.to_date(F.lit(end_date))
    return df.where(start_op & end_op).select("client_id").distinct()


def history_clients(df: DataFrame, cutoff_date: str) -> DataFrame:
    return df.where(F.col("event_date") < F.to_date(F.lit(cutoff_date))).select("client_id").distinct()


def target_purchase_clients(product_buy: DataFrame, cutoff_date: str, target_end: str) -> DataFrame:
    return (
        product_buy.where(
            (F.col("event_date") >= F.to_date(F.lit(cutoff_date)))
            & (F.col("event_date") <= F.to_date(F.lit(target_end)))
        )
        .select("client_id")
        .distinct()
    )


def label_balance_for_window(product_buy: DataFrame, window: dict[str, Any]) -> dict[str, Any]:
    eligible = history_clients(product_buy, window["cutoff_date"]).cache()
    target = target_purchase_clients(product_buy, window["cutoff_date"], window["target_end"]).cache()
    eligible_clients = eligible.count()
    non_churn_clients = eligible.join(target, "client_id", "inner").count()
    churn_clients = eligible_clients - non_churn_clients
    eligible.unpersist()
    target.unpersist()
    return {
        **window,
        "eligible_clients": eligible_clients,
        "churn_clients": churn_clients,
        "non_churn_clients": non_churn_clients,
        "churn_rate": round(churn_clients / eligible_clients, 6) if eligible_clients else None,
        "non_churn_rate": round(non_churn_clients / eligible_clients, 6) if eligible_clients else None,
        "leakage_rule": LEAKAGE_RULE,
    }


def target_candidate_rows(
    product_buy: DataFrame,
    add_to_cart: DataFrame,
    windows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows = []
    for window in windows:
        target = target_purchase_clients(product_buy, window["cutoff_date"], window["target_end"]).cache()
        purchase_history = history_clients(product_buy, window["cutoff_date"]).cache()
        cart_history = history_clients(add_to_cart, window["cutoff_date"]).cache()
        active_history = purchase_history.union(cart_history).distinct().cache()

        candidates = [
            {
                "target_name": "purchase_propensity",
                "business_question": "Which active clients are likely to purchase in the target window?",
                "eligible_df": active_history,
                "positive_means": "purchase in target window",
                "negative_means": "no purchase in target window",
                "data_coverage_note": "Default active cohort uses add_to_cart or purchase history.",
                "implementation_complexity": "medium",
                "complexity_reason": (
                    "Requires active-user cohort and future purchase label, but is broad and business-friendly."
                ),
                "business_value": "high",
                "business_value_reason": "Supports campaign targeting and general purchase likelihood scoring.",
            },
            {
                "target_name": "cart_conversion",
                "business_question": "Among clients who showed cart intent, who will convert to purchase in the target window?",
                "eligible_df": cart_history,
                "positive_means": "cart conversion purchase in target window",
                "negative_means": "possible cart abandonment or no conversion",
                "data_coverage_note": "Cohort uses clients with add_to_cart history.",
                "implementation_complexity": "low_to_medium",
                "complexity_reason": "Clear cohort from add_to_cart and clear target purchase label.",
                "business_value": "high",
                "business_value_reason": "Directly supports cart recovery and conversion campaigns.",
            },
            {
                "target_name": "purchase_based_churn",
                "business_question": "Among clients who have purchased before, who will not purchase again in the target window?",
                "eligible_df": purchase_history,
                "positive_means": "churn: no purchase in target window",
                "negative_means": "non-churn: purchase in target window",
                "data_coverage_note": "Cohort uses clients with purchase history.",
                "implementation_complexity": "low",
                "complexity_reason": (
                    "Clear purchase-only cohort, but business interpretation is narrower and label may be highly imbalanced."
                ),
                "business_value": "medium",
                "business_value_reason": "Useful for retention, but only applies to previous purchasers.",
            },
        ]

        for candidate in candidates:
            eligible_df = candidate["eligible_df"]
            eligible_clients = eligible_df.count()
            target_purchase_count = eligible_df.join(target, "client_id", "inner").count()
            if candidate["target_name"] == "purchase_based_churn":
                positive_clients = eligible_clients - target_purchase_count
                negative_clients = target_purchase_count
            else:
                positive_clients = target_purchase_count
                negative_clients = eligible_clients - target_purchase_count

            positive_rate = round(positive_clients / eligible_clients, 6) if eligible_clients else None
            negative_rate = round(negative_clients / eligible_clients, 6) if eligible_clients else None
            rows.append(
                {
                    "target_name": candidate["target_name"],
                    "business_question": candidate["business_question"],
                    "window_days": window["target_days"],
                    "history_start": window["history_start"],
                    "cutoff_date": window["cutoff_date"],
                    "target_end": window["target_end"],
                    "eligible_clients": eligible_clients,
                    "positive_clients": positive_clients,
                    "negative_clients": negative_clients,
                    "positive_rate": positive_rate,
                    "negative_rate": negative_rate,
                    "positive_means": candidate["positive_means"],
                    "negative_means": candidate["negative_means"],
                    "data_coverage_note": candidate["data_coverage_note"],
                    "label_imbalance_note": imbalance_note(positive_rate),
                    "implementation_complexity": candidate["implementation_complexity"],
                    "complexity_reason": candidate["complexity_reason"],
                    "business_value": candidate["business_value"],
                    "business_value_reason": candidate["business_value_reason"],
                    "leakage_rule": LEAKAGE_RULE,
                    "recommended_for_mvp": False,
                    "reason": None,
                }
            )

        target.unpersist()
        purchase_history.unpersist()
        cart_history.unpersist()
        active_history.unpersist()

    apply_recommendation_ranks(rows)
    return rows


def imbalance_note(positive_rate: float | None) -> str:
    if positive_rate is None:
        return "not_available"
    if 0.10 <= positive_rate <= 0.40:
        return "moderate_for_baseline"
    if positive_rate < 0.10:
        return "positive_class_low"
    return "positive_class_high"


def rank_desc(rows: list[dict[str, Any]], key: str, output_key: str) -> None:
    values = sorted({row[key] for row in rows if row.get(key) is not None}, reverse=True)
    ranks = {value: index + 1 for index, value in enumerate(values)}
    for row in rows:
        row[output_key] = ranks.get(row.get(key))


def rank_asc_by_score(rows: list[dict[str, Any]], score_fn: Any, output_key: str) -> None:
    scored = sorted({score_fn(row) for row in rows if score_fn(row) is not None})
    ranks = {value: index + 1 for index, value in enumerate(scored)}
    for row in rows:
        score = score_fn(row)
        row[output_key] = ranks.get(score)


def apply_recommendation_ranks(rows: list[dict[str, Any]]) -> None:
    complexity_scores = {"low": 1, "low_to_medium": 2, "medium": 3, "high": 4}
    business_scores = {"high": 1, "medium": 2, "low": 3}

    rank_desc(rows, "eligible_clients", "coverage_rank")
    rank_asc_by_score(
        rows,
        lambda row: abs((row["positive_rate"] or 0) - 0.25) if row.get("positive_rate") is not None else None,
        "balance_rank",
    )
    for row in rows:
        row["complexity_rank"] = complexity_scores.get(row["implementation_complexity"])
        row["business_value_rank"] = business_scores.get(row["business_value"])

    for row in rows:
        row["overall_recommendation_score"] = (
            (row["coverage_rank"] or 99)
            + (row["balance_rank"] or 99)
            + (row["complexity_rank"] or 99)
            + (row["business_value_rank"] or 99)
        )

    best = min(rows, key=lambda row: row["overall_recommendation_score"])
    best["recommended_for_mvp"] = True
    best["reason"] = (
        "Best overall trade-off across coverage, label balance, implementation complexity, and business actionability."
    )
    sorted_scores = sorted({row["overall_recommendation_score"] for row in rows})
    score_ranks = {score: index + 1 for index, score in enumerate(sorted_scores)}
    for row in rows:
        row["overall_recommendation_rank"] = score_ranks[row["overall_recommendation_score"]]
        if row["reason"] is None:
            row["reason"] = "Alternative target/window retained for review."


def active_cohort_rows(
    product_buy: DataFrame,
    add_to_cart: DataFrame,
    windows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows = []
    for window in windows:
        target = target_purchase_clients(product_buy, window["cutoff_date"], window["target_end"]).cache()
        option_a = history_clients(product_buy, window["cutoff_date"]).cache()
        option_b = (
            history_clients(product_buy, window["cutoff_date"])
            .union(history_clients(add_to_cart, window["cutoff_date"]))
            .distinct()
            .cache()
        )

        for option_name, description, cohort in [
            ("A", "at least 1 purchase in history window", option_a),
            ("B", "at least 1 add_to_cart or purchase event in history window", option_b),
        ]:
            eligible_clients = cohort.count()
            clients_with_purchase = cohort.join(target, "client_id", "inner").count()
            rows.append(
                {
                    "target_days": window["target_days"],
                    "cutoff_date": window["cutoff_date"],
                    "target_end": window["target_end"],
                    "cohort_option": option_name,
                    "cohort_definition": description,
                    "eligible_clients": eligible_clients,
                    "clients_with_purchase_in_target": clients_with_purchase,
                    "target_positive_rate": round(clients_with_purchase / eligible_clients, 6)
                    if eligible_clients
                    else None,
                    "status": "computed",
                    "notes": None,
                }
            )
            cohort.unpersist()

        rows.append(
            {
                "target_days": window["target_days"],
                "cutoff_date": window["cutoff_date"],
                "target_end": window["target_end"],
                "cohort_option": "C",
                "cohort_definition": "at least 1 page_visit, search_query, add_to_cart, or product_buy event in history window",
                "eligible_clients": None,
                "clients_with_purchase_in_target": None,
                "target_positive_rate": None,
                "status": "skipped",
                "notes": "Skipped by default because page_visit is the largest table; compute as targeted follow-up if needed.",
            }
        )
        target.unpersist()
    return rows


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})


def write_notes(path: Path, recommendation: str) -> None:
    path.write_text(
        "\n".join(
            [
                "# Business Target Selection Notes",
                "",
                "This artifact contains aggregate-only EDA notes.",
                "",
                f"Recommendation: {recommendation}",
                "",
                f"Leakage rule: {LEAKAGE_RULE}",
            ]
        ),
        encoding="utf-8",
    )


def recommend_window(balance_rows: list[dict[str, Any]]) -> dict[str, Any]:
    # Prefer a middle target window if its non-churn rate is not extremely small.
    by_days = {row["target_days"]: row for row in balance_rows}
    if 30 in by_days and by_days[30]["non_churn_rate"] is not None and by_days[30]["non_churn_rate"] >= 0.03:
        return by_days[30]
    return max(balance_rows, key=lambda row: row["non_churn_rate"] or 0)


def recommended_business_target(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return next(row for row in rows if row["recommended_for_mvp"])


def main() -> int:
    print("Business target selection EDA job")
    print("Primary input: data/raw/synerise_dataset/product_buy.parquet")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    spark = start_spark()
    global spark_for_dates
    spark_for_dates = spark

    try:
        product_buy = read_event_table(spark, PRODUCT_BUY_PATH, "product_buy").cache()
        add_to_cart = read_event_table(spark, ADD_TO_CART_PATH, "add_to_cart").cache()

        time_range = collect_single_row(
            product_buy.agg(
                F.min("event_date").cast("string").alias("min_date"),
                F.max("event_date").cast("string").alias("max_date"),
                F.count(F.lit(1)).alias("total_purchase_events"),
            )
        )
        span_row = collect_single_row(
            spark.range(1).select(
                F.datediff(F.to_date(F.lit(time_range["max_date"])), F.to_date(F.lit(time_range["min_date"]))).alias(
                    "date_span_days"
                )
            )
        )
        time_range.update(span_row)

        windows = build_windows(time_range["min_date"], time_range["max_date"])
        frequency = purchase_frequency_summary(product_buy)
        balance_rows = [label_balance_for_window(product_buy, window) for window in windows]
        cohort_rows = active_cohort_rows(product_buy, add_to_cart, windows)
        target_rows = target_candidate_rows(product_buy, add_to_cart, windows)
        recommended_churn = recommend_window(balance_rows)
        recommended_target = recommended_business_target(target_rows)
        recommendation = (
            f"{recommended_target['target_name']} is recommended as the provisional MVP target using a "
            f"{recommended_target['window_days']}-day target window, pending preprocessing validation."
        )

        summary = {
            "generated_at_date": datetime.now(timezone.utc).date().isoformat(),
            "phase": "Phase 1.1: Business Target Selection EDA",
            "inputs": {
                "primary": relative_path(PRODUCT_BUY_PATH),
                "optional_used": [relative_path(ADD_TO_CART_PATH)],
                "optional_skipped_by_default": [relative_path(PAGE_VISIT_PATH), relative_path(SEARCH_QUERY_PATH)],
            },
            "notes": [
                "Artifacts contain aggregate-only feasibility summaries.",
                "No client-level labels, raw client ids, row samples, query text, or product names are persisted.",
                "This is not final training label generation.",
            ],
            "dataset_time_range": time_range,
            "candidate_windows": windows,
            "purchase_frequency": frequency,
            "business_target_comparison": target_rows,
            "candidate_churn_label_balance": balance_rows,
            "active_cohort_comparison": cohort_rows,
            "recommendation": {
                "status": "provisional_mvp_target_selected",
                "recommended_target_name": recommended_target["target_name"],
                "recommended_target_days": recommended_target["window_days"],
                "provisional_label_definition": provisional_label_definition(recommended_target["target_name"]),
                "reason": recommendation,
            },
            "leakage_validation": [
                "Features must only use events before cutoff_date.",
                "Labels must only use purchase events from cutoff_date to target_end.",
                "No target-window behavior should be used in feature calculations.",
            ],
        }

        business_summary = {
            "generated_at_date": datetime.now(timezone.utc).date().isoformat(),
            "phase": "Phase 1.1: Business Target Selection EDA",
            "notes": [
                "Artifacts contain aggregate-only business target comparison summaries.",
                "No final labels, client-level outputs, raw client ids, row samples, query text, or product names are persisted.",
            ],
            "candidate_targets": [
                "purchase_propensity",
                "cart_conversion",
                "purchase_based_churn",
            ],
            "candidate_windows": windows,
            "business_target_comparison": target_rows,
            "recommendation": summary["recommendation"],
            "ranking_heuristics": [
                "Higher eligible_clients improves coverage rank.",
                "Positive rate closer to 25% improves balance rank; 10% to 40% is considered easier for baseline modeling.",
                "Lower implementation complexity improves complexity rank.",
                "Higher direct business actionability improves business value rank.",
            ],
            "leakage_validation": summary["leakage_validation"],
        }

        write_json(SUMMARY_PATH, summary)
        write_json(BUSINESS_SELECTION_SUMMARY_PATH, business_summary)
        write_csv(BUSINESS_TARGET_COMPARISON_PATH, target_rows, list(target_rows[0].keys()))
        write_csv(PURCHASE_FREQUENCY_PATH, [frequency], list(frequency.keys()))
        write_csv(WINDOW_BALANCE_PATH, balance_rows, list(balance_rows[0].keys()))
        write_csv(ACTIVE_COHORT_PATH, cohort_rows, list(cohort_rows[0].keys()))
        write_notes(NOTES_PATH, recommendation)

        print("Wrote artifacts:")
        print("  artifacts/eda/business_target_selection_summary.json")
        print("  artifacts/eda/business_target_comparison.csv")
        print("  artifacts/eda/target_feasibility_summary.json")
        print("  artifacts/eda/churn_window_balance.csv")
        print("  artifacts/eda/purchase_frequency_summary.csv")
        print("  artifacts/eda/active_cohort_comparison.csv")
        print("  artifacts/eda/target_feasibility_notes.md")
    except Exception as exc:  # noqa: BLE001
        print(short_error_summary("Target feasibility EDA failed.", exc))
        return 1
    finally:
        spark.stop()

    return 0


def provisional_label_definition(target_name: str) -> str:
    if target_name == "purchase_propensity":
        return (
            "Eligible clients have at least 1 add_to_cart or purchase event before cutoff_date; positive clients "
            "have at least 1 purchase from cutoff_date to target_end; negative clients have no purchase in that target window."
        )
    if target_name == "cart_conversion":
        return (
            "Eligible clients have at least 1 add_to_cart event before cutoff_date; positive clients have at least "
            "1 purchase from cutoff_date to target_end; negative clients have no purchase in that target window."
        )
    return (
        "Eligible clients have at least 1 purchase before cutoff_date; positive clients are churn clients with no "
        "purchase from cutoff_date to target_end; negative clients have at least 1 purchase in that target window."
    )


if __name__ == "__main__":
    sys.exit(main())
