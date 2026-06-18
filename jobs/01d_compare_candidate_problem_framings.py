"""Compare candidate problem framings with aggregate-only EDA.

This job supports a mentor-facing decision about whether the current target
should be kept, reframed, broadened, or replaced. It reads processed event
tables already available for the MVP and writes aggregate-only artifacts under
artifacts/problem_framing/.

It does not train models, rerun scoring, change API behavior, output raw client
IDs, output raw query text, output product names, or write row-level samples.
"""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.dataset as ds


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "pipeline_config.yaml"
DEFAULT_PROCESSED_BASE = PROJECT_ROOT / "data" / "processed"
EDA_EVENT_OVERVIEW = PROJECT_ROOT / "artifacts" / "eda" / "event_table_overview.csv"
ARTIFACT_DIR = PROJECT_ROOT / "artifacts" / "problem_framing"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare candidate problem framings with aggregate-only EDA.")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH.relative_to(PROJECT_ROOT).as_posix())
    parser.add_argument("--processed-base", default=DEFAULT_PROCESSED_BASE.relative_to(PROJECT_ROOT).as_posix())
    return parser.parse_args()


def resolve_repo_path(path_text: str) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        raise ValueError("path arguments must be repo-relative")
    return PROJECT_ROOT / path


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


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: normalize_value(row.get(key)) for key in fieldnames})


def read_parquet(path: Path, columns: list[str]) -> pd.DataFrame:
    return ds.dataset(str(path), format="parquet").to_table(columns=columns).to_pandas()


def read_event_clients(processed_base: Path, table_name: str, cutoff: pd.Timestamp) -> set[int]:
    frame = read_parquet(processed_base / "events" / table_name, ["client_id", "event_ts"])
    frame["event_ts"] = pd.to_datetime(frame["event_ts"], errors="coerce")
    return set(frame.loc[frame["event_ts"] < cutoff, "client_id"].dropna().astype("int64").unique())


def read_target_buyers(processed_base: Path, cutoff: pd.Timestamp, target_end: pd.Timestamp) -> set[int]:
    frame = read_parquet(processed_base / "events" / "product_buy", ["client_id", "event_ts"])
    frame["event_ts"] = pd.to_datetime(frame["event_ts"], errors="coerce")
    target = frame[(frame["event_ts"] >= cutoff) & (frame["event_ts"] < target_end + pd.Timedelta(days=1))]
    return set(target["client_id"].dropna().astype("int64").unique())


def rate(numerator: int | float, denominator: int | float) -> float | None:
    if not denominator:
        return None
    return float(numerator) / float(denominator)


def eda_value(table_name: str, column: str) -> str | None:
    if not EDA_EVENT_OVERVIEW.exists():
        return None
    with EDA_EVENT_OVERVIEW.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row.get("table_name") == table_name:
                return row.get(column)
    return None


def build_candidates(cart: set[int], buy: set[int], search: set[int]) -> dict[str, dict[str, Any]]:
    current = cart | buy
    return {
        "current_prior_cart_or_purchase": {
            "display_name": "Option A: 30-day purchase prediction for prior cart/purchase users",
            "cohort": current,
            "cohort_definition": "pre-cutoff add_to_cart OR product_buy",
            "business_action": "Rank prior cart/purchase users for purchase follow-up.",
            "recommendation_status": "keep_for_mvp_rename_reframe",
        },
        "cart_only": {
            "display_name": "Option B: 30-day cart conversion prediction",
            "cohort": cart,
            "cohort_definition": "pre-cutoff add_to_cart",
            "business_action": "Target cart-intent users likely to convert.",
            "recommendation_status": "future_extension_or_variant",
        },
        "search_cart_buy": {
            "display_name": "Option C: 30-day purchase propensity for search/cart/buy active users",
            "cohort": search | cart | buy,
            "cohort_definition": "pre-cutoff search_query OR add_to_cart OR product_buy",
            "business_action": "Broader purchase propensity among behavior-active users.",
            "recommendation_status": "broaden_in_next_iteration",
        },
        "buy_only": {
            "display_name": "Option E: 30-day repeat purchase prediction",
            "cohort": buy,
            "cohort_definition": "pre-cutoff product_buy",
            "business_action": "Retain or re-engage previous buyers.",
            "recommendation_status": "future_extension",
        },
        "search_only": {
            "display_name": "Search-only diagnostic cohort",
            "cohort": search - current,
            "cohort_definition": "pre-cutoff search_query AND NOT add_to_cart/product_buy",
            "business_action": "Estimate whether search-only users can broaden purchase propensity.",
            "recommendation_status": "diagnostic_not_standalone_target",
        },
    }


def candidate_tables(candidates: dict[str, dict[str, Any]], target_buyers: set[int]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    current = candidates["current_prior_cart_or_purchase"]["cohort"]
    cart = candidates["cart_only"]["cohort"]
    cohort_rows: list[dict[str, Any]] = []
    balance_rows: list[dict[str, Any]] = []
    for name, candidate in candidates.items():
        cohort = candidate["cohort"]
        positives = len(cohort & target_buyers)
        cohort_rows.append(
            {
                "candidate": name,
                "display_name": candidate["display_name"],
                "cohort_definition": candidate["cohort_definition"],
                "cohort_client_count": len(cohort),
                "overlap_with_current_count": len(cohort & current),
                "overlap_with_current_rate": rate(len(cohort & current), len(cohort)),
                "extra_clients_vs_current": len(cohort - current),
                "extra_clients_vs_cart_only": len(cohort - cart),
                "can_build_from_processed_data": True,
                "requires_page_visit": False,
                "business_action": candidate["business_action"],
                "recommendation_status": candidate["recommendation_status"],
            }
        )
        balance_rows.append(
            {
                "candidate": name,
                "cohort_client_count": len(cohort),
                "positive_client_count": positives,
                "negative_client_count": len(cohort) - positives,
                "positive_rate": rate(positives, len(cohort)),
                "positive_label_source": "product_buy events in the 30-day target window",
                "class_imbalance_risk": "high" if rate(positives, len(cohort)) is not None and rate(positives, len(cohort)) < 0.05 else "moderate",
                "api_serving_fit": "good_for_batch_lookup_if_scored_offline",
            }
        )

    page_clients = int(eda_value("page_visit", "distinct_client_id_count") or 0)
    cohort_rows.append(
        {
            "candidate": "all_active_with_page_visit",
            "display_name": "Option D: 30-day purchase propensity for all observed active users",
            "cohort_definition": "pre-cutoff page_visit OR search_query OR add_to_cart OR product_buy",
            "cohort_client_count": page_clients,
            "overlap_with_current_count": None,
            "overlap_with_current_rate": None,
            "extra_clients_vs_current": None,
            "extra_clients_vs_cart_only": None,
            "can_build_from_processed_data": False,
            "requires_page_visit": True,
            "business_action": "Broadest active-user purchase propensity.",
            "recommendation_status": "future_extension_requires_page_visit_eda",
        }
    )
    balance_rows.append(
        {
            "candidate": "all_active_with_page_visit",
            "cohort_client_count": page_clients,
            "positive_client_count": None,
            "negative_client_count": None,
            "positive_rate": None,
            "positive_label_source": "would use product_buy events after page_visit cohort processing",
            "class_imbalance_risk": "unknown_until_page_visit_cohort_built",
            "api_serving_fit": "future_fit_after_offline_scoring",
        }
    )
    return cohort_rows, balance_rows


def overlap_rows(candidates: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for left_name, left in candidates.items():
        for right_name, right in candidates.items():
            left_set = left["cohort"]
            right_set = right["cohort"]
            intersection = len(left_set & right_set)
            union = len(left_set | right_set)
            rows.append(
                {
                    "left_candidate": left_name,
                    "right_candidate": right_name,
                    "left_count": len(left_set),
                    "right_count": len(right_set),
                    "intersection_count": intersection,
                    "intersection_over_left_rate": rate(intersection, len(left_set)),
                    "jaccard_rate": rate(intersection, union),
                }
            )
    return rows


def feature_rows() -> list[dict[str, Any]]:
    return [
        {"option": "Option A current reframed", "available_inputs": "add_to_cart, product_buy, remove_from_cart, search_query, product metadata", "missing_inputs": "page_visit excluded", "current_pipeline_reuse": "high", "feature_implication": "Existing count, recency, ratio, and metadata features are usable."},
        {"option": "Option B cart conversion", "available_inputs": "add_to_cart, product_buy, remove_from_cart, product metadata", "missing_inputs": "none for basic cart conversion", "current_pipeline_reuse": "high", "feature_implication": "Existing features can be reused with a narrower cart-intent framing."},
        {"option": "Option C search/cart/buy", "available_inputs": "search_query, add_to_cart, product_buy, remove_from_cart, product metadata", "missing_inputs": "query text intentionally excluded", "current_pipeline_reuse": "medium_high", "feature_implication": "Requires cohort/label rebuild; search signal should remain aggregate/simple."},
        {"option": "Option D all observed active users", "available_inputs": "raw page_visit EDA only; processed page_visit deferred", "missing_inputs": "processed page_visit features", "current_pipeline_reuse": "medium", "feature_implication": "Requires page_visit processing and feature review before modeling."},
        {"option": "Option E repeat purchase", "available_inputs": "product_buy, product metadata", "missing_inputs": "none for basic repeat purchase", "current_pipeline_reuse": "medium_high", "feature_implication": "Narrower retention target; purchase recency/count features are central."},
        {"option": "Option F next product/category ranking", "available_inputs": "product_buy, add_to_cart, product metadata", "missing_inputs": "ranking labels, candidate generation, ranking evaluation", "current_pipeline_reuse": "low_medium", "feature_implication": "Requires a different recommendation/ranking pipeline."},
    ]


def cost_rows() -> list[dict[str, Any]]:
    return [
        {"candidate": "current_prior_cart_or_purchase", "required_large_tables": "add_to_cart, product_buy", "known_row_counts": f"add_to_cart={eda_value('add_to_cart','row_count')}; product_buy={eda_value('product_buy','row_count')}", "processing_cost": "already_processed", "cost_benefit_note": "Best MVP fit because the processed feature/label path already exists."},
        {"candidate": "cart_only", "required_large_tables": "add_to_cart, product_buy", "known_row_counts": f"add_to_cart={eda_value('add_to_cart','row_count')}; product_buy={eda_value('product_buy','row_count')}", "processing_cost": "low_incremental", "cost_benefit_note": "Clear target, but narrower and would require label/cohort rebuild."},
        {"candidate": "search_cart_buy", "required_large_tables": "search_query, add_to_cart, product_buy", "known_row_counts": f"search_query={eda_value('search_query','row_count')}; add_to_cart={eda_value('add_to_cart','row_count')}; product_buy={eda_value('product_buy','row_count')}", "processing_cost": "moderate_incremental", "cost_benefit_note": "Useful next iteration because search-only users add coverage but have low positive rate."},
        {"candidate": "all_active_with_page_visit", "required_large_tables": "page_visit plus all current event tables", "known_row_counts": f"page_visit={eda_value('page_visit','row_count')}; distinct_clients={eda_value('page_visit','distinct_client_id_count')}", "processing_cost": "high_future_extension", "cost_benefit_note": "Page visit has broad coverage but weaker intent and high volume; defer until targeted page-visit EDA."},
        {"candidate": "buy_only_repeat_purchase", "required_large_tables": "product_buy", "known_row_counts": f"product_buy={eda_value('product_buy','row_count')}", "processing_cost": "low_incremental", "cost_benefit_note": "Good retention extension but too narrow for the current general scoring MVP."},
    ]


def write_recommendation(balance_rows: list[dict[str, Any]], cohort_rows: list[dict[str, Any]]) -> None:
    balance = {row["candidate"]: row for row in balance_rows}
    cohorts = {row["candidate"]: row for row in cohort_rows}
    lines = [
        "# Problem Framing Recommendation",
        "",
        f"Generated at date: {datetime.now(timezone.utc).date().isoformat()}",
        "",
        "This artifact summarizes aggregate-only evidence for choosing the MVP problem framing. It does not include raw client IDs, query text, product names, row-level samples, or predictions.",
        "",
        "## Candidate Balance",
        "",
        "| Candidate | Cohort clients | Positive clients | Positive rate |",
        "| --- | ---: | ---: | ---: |",
    ]
    for key in ["current_prior_cart_or_purchase", "cart_only", "search_cart_buy", "search_only", "buy_only", "all_active_with_page_visit"]:
        row = balance[key]
        lines.append(f"| {key} | {row['cohort_client_count']} | {normalize_value(row['positive_client_count'])} | {normalize_value(row['positive_rate'])} |")
    lines.extend(
        [
            "",
            "## Recommendation",
            "",
            "For the current MVP, keep the current target but rename/reframe it as `30-day purchase prediction for prior cart/purchase users`. This is the most defensible option because it reuses the completed leakage-safe pipeline, has a clear eligibility definition, is meaningfully broader than cart conversion, and avoids introducing a new cohort/modeling cycle before mentor review.",
            "",
            f"The search/cart/buy option adds {cohorts['search_cart_buy']['extra_clients_vs_current']} clients beyond the current cohort, but its positive rate is {normalize_value(balance['search_cart_buy']['positive_rate'])}; this is better treated as the next iteration.",
            f"The search-only diagnostic cohort has {balance['search_only']['cohort_client_count']} clients and positive rate {normalize_value(balance['search_only']['positive_rate'])}, so broadening with search would add coverage but also more imbalance/noise.",
            "",
            "Cart conversion, repeat purchase, all-active page-visit propensity, and next-product/category ranking are valid future problem definitions, but they should not replace the current MVP now.",
            "",
        ]
    )
    (ARTIFACT_DIR / "problem_framing_recommendation.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    args = parse_args()
    config = read_simple_yaml(resolve_repo_path(args.config))
    processed_base = resolve_repo_path(args.processed_base)
    cutoff = pd.Timestamp(config["target"]["cutoff_date"])
    target_end = pd.Timestamp(config["target"]["target_end"])

    cart = read_event_clients(processed_base, "add_to_cart", cutoff)
    buy = read_event_clients(processed_base, "product_buy", cutoff)
    search = read_event_clients(processed_base, "search_query", cutoff)
    target_buyers = read_target_buyers(processed_base, cutoff, target_end)

    candidates = build_candidates(cart, buy, search)
    cohort_rows, balance_rows = candidate_tables(candidates, target_buyers)
    overlap = overlap_rows(candidates)
    features = feature_rows()
    costs = cost_rows()

    write_csv(ARTIFACT_DIR / "candidate_cohort_comparison.csv", cohort_rows, ["candidate", "display_name", "cohort_definition", "cohort_client_count", "overlap_with_current_count", "overlap_with_current_rate", "extra_clients_vs_current", "extra_clients_vs_cart_only", "can_build_from_processed_data", "requires_page_visit", "business_action", "recommendation_status"])
    write_csv(ARTIFACT_DIR / "candidate_target_balance.csv", balance_rows, ["candidate", "cohort_client_count", "positive_client_count", "negative_client_count", "positive_rate", "positive_label_source", "class_imbalance_risk", "api_serving_fit"])
    write_csv(ARTIFACT_DIR / "candidate_overlap_matrix.csv", overlap, ["left_candidate", "right_candidate", "left_count", "right_count", "intersection_count", "intersection_over_left_rate", "jaccard_rate"])
    write_csv(ARTIFACT_DIR / "candidate_feature_availability.csv", features, ["option", "available_inputs", "missing_inputs", "current_pipeline_reuse", "feature_implication"])
    write_csv(ARTIFACT_DIR / "candidate_processing_cost_estimate.csv", costs, ["candidate", "required_large_tables", "known_row_counts", "processing_cost", "cost_benefit_note"])
    write_recommendation(balance_rows, cohort_rows)

    print("Candidate problem framing comparison completed.")
    print(f"Wrote artifacts under {relative_path(ARTIFACT_DIR)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
