"""Run E6.1 pruning experiments for trend/velocity features.

This job keeps the frozen Baseline V2-2 feature set unchanged and compares
minimal subsets of the already-defined E6 velocity features. It does not add
new features, change labels, change temporal splits, change the model class, or
persist row-level predictions.
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
E6_JOB_PATH = PROJECT_ROOT / "experiments" / "05l_train_e6_velocity.py"
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "e6_velocity_features.json"
DEFAULT_TRAIN_INPUT = (
    PROJECT_ROOT / "data" / "processed" / "training" / "e1_train_2022_10_10" / "purchase_propensity_30d"
)
DEFAULT_VALIDATION_INPUT = (
    PROJECT_ROOT / "data" / "processed" / "training" / "e1_valid_2022_11_09" / "purchase_propensity_30d"
)
DEFAULT_EVENTS_BASE = PROJECT_ROOT / "data" / "processed" / "events"
DEFAULT_ARTIFACT_DIR = PROJECT_ROOT / "artifacts" / "modeling" / "e6_trend_velocity"
DEFAULT_MAX_ITER = 20
PR_AUC_CONTRIBUTION_RULE = 0.002


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
    parser = argparse.ArgumentParser(description="Run E6.1 trend/velocity feature pruning.")
    parser.add_argument("--feature-config", default=DEFAULT_CONFIG_PATH.relative_to(PROJECT_ROOT).as_posix())
    parser.add_argument("--train-input", default=DEFAULT_TRAIN_INPUT.relative_to(PROJECT_ROOT).as_posix())
    parser.add_argument("--validation-input", default=DEFAULT_VALIDATION_INPUT.relative_to(PROJECT_ROOT).as_posix())
    parser.add_argument("--events-base", default=DEFAULT_EVENTS_BASE.relative_to(PROJECT_ROOT).as_posix())
    parser.add_argument("--artifact-dir", default=DEFAULT_ARTIFACT_DIR.relative_to(PROJECT_ROOT).as_posix())
    parser.add_argument("--max-iter", type=int, default=DEFAULT_MAX_ITER)
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


def metric_delta(candidate: float, baseline: float) -> float:
    return E6.safe_divide(candidate - baseline, baseline)


def build_experiments(feature_groups: dict[str, list[str]]) -> list[dict[str, Any]]:
    cart_features = feature_groups["cart_velocity"]
    buy_features = feature_groups["buy_velocity"]
    search_features = feature_groups["search_velocity"]
    activity_features = feature_groups["activity_acceleration"]
    return [
        {
            "experiment_id": "full_e6",
            "description": "V2-2 plus all E6 velocity features",
            "included_e6_features": cart_features + buy_features + search_features + activity_features,
        },
        {
            "experiment_id": "without_activity_intensity_ratio",
            "description": "Full E6 without activity_intensity_ratio",
            "included_e6_features": cart_features + buy_features + search_features,
        },
        {
            "experiment_id": "without_search_velocity",
            "description": "Full E6 without search velocity",
            "included_e6_features": cart_features + buy_features + activity_features,
        },
        {
            "experiment_id": "buy_plus_cart_velocity_only",
            "description": "V2-2 plus buy and cart velocity features only",
            "included_e6_features": buy_features + cart_features,
        },
        {
            "experiment_id": "buy_velocity_only",
            "description": "V2-2 plus buy velocity features only",
            "included_e6_features": buy_features,
        },
    ]


def decision_for_row(row: dict[str, Any], full_row: dict[str, Any], best_lift5: float) -> str:
    if row["experiment_id"] == "full_e6":
        return "REFERENCE"
    pr_loss_vs_full = metric_delta(full_row["pr_auc"], row["pr_auc"])
    lift5_best_gap = metric_delta(row["lift_at_5pct"], best_lift5)
    if pr_loss_vs_full <= PR_AUC_CONTRIBUTION_RULE and lift5_best_gap >= -PR_AUC_CONTRIBUTION_RULE:
        return "KEEP_AS_CANDIDATE"
    if row["lift_at_5pct"] >= full_row["lift_at_5pct"]:
        return "KEEP_AS_CANDIDATE"
    return "DROP_FROM_MINIMAL_SET"


def best_recommendation(rows: list[dict[str, Any]]) -> dict[str, Any]:
    best_lift5 = max(row["lift_at_5pct"] for row in rows)
    full_row = next(row for row in rows if row["experiment_id"] == "full_e6")
    candidates = []
    for row in rows:
        decision = decision_for_row(row, full_row, best_lift5)
        row["decision"] = decision
        if decision in {"REFERENCE", "KEEP_AS_CANDIDATE"}:
            candidates.append(row)
    return sorted(candidates, key=lambda item: (-item["lift_at_5pct"], -item["pr_auc"], item["feature_count"]))[0]


def write_pruning_review(path: Path, rows: list[dict[str, Any]], recommended: dict[str, Any]) -> None:
    full = next(row for row in rows if row["experiment_id"] == "full_e6")
    lines = [
        "# E6.1 Pruning Evaluation",
        "",
        "## Goal",
        "",
        "Identify the smallest E6 velocity feature subset that preserves the ranking gains observed in the full E6 experiment.",
        "",
        "## Constraints",
        "",
        "- Baseline V2-2 features are unchanged.",
        "- Only previously-defined E6 features are compared.",
        "- No new features, model classes, labels, temporal splits, threshold tuning, or calibration are introduced.",
        "- Artifacts are aggregate-only and contain no row-level predictions.",
        "",
        "## Metric Comparison",
        "",
        "| Experiment | E6 features | ROC-AUC | PR-AUC | Precision@1% | Precision@5% | Precision@10% | Lift@5% | PR-AUC vs full E6 | Lift@5 vs full E6 | Decision |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows:
        lines.append(
            f"| `{row['experiment_id']}` | {row['feature_count']} | {row['roc_auc']:.6f} | {row['pr_auc']:.6f} | "
            f"{row['precision_at_1pct']:.6f} | {row['precision_at_5pct']:.6f} | {row['precision_at_10pct']:.6f} | "
            f"{row['lift_at_5pct']:.6f} | {metric_delta(row['pr_auc'], full['pr_auc']):.2%} | "
            f"{metric_delta(row['lift_at_5pct'], full['lift_at_5pct']):.2%} | {row['decision']} |"
        )
    lines.extend(
        [
            "",
            "## Decision Rule",
            "",
            "Keep only E6 features that contribute at least 0.2% PR-AUC or improve Lift@5. Prefer the simplest model with maximal TopK gain.",
            "",
            "## Recommendation",
            "",
            f"Recommended E6.1 candidate: `{recommended['experiment_id']}`.",
            "",
            f"Included E6 features: `{recommended['included_e6_features']}`.",
            "",
            "This recommendation should be reviewed against the full E6 reference before freezing a production feature set.",
            "",
            "## Privacy",
            "",
            "This report contains only aggregate metrics and feature names. It does not include raw client IDs, raw query text, product names, row-level examples, or row-level predictions.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    config_path = V21.resolve_repo_path(args.feature_config)
    train_input = V21.resolve_repo_path(args.train_input)
    validation_input = V21.resolve_repo_path(args.validation_input)
    events_base = V21.resolve_repo_path(args.events_base)
    artifact_dir = V21.resolve_repo_path(args.artifact_dir)
    csv_path = artifact_dir / "e6_feature_ablation_summary.csv"
    review_path = artifact_dir / "e6_pruning_evaluation.md"
    summary_path = artifact_dir / "e6_pruning_summary.json"

    config = E6.load_feature_config(config_path)
    removed_features = set(config["removed_features"])
    feature_groups: dict[str, list[str]] = config["engineered_feature_groups"]
    experiments = build_experiments(feature_groups)
    e6_feature_set = set(E6.flatten_feature_groups(feature_groups))

    spark = E6.start_spark()
    try:
        train_base = spark.read.parquet(str(train_input)).cache()
        validation_base = spark.read.parquet(str(validation_input)).cache()
        events_by_name = {
            name: spark.read.parquet(str(events_base / name)).cache()
            for name in E6.EVENT_TABLES_FOR_ACTIVITY
        }
        if V21.label_like_columns(train_base.columns) or V21.label_like_columns(validation_base.columns):
            raise ValueError("Training or validation dataset contains label-like feature columns")

        feature_columns_before = V21.numeric_feature_columns(train_base.schema, set())
        missing_removed = sorted(removed_features.difference(feature_columns_before))
        if missing_removed:
            raise ValueError(f"Configured removed features are absent from training dataset: {missing_removed}")

        train_activity_proxy = E6.active_days_30d_proxy(events_by_name, E6.TRAIN_CUTOFF_DATE).cache()
        validation_activity_proxy = E6.active_days_30d_proxy(events_by_name, E6.VALIDATION_CUTOFF_DATE).cache()
        train_augmented = E6.add_e6_features(train_base, train_activity_proxy).cache()
        validation_augmented = E6.add_e6_features(validation_base, validation_activity_proxy).cache()
        full_feature_columns = V21.numeric_feature_columns(train_augmented.schema, removed_features)
        if V21.numeric_feature_columns(validation_augmented.schema, removed_features) != full_feature_columns:
            raise ValueError("E6 train and validation feature columns differ")
        if sorted(e6_feature_set.difference(full_feature_columns)):
            raise ValueError("One or more E6 features are missing from model input")

        train_summary = V21.dataset_summary(train_augmented, "Train")
        validation_summary = V21.dataset_summary(validation_augmented, "Validation")
        positive_weight = train_summary["row_count"] / (2.0 * train_summary["positive_count"])
        negative_weight = train_summary["row_count"] / (2.0 * train_summary["negative_count"])
        train_df = V21.add_class_weight(train_augmented, positive_weight, negative_weight).cache()
        base_v22_columns = [feature for feature in full_feature_columns if feature not in e6_feature_set]

        rows: list[dict[str, Any]] = []
        for experiment in experiments:
            included_e6_features = experiment["included_e6_features"]
            feature_columns = base_v22_columns + included_e6_features
            metrics = E6.train_and_evaluate(
                train_df=train_df,
                validation_df=validation_augmented,
                feature_columns=feature_columns,
                validation_summary=validation_summary,
                max_iter=args.max_iter,
                model_output_path=None,
            )
            rows.append(
                {
                    "experiment_id": experiment["experiment_id"],
                    "description": experiment["description"],
                    "included_e6_features": ",".join(included_e6_features),
                    "feature_count": len(included_e6_features),
                    "total_model_feature_count": len(feature_columns),
                    **metrics,
                }
            )

        recommended = best_recommendation(rows)
        full = next(row for row in rows if row["experiment_id"] == "full_e6")
        for row in rows:
            row["pr_auc_vs_full_e6"] = metric_delta(row["pr_auc"], full["pr_auc"])
            row["lift5_vs_full_e6"] = metric_delta(row["lift_at_5pct"], full["lift_at_5pct"])

        write_csv(
            csv_path,
            rows,
            [
                "experiment_id",
                "description",
                "included_e6_features",
                "feature_count",
                "total_model_feature_count",
                "roc_auc",
                "pr_auc",
                "precision_at_1pct",
                "precision_at_5pct",
                "precision_at_10pct",
                "lift_at_1pct",
                "lift_at_5pct",
                "lift_at_10pct",
                "pr_auc_vs_full_e6",
                "lift5_vs_full_e6",
                "decision",
            ],
        )
        write_pruning_review(review_path, rows, recommended)
        summary = {
            "experiment": "e6_1_pruning",
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "base_model": "baseline_v2_2_plus_e6_candidates",
            "decision_rule": "Keep E6 features contributing >=0.2% PR-AUC or improving Lift@5; prefer simplest model with maximal TopK gain.",
            "recommended_experiment": recommended["experiment_id"],
            "recommended_e6_features": recommended["included_e6_features"],
            "rows": [{key: normalize_value(value) for key, value in row.items()} for row in rows],
        }
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

        print("E6.1 pruning experiment completed.")
        print(f"Experiments run: {len(rows)}")
        print(f"Recommended experiment: {recommended['experiment_id']}")
        print(f"Recommended E6 features: {recommended['included_e6_features']}")
        print(f"PR-AUC: {recommended['pr_auc']:.6f}")
        print(f"Lift@5%: {recommended['lift_at_5pct']:.6f}")
        print(f"Evaluation: {V21.relative_path(review_path)}")
        print(f"CSV summary: {V21.relative_path(csv_path)}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
