"""Train Baseline V2-4 consolidated E6.1 model.

V2-4 keeps the frozen Baseline V2-2 feature set unchanged and adds only the
selected E6.1 velocity features. It compares V2-2, full E6, and V2-4 using the
same temporal validation setup and Logistic Regression pipeline.

The job writes aggregate-only artifacts and does not persist row-level
predictions.
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
E6_JOB_PATH = PROJECT_ROOT / "experiments" / "05l_train_e6_velocity.py"
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "baseline_v24_features.json"
DEFAULT_FULL_E6_SUMMARY = PROJECT_ROOT / "artifacts" / "modeling" / "e6_trend_velocity" / "e6_summary.json"
DEFAULT_TRAIN_INPUT = (
    PROJECT_ROOT / "data" / "processed" / "training" / "e1_train_2022_10_10" / "purchase_propensity_30d"
)
DEFAULT_VALIDATION_INPUT = (
    PROJECT_ROOT / "data" / "processed" / "training" / "e1_valid_2022_11_09" / "purchase_propensity_30d"
)
DEFAULT_EVENTS_BASE = PROJECT_ROOT / "data" / "processed" / "events"
DEFAULT_MODEL_OUTPUT = PROJECT_ROOT / "data" / "models" / "purchase_propensity_baseline_v24"
DEFAULT_ARTIFACT_DIR = PROJECT_ROOT / "artifacts" / "modeling" / "baseline_v2"
DEFAULT_MAX_ITER = 20


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
    parser = argparse.ArgumentParser(description="Train Baseline V2-4 consolidated E6.1 model.")
    parser.add_argument("--feature-config", default=DEFAULT_CONFIG_PATH.relative_to(PROJECT_ROOT).as_posix())
    parser.add_argument("--full-e6-summary", default=DEFAULT_FULL_E6_SUMMARY.relative_to(PROJECT_ROOT).as_posix())
    parser.add_argument("--train-input", default=DEFAULT_TRAIN_INPUT.relative_to(PROJECT_ROOT).as_posix())
    parser.add_argument("--validation-input", default=DEFAULT_VALIDATION_INPUT.relative_to(PROJECT_ROOT).as_posix())
    parser.add_argument("--events-base", default=DEFAULT_EVENTS_BASE.relative_to(PROJECT_ROOT).as_posix())
    parser.add_argument("--model-output", default=DEFAULT_MODEL_OUTPUT.relative_to(PROJECT_ROOT).as_posix())
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


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def metric_delta(candidate: float, baseline: float) -> float:
    return E6.safe_divide(candidate - baseline, baseline)


def decision(metrics: dict[str, Any]) -> str:
    improves_pr = metrics["pr_auc"] > E6.BASELINE_V22_METRICS["pr_auc"]
    improves_lift5 = metrics["lift_at_5pct"] > E6.BASELINE_V22_METRICS["lift_at_5pct"]
    if improves_pr or improves_lift5:
        return "CANDIDATE_FOR_PRODUCTION_MERGE"
    return "KEEP_V2_2"


def comparison_rows(v24_metrics: dict[str, Any], full_e6_metrics: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "model": "baseline_v2_2",
            "description": "Frozen production baseline candidate before E6",
            "total_model_feature_count": 27,
            "e6_feature_count": 0,
            "included_e6_features": "",
            "excluded_e6_features": "",
            **E6.BASELINE_V22_METRICS,
        },
        {
            "model": "v2_2_plus_full_e6",
            "description": "V2-2 plus all six E6 velocity features",
            "total_model_feature_count": 33,
            "e6_feature_count": 6,
            "included_e6_features": "cart_velocity_30d_vs_90d,cart_delta_30d_90d,buy_velocity_30d_vs_90d,buy_delta_30d_90d,search_velocity_30d_vs_90d,activity_intensity_ratio",
            "excluded_e6_features": "",
            **full_e6_metrics,
        },
        {
            "model": "baseline_v2_4_e6_1",
            "description": "V2-2 plus selected E6.1 velocity subset",
            "total_model_feature_count": 32,
            "e6_feature_count": 5,
            "included_e6_features": "cart_velocity_30d_vs_90d,cart_delta_30d_90d,buy_velocity_30d_vs_90d,buy_delta_30d_90d,search_velocity_30d_vs_90d",
            "excluded_e6_features": "activity_intensity_ratio",
            **v24_metrics,
        },
    ]


def write_evaluation(path: Path, rows: list[dict[str, Any]], v24_metrics: dict[str, Any], train_summary: dict[str, Any], validation_summary: dict[str, Any], model_output: Path) -> None:
    metric_keys = [
        ("ROC-AUC", "roc_auc"),
        ("PR-AUC", "pr_auc"),
        ("Precision@1%", "precision_at_1pct"),
        ("Precision@5%", "precision_at_5pct"),
        ("Precision@10%", "precision_at_10pct"),
        ("Lift@1%", "lift_at_1pct"),
        ("Lift@5%", "lift_at_5pct"),
        ("Lift@10%", "lift_at_10pct"),
    ]
    lines = [
        "# V2-4 Consolidation Evaluation",
        "",
        "## Goal",
        "",
        "Compare Baseline V2-2, V2-2 plus full E6, and V2-2 plus the pruned E6.1 feature set.",
        "",
        "## Constraints",
        "",
        "- V2-2 features are unchanged.",
        "- V2-4 adds only the selected E6.1 velocity features.",
        "- No sequence, graph, transition, threshold tuning, calibration, or model architecture changes are introduced.",
        "- Artifacts are aggregate-only.",
        "",
        "## Metric Comparison",
        "",
        "| Model | Total features | E6 features | ROC-AUC | PR-AUC | Precision@1% | Precision@5% | Precision@10% | Lift@1% | Lift@5% | Lift@10% | PR-AUC vs V2-2 | Lift@5 vs V2-2 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        lines.append(
            f"| `{row['model']}` | {row['total_model_feature_count']} | {row['e6_feature_count']} | "
            f"{row['roc_auc']:.6f} | {row['pr_auc']:.6f} | {row['precision_at_1pct']:.6f} | "
            f"{row['precision_at_5pct']:.6f} | {row['precision_at_10pct']:.6f} | "
            f"{row['lift_at_1pct']:.6f} | {row['lift_at_5pct']:.6f} | {row['lift_at_10pct']:.6f} | "
            f"{metric_delta(row['pr_auc'], E6.BASELINE_V22_METRICS['pr_auc']):.2%} | "
            f"{metric_delta(row['lift_at_5pct'], E6.BASELINE_V22_METRICS['lift_at_5pct']):.2%} |"
        )
    lines.extend(
        [
            "",
            "## Decision",
            "",
            decision(v24_metrics),
            "",
            "Decision rule: if E6.1 improves PR-AUC or Lift@5 versus V2-2, mark as candidate for production merge. Prefer the smallest feature set with stable TopK improvement.",
            "",
            "## V2-4 Feature Change",
            "",
            "Added E6.1 features:",
            "",
            "- `cart_velocity_30d_vs_90d`",
            "- `cart_delta_30d_90d`",
            "- `buy_velocity_30d_vs_90d`",
            "- `buy_delta_30d_90d`",
            "- `search_velocity_30d_vs_90d`",
            "",
            "Excluded noisy E6 feature:",
            "",
            "- `activity_intensity_ratio`",
            "",
            "## Experiment Context",
            "",
            f"- Train rows: {train_summary['row_count']:,}",
            f"- Validation rows: {validation_summary['row_count']:,}",
            f"- Train positive rate: {train_summary['positive_rate']:.6f}",
            f"- Validation positive rate: {validation_summary['positive_rate']:.6f}",
            f"- V2-4 model output: `{V21.relative_path(model_output)}`",
            "",
            "## Privacy",
            "",
            "No row-level predictions, raw client IDs, raw query text, product names, or row-level examples are written to artifacts.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_ablation_summary(path: Path, rows: list[dict[str, Any]]) -> None:
    v22 = next(row for row in rows if row["model"] == "baseline_v2_2")
    full = next(row for row in rows if row["model"] == "v2_2_plus_full_e6")
    v24 = next(row for row in rows if row["model"] == "baseline_v2_4_e6_1")
    lines = [
        "# V2-4 Ablation Summary",
        "",
        "## Summary",
        "",
        "V2-4 consolidates E6.1 by retaining the velocity features that preserved ranking gains while removing the noisy `activity_intensity_ratio` feature.",
        "",
        "## Comparison",
        "",
        "| Comparison | PR-AUC change | Lift@5 change | Interpretation |",
        "|---|---:|---:|---|",
        f"| Full E6 vs V2-2 | {metric_delta(full['pr_auc'], v22['pr_auc']):.2%} | {metric_delta(full['lift_at_5pct'], v22['lift_at_5pct']):.2%} | Full velocity signal improves ranking over V2-2. |",
        f"| V2-4 E6.1 vs V2-2 | {metric_delta(v24['pr_auc'], v22['pr_auc']):.2%} | {metric_delta(v24['lift_at_5pct'], v22['lift_at_5pct']):.2%} | Pruned velocity signal remains additive over V2-2. |",
        f"| V2-4 E6.1 vs full E6 | {metric_delta(v24['pr_auc'], full['pr_auc']):.2%} | {metric_delta(v24['lift_at_5pct'], full['lift_at_5pct']):.2%} | Removing `activity_intensity_ratio` checks whether the simpler subset preserves or improves TopK ranking. |",
        "",
        "## Feature Decision",
        "",
        "- Keep cart velocity features as minor positive contributors.",
        "- Keep buy velocity features as the dominant E6 signal family.",
        "- Keep search velocity in V2-4 because E6.1 selection retained the best TopK candidate.",
        "- Exclude `activity_intensity_ratio` because E6.1 confirmed it as noisy.",
        "",
        "## Production Merge Readiness",
        "",
        "V2-4 is a candidate for production merge only if its PR-AUC or Lift@5 remains above V2-2 in the consolidated run.",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    config_path = V21.resolve_repo_path(args.feature_config)
    full_e6_summary_path = V21.resolve_repo_path(args.full_e6_summary)
    train_input = V21.resolve_repo_path(args.train_input)
    validation_input = V21.resolve_repo_path(args.validation_input)
    events_base = V21.resolve_repo_path(args.events_base)
    model_output = V21.resolve_repo_path(args.model_output)
    artifact_dir = V21.resolve_repo_path(args.artifact_dir)
    evaluation_path = artifact_dir / "v24_consolidation_evaluation.md"
    comparison_path = artifact_dir / "v24_feature_comparison.csv"
    ablation_path = artifact_dir / "v24_ablation_summary.md"
    summary_path = artifact_dir / "v24_summary.json"

    config = load_json(config_path)
    full_e6_summary = load_json(full_e6_summary_path)
    removed_features = set(config["removed_features"])
    selected_e6_features: list[str] = config["selected_e6_features"]
    excluded_e6_features: list[str] = config["excluded_e6_features"]
    all_e6_features = selected_e6_features + excluded_e6_features

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
            raise ValueError("V2-4 train and validation feature columns differ")
        missing_selected = sorted(set(all_e6_features).difference(full_feature_columns))
        if missing_selected:
            raise ValueError(f"Configured E6 features are missing from model input: {missing_selected}")

        base_v22_columns = [feature for feature in full_feature_columns if feature not in set(all_e6_features)]
        v24_feature_columns = base_v22_columns + selected_e6_features
        train_summary = V21.dataset_summary(train_augmented, "Train")
        validation_summary = V21.dataset_summary(validation_augmented, "Validation")
        positive_weight = train_summary["row_count"] / (2.0 * train_summary["positive_count"])
        negative_weight = train_summary["row_count"] / (2.0 * train_summary["negative_count"])
        train_df = V21.add_class_weight(train_augmented, positive_weight, negative_weight).cache()

        v24_metrics = E6.train_and_evaluate(
            train_df=train_df,
            validation_df=validation_augmented,
            feature_columns=v24_feature_columns,
            validation_summary=validation_summary,
            max_iter=args.max_iter,
            model_output_path=model_output,
        )
        rows = comparison_rows(v24_metrics, full_e6_summary["metrics"])
        for row in rows:
            row["pr_auc_vs_v22"] = metric_delta(row["pr_auc"], E6.BASELINE_V22_METRICS["pr_auc"])
            row["lift5_vs_v22"] = metric_delta(row["lift_at_5pct"], E6.BASELINE_V22_METRICS["lift_at_5pct"])

        write_csv(
            comparison_path,
            rows,
            [
                "model",
                "description",
                "total_model_feature_count",
                "e6_feature_count",
                "included_e6_features",
                "excluded_e6_features",
                "roc_auc",
                "pr_auc",
                "precision_at_1pct",
                "precision_at_5pct",
                "precision_at_10pct",
                "lift_at_1pct",
                "lift_at_5pct",
                "lift_at_10pct",
                "pr_auc_vs_v22",
                "lift5_vs_v22",
            ],
        )
        write_evaluation(evaluation_path, rows, v24_metrics, train_summary, validation_summary, model_output)
        write_ablation_summary(ablation_path, rows)
        summary = {
            "experiment": "baseline_v2_4_e6_1_consolidated",
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "base_model": "baseline_v2_2",
            "model_output": args.model_output,
            "selected_e6_features": selected_e6_features,
            "excluded_e6_features": excluded_e6_features,
            "feature_count": len(v24_feature_columns),
            "metrics": {key: normalize_value(value) for key, value in v24_metrics.items()},
            "decision": decision(v24_metrics),
        }
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

        print("Baseline V2-4 consolidated experiment completed.")
        print(f"Selected E6.1 features: {len(selected_e6_features)}")
        print(f"Total model feature count: {len(v24_feature_columns)}")
        print(f"ROC-AUC: {v24_metrics['roc_auc']:.6f}")
        print(f"PR-AUC: {v24_metrics['pr_auc']:.6f}")
        print(f"Lift@5%: {v24_metrics['lift_at_5pct']:.6f}")
        print(f"Decision: {decision(v24_metrics)}")
        print(f"Evaluation: {V21.relative_path(evaluation_path)}")
        print(f"Feature comparison: {V21.relative_path(comparison_path)}")
        print(f"Ablation summary: {V21.relative_path(ablation_path)}")
        print(f"Model output: {V21.relative_path(model_output)}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
