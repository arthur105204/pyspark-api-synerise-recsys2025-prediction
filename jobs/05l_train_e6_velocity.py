"""Run E6 trend/velocity feature experiment on top of Baseline V2-2.

This job adds a small, controlled set of derived velocity features in-memory,
trains an experimental Logistic Regression model using the same temporal split
and evaluation pattern as V2-2, and runs group ablations for the E6 feature
families.

It does not modify existing V2-2 features, preprocessing outputs, labels,
production models, API behavior, or model architecture. It does not persist
row-level predictions or raw identifiers in artifacts.
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
V21_JOB_PATH = PROJECT_ROOT / "jobs" / "05g_train_baseline_v21.py"
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "e6_velocity_features.json"
DEFAULT_TRAIN_INPUT = (
    PROJECT_ROOT / "data" / "processed" / "training" / "e1_train_2022_10_10" / "purchase_propensity_30d"
)
DEFAULT_VALIDATION_INPUT = (
    PROJECT_ROOT / "data" / "processed" / "training" / "e1_valid_2022_11_09" / "purchase_propensity_30d"
)
DEFAULT_EVENTS_BASE = PROJECT_ROOT / "data" / "processed" / "events"
DEFAULT_MODEL_OUTPUT = PROJECT_ROOT / "data" / "models" / "purchase_propensity_e6_velocity"
DEFAULT_ARTIFACT_DIR = PROJECT_ROOT / "artifacts" / "modeling" / "e6_trend_velocity"
DEFAULT_MAX_ITER = 20
TRAIN_CUTOFF_DATE = "2022-10-10"
VALIDATION_CUTOFF_DATE = "2022-11-09"
ACTIVE_DAYS_PROXY_WINDOW = 30
EVENT_TABLES_FOR_ACTIVITY = ("add_to_cart", "remove_from_cart", "product_buy", "search_query")
BASELINE_V22_METRICS = {
    "roc_auc": 0.834208,
    "pr_auc": 0.255374,
    "precision_at_1pct": 0.484882,
    "precision_at_5pct": 0.296158,
    "precision_at_10pct": 0.216155,
    "lift_at_1pct": 11.135066,
    "lift_at_5pct": 6.801107,
    "lift_at_10pct": 4.963885,
}


def load_v21_module() -> Any:
    spec = importlib.util.spec_from_file_location("baseline_v21_job", V21_JOB_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load Baseline V2-1 helper module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


V21 = load_v21_module()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run E6 trend/velocity feature experiment.")
    parser.add_argument("--feature-config", default=DEFAULT_CONFIG_PATH.relative_to(PROJECT_ROOT).as_posix())
    parser.add_argument("--train-input", default=DEFAULT_TRAIN_INPUT.relative_to(PROJECT_ROOT).as_posix())
    parser.add_argument("--validation-input", default=DEFAULT_VALIDATION_INPUT.relative_to(PROJECT_ROOT).as_posix())
    parser.add_argument("--events-base", default=DEFAULT_EVENTS_BASE.relative_to(PROJECT_ROOT).as_posix())
    parser.add_argument("--model-output", default=DEFAULT_MODEL_OUTPUT.relative_to(PROJECT_ROOT).as_posix())
    parser.add_argument("--artifact-dir", default=DEFAULT_ARTIFACT_DIR.relative_to(PROJECT_ROOT).as_posix())
    parser.add_argument("--max-iter", type=int, default=DEFAULT_MAX_ITER)
    return parser.parse_args()


def start_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("e6-trend-velocity")
        .master("local[*]")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def normalize_value(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    return value


def safe_divide(numerator: float, denominator: float) -> float:
    return float(numerator) / float(denominator) if denominator else 0.0


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: normalize_value(value) for key, value in row.items()})


def load_feature_config(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if "removed_features" not in payload or "engineered_feature_groups" not in payload:
        raise ValueError("E6 config must contain removed_features and engineered_feature_groups")
    return payload


def filter_event_history(df: DataFrame, cutoff_date: str, window_days: int | None = None) -> DataFrame:
    condition = F.col("event_date") < F.to_date(F.lit(cutoff_date))
    if window_days is not None:
        condition = condition & (F.col("event_date") >= F.date_sub(F.to_date(F.lit(cutoff_date)), int(window_days)))
    return df.where(condition)


def active_days_30d_proxy(events_by_name: dict[str, DataFrame], cutoff_date: str) -> DataFrame:
    frames = [
        filter_event_history(events_by_name[name], cutoff_date, ACTIVE_DAYS_PROXY_WINDOW).select("client_id", "event_date")
        for name in EVENT_TABLES_FOR_ACTIVITY
    ]
    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame)
    return (
        combined.distinct()
        .groupBy("client_id")
        .agg(F.count(F.lit(1)).alias("active_days_count_30d_proxy"))
    )


def count_col(name: str) -> F.Column:
    return F.coalesce(F.col(name).cast("double"), F.lit(0.0))


def add_e6_features(base_df: DataFrame, active_proxy_df: DataFrame) -> DataFrame:
    with_proxy = base_df.join(active_proxy_df, "client_id", "left").fillna({"active_days_count_30d_proxy": 0})
    return (
        with_proxy.withColumn(
            "cart_velocity_30d_vs_90d",
            (count_col("add_to_cart_count_30d") + F.lit(1.0)) / (count_col("add_to_cart_count_90d") + F.lit(1.0)),
        )
        .withColumn(
            "cart_delta_30d_90d",
            count_col("add_to_cart_count_30d") - (count_col("add_to_cart_count_90d") / F.lit(3.0)),
        )
        .withColumn(
            "buy_velocity_30d_vs_90d",
            (count_col("product_buy_count_30d") + F.lit(1.0)) / (count_col("product_buy_count_90d") + F.lit(1.0)),
        )
        .withColumn(
            "buy_delta_30d_90d",
            count_col("product_buy_count_30d") - (count_col("product_buy_count_90d") / F.lit(3.0)),
        )
        .withColumn(
            "search_velocity_30d_vs_90d",
            (count_col("search_query_count_30d") + F.lit(1.0)) / (count_col("search_query_count_90d") + F.lit(1.0)),
        )
        .withColumn(
            "activity_intensity_ratio",
            (count_col("active_days_count_30d_proxy") + F.lit(1.0)) / (count_col("active_days_count") + F.lit(1.0)),
        )
        .drop("active_days_count_30d_proxy")
    )


def flatten_feature_groups(feature_groups: dict[str, list[str]]) -> list[str]:
    features: list[str] = []
    for group_features in feature_groups.values():
        features.extend(group_features)
    return features


def feature_distribution(df: DataFrame, feature_names: list[str], row_count: int) -> list[dict[str, Any]]:
    rows = []
    for feature_name in feature_names:
        row = df.agg(
            F.count(F.col(feature_name)).alias("non_null_count"),
            F.mean(F.col(feature_name).cast("double")).alias("mean"),
            F.stddev(F.col(feature_name).cast("double")).alias("stddev"),
            F.min(F.col(feature_name)).alias("min"),
            F.max(F.col(feature_name)).alias("max"),
        ).collect()[0].asDict()
        non_null_count = int(row["non_null_count"] or 0)
        rows.append(
            {
                "feature_name": feature_name,
                "non_null_count": non_null_count,
                "missing_count": row_count - non_null_count,
                "missing_rate": safe_divide(row_count - non_null_count, row_count),
                "mean": row["mean"],
                "stddev": row["stddev"],
                "min": row["min"],
                "max": row["max"],
            }
        )
    return rows


def evaluate_model(model: Any, validation_df: DataFrame, validation_summary: dict[str, Any]) -> dict[str, Any]:
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
    topk = V21.topk_metrics(
        predictions,
        validation_summary["row_count"],
        validation_summary["positive_count"],
        validation_summary["positive_rate"],
    )
    predictions.unpersist()
    return {"roc_auc": roc_auc, "pr_auc": pr_auc, **topk}


def train_and_evaluate(
    train_df: DataFrame,
    validation_df: DataFrame,
    feature_columns: list[str],
    validation_summary: dict[str, Any],
    max_iter: int,
    model_output_path: Path | None = None,
) -> dict[str, Any]:
    imputed_columns = [f"{column}__imputed" for column in feature_columns]
    pipeline = V21.Pipeline(
        stages=[
            V21.Imputer(inputCols=feature_columns, outputCols=imputed_columns, strategy="median"),
            V21.VectorAssembler(inputCols=imputed_columns, outputCol="features"),
            V21.LogisticRegression(
                featuresCol="features",
                labelCol="label",
                weightCol="class_weight",
                maxIter=int(max_iter),
                standardization=True,
            ),
        ]
    )
    model = pipeline.fit(train_df)
    if model_output_path is not None:
        V21.clean_output_dir(model_output_path, PROJECT_ROOT / "data" / "models")
        model.write().overwrite().save(str(model_output_path))
    return evaluate_model(model, validation_df, validation_summary)


def metric_delta(candidate: float, baseline: float) -> float:
    return safe_divide(candidate - baseline, baseline)


def adoption_decision(metrics: dict[str, Any]) -> str:
    pr_delta = metric_delta(metrics["pr_auc"], BASELINE_V22_METRICS["pr_auc"])
    lift5_delta = metric_delta(metrics["lift_at_5pct"], BASELINE_V22_METRICS["lift_at_5pct"])
    if pr_delta >= 0.005 or lift5_delta >= 0.005:
        return "ADOPT_FOR_REVIEW"
    return "INVESTIGATE"


def write_feature_definitions(path: Path) -> None:
    lines = [
        "# E6 Feature Definitions",
        "",
        "## Scope",
        "",
        "E6 adds only derived trend/velocity features on top of the frozen Baseline V2-2 feature set. Existing V2-2 features are not modified or removed.",
        "",
        "| Feature | Definition | Rationale | Leakage/null handling |",
        "|---|---|---|---|",
        "| `cart_velocity_30d_vs_90d` | `(add_to_cart_count_30d + 1) / (add_to_cart_count_90d + 1)` | Captures whether cart activity is concentrated recently. | Uses pre-cutoff counts; +1 smoothing prevents division by zero. |",
        "| `cart_delta_30d_90d` | `add_to_cart_count_30d - (add_to_cart_count_90d / 3)` | Compares recent cart volume against an average 30-day slice of 90-day history. | Uses pre-cutoff counts; nulls treated as 0. |",
        "| `buy_velocity_30d_vs_90d` | `(product_buy_count_30d + 1) / (product_buy_count_90d + 1)` | Captures recent acceleration in purchase behavior. | Uses pre-cutoff counts; +1 smoothing prevents division by zero. |",
        "| `buy_delta_30d_90d` | `product_buy_count_30d - (product_buy_count_90d / 3)` | Compares recent buying to medium-term buying pace. | Uses pre-cutoff counts; nulls treated as 0. |",
        "| `search_velocity_30d_vs_90d` | `(search_query_count_30d + 1) / (search_query_count_90d + 1)` | Captures recent acceleration in discovery/search activity. | Uses pre-cutoff counts; no raw query text. |",
        "| `activity_intensity_ratio` | `(active_days_count_30d_proxy + 1) / (active_days_count + 1)` | Captures recent activity concentration across cart, buy, remove, and search events. | `active_days_count_30d_proxy` is computed from pre-cutoff event dates only and is not used as a model feature. |",
        "",
        "## Non-Goals",
        "",
        "- No sequence or graph features.",
        "- No search-to-cart transition features.",
        "- No query text or embedding features.",
        "- No model architecture changes.",
        "- No production baseline retraining.",
        "",
        "## Privacy",
        "",
        "Artifacts are aggregate-only. No raw client IDs, raw query text, product names, row-level predictions, or row-level examples are persisted.",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_model_evaluation(path: Path, metrics: dict[str, Any], feature_count: int, train_summary: dict[str, Any], validation_summary: dict[str, Any], model_output: Path) -> None:
    rows = [
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
        "# E6 Model Evaluation",
        "",
        "## Comparison: V2-2 Baseline vs V2-2 + E6 Features",
        "",
        "| Metric | V2-2 baseline | V2-2 + E6 | Relative change |",
        "|---|---:|---:|---:|",
    ]
    for label, key in rows:
        lines.append(f"| {label} | {BASELINE_V22_METRICS[key]:.6f} | {metrics[key]:.6f} | {metric_delta(metrics[key], BASELINE_V22_METRICS[key]):.2%} |")
    lines.extend(
        [
            "",
            "## Adoption Decision",
            "",
            adoption_decision(metrics),
            "",
            "Adoption requires PR-AUC improvement >= 0.5% or Lift@5% improvement >= 0.5%. Otherwise, E6 remains investigational and is not adopted into production.",
            "",
            "## Experiment Context",
            "",
            "- Base production candidate: Baseline V2-2.",
            f"- E6 model feature count: {feature_count}",
            f"- Train rows: {train_summary['row_count']:,}",
            f"- Validation rows: {validation_summary['row_count']:,}",
            f"- Train positive rate: {train_summary['positive_rate']:.6f}",
            f"- Validation positive rate: {validation_summary['positive_rate']:.6f}",
            f"- Experimental model output: `{V21.relative_path(model_output)}`",
            "",
            "## Privacy",
            "",
            "Only aggregate metrics are written. No row-level scores, raw client IDs, raw query text, or product names are persisted in artifacts.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_ablation_analysis(path: Path, ablation_rows: list[dict[str, Any]]) -> None:
    lines = [
        "# E6 Ablation Analysis",
        "",
        "Each row removes one E6 feature group from the full V2-2 + E6 experiment. This measures whether each velocity group contributes incremental ranking value.",
        "",
        "| Removed E6 group | Removed features | ROC-AUC | PR-AUC | Precision@5% | Lift@5% | PR-AUC vs full E6 | Lift@5 vs full E6 |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    full = next(row for row in ablation_rows if row["removed_group"] == "none_full_e6")
    for row in ablation_rows:
        lines.append(
            f"| {row['removed_group']} | {row['removed_features']} | {row['roc_auc']:.6f} | {row['pr_auc']:.6f} | "
            f"{row['precision_at_5pct']:.6f} | {row['lift_at_5pct']:.6f} | "
            f"{metric_delta(row['pr_auc'], full['pr_auc']):.2%} | {metric_delta(row['lift_at_5pct'], full['lift_at_5pct']):.2%} |"
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "If removing a group improves or does not change metrics, that group is not clearly additive. If removing a group hurts PR-AUC or Lift@5%, that group may contain useful velocity signal.",
            "",
            "## Privacy",
            "",
            "This artifact contains aggregate model metrics only.",
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
    model_output = V21.resolve_repo_path(args.model_output)
    artifact_dir = V21.resolve_repo_path(args.artifact_dir)
    definitions_path = artifact_dir / "E6_feature_definitions.md"
    distribution_path = artifact_dir / "e6_feature_distribution.csv"
    evaluation_path = artifact_dir / "e6_model_evaluation.md"
    ablation_path = artifact_dir / "e6_ablation_analysis.md"
    ablation_csv_path = artifact_dir / "e6_ablation_results.csv"
    summary_path = artifact_dir / "e6_summary.json"

    config = load_feature_config(config_path)
    removed_features = set(config["removed_features"])
    feature_groups: dict[str, list[str]] = config["engineered_feature_groups"]
    e6_features = flatten_feature_groups(feature_groups)

    spark = start_spark()
    try:
        train_base = spark.read.parquet(str(train_input)).cache()
        validation_base = spark.read.parquet(str(validation_input)).cache()
        events_by_name = {
            name: spark.read.parquet(str(events_base / name)).cache()
            for name in EVENT_TABLES_FOR_ACTIVITY
        }
        if V21.label_like_columns(train_base.columns) or V21.label_like_columns(validation_base.columns):
            raise ValueError("Training or validation dataset contains label-like feature columns")

        feature_columns_before = V21.numeric_feature_columns(train_base.schema, set())
        missing_removed = sorted(removed_features.difference(feature_columns_before))
        if missing_removed:
            raise ValueError(f"Configured removed features are absent from training dataset: {missing_removed}")

        train_activity_proxy = active_days_30d_proxy(events_by_name, TRAIN_CUTOFF_DATE).cache()
        validation_activity_proxy = active_days_30d_proxy(events_by_name, VALIDATION_CUTOFF_DATE).cache()
        train_augmented = add_e6_features(train_base, train_activity_proxy).cache()
        validation_augmented = add_e6_features(validation_base, validation_activity_proxy).cache()

        feature_columns_e6 = V21.numeric_feature_columns(train_augmented.schema, removed_features)
        validation_feature_columns_e6 = V21.numeric_feature_columns(validation_augmented.schema, removed_features)
        if validation_feature_columns_e6 != feature_columns_e6:
            raise ValueError("E6 train and validation feature columns differ")
        missing_e6 = sorted(set(e6_features).difference(feature_columns_e6))
        if missing_e6:
            raise ValueError(f"E6 engineered features missing from model input: {missing_e6}")

        train_summary = V21.dataset_summary(train_augmented, "Train")
        validation_summary = V21.dataset_summary(validation_augmented, "Validation")
        distribution_rows = feature_distribution(train_augmented, e6_features, train_summary["row_count"])
        write_csv(
            distribution_path,
            distribution_rows,
            ["feature_name", "non_null_count", "missing_count", "missing_rate", "mean", "stddev", "min", "max"],
        )
        write_feature_definitions(definitions_path)

        positive_weight = train_summary["row_count"] / (2.0 * train_summary["positive_count"])
        negative_weight = train_summary["row_count"] / (2.0 * train_summary["negative_count"])
        train_df = V21.add_class_weight(train_augmented, positive_weight, negative_weight).cache()

        full_metrics = train_and_evaluate(
            train_df=train_df,
            validation_df=validation_augmented,
            feature_columns=feature_columns_e6,
            validation_summary=validation_summary,
            max_iter=args.max_iter,
            model_output_path=model_output,
        )
        ablation_rows: list[dict[str, Any]] = [
            {
                "removed_group": "none_full_e6",
                "removed_features": "none",
                **full_metrics,
            }
        ]
        for group_name, group_features in feature_groups.items():
            ablation_features = [feature for feature in feature_columns_e6 if feature not in set(group_features)]
            metrics = train_and_evaluate(
                train_df=train_df,
                validation_df=validation_augmented,
                feature_columns=ablation_features,
                validation_summary=validation_summary,
                max_iter=args.max_iter,
                model_output_path=None,
            )
            ablation_rows.append(
                {
                    "removed_group": group_name,
                    "removed_features": ",".join(group_features),
                    **metrics,
                }
            )

        write_csv(
            ablation_csv_path,
            ablation_rows,
            [
                "removed_group",
                "removed_features",
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
        write_model_evaluation(
            evaluation_path,
            full_metrics,
            len(feature_columns_e6),
            train_summary,
            validation_summary,
            model_output,
        )
        write_ablation_analysis(ablation_path, ablation_rows)

        summary = {
            "experiment": "e6_trend_velocity",
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "base_model": "baseline_v2_2",
            "train_input": args.train_input,
            "validation_input": args.validation_input,
            "model_output": args.model_output,
            "feature_count_e6": len(feature_columns_e6),
            "e6_features": e6_features,
            "metrics": {key: normalize_value(value) for key, value in full_metrics.items()},
            "decision": adoption_decision(full_metrics),
        }
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

        print("E6 trend/velocity experiment completed.")
        print(f"E6 feature count added: {len(e6_features)}")
        print(f"Total model feature count: {len(feature_columns_e6)}")
        print(f"ROC-AUC: {full_metrics['roc_auc']:.6f}")
        print(f"PR-AUC: {full_metrics['pr_auc']:.6f}")
        print(f"Lift@5%: {full_metrics['lift_at_5pct']:.6f}")
        print(f"Decision: {adoption_decision(full_metrics)}")
        print(f"Evaluation: {V21.relative_path(evaluation_path)}")
        print(f"Ablation: {V21.relative_path(ablation_path)}")
        print(f"Model output: {V21.relative_path(model_output)}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
