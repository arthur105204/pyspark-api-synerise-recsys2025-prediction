"""Export lightweight Logistic Regression metadata for manual API scoring.

This Phase 7B job reads the saved Phase 5 Spark ML pipeline metadata and writes
a local JSON file that the API can use without Spark in the request path.

It exports model coefficients, intercept, feature order, and imputation values
only. It does not persist row-level data, client ids, raw feature rows, or
prediction examples.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import pyarrow.parquet as pq
except ImportError as exc:  # pragma: no cover - exercised only when dependency is absent
    pq = None
    PYARROW_IMPORT_ERROR = exc
else:
    PYARROW_IMPORT_ERROR = None


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MODEL_PATH = PROJECT_ROOT / "data" / "models" / "purchase_propensity_baseline"
DEFAULT_FEATURE_PROCESSING_PATH = PROJECT_ROOT / "artifacts" / "modeling" / "feature_processing_summary.csv"
DEFAULT_MODEL_SUMMARY_PATH = PROJECT_ROOT / "artifacts" / "modeling" / "baseline_model_summary.json"
DEFAULT_OUTPUT_PATH = PROJECT_ROOT / "data" / "serving" / "model_metadata" / "baseline_lr_v1.json"
DEFAULT_ARTIFACT_PATH = PROJECT_ROOT / "artifacts" / "serving" / "model_metadata_summary.json"
DEFAULT_MODEL_VERSION = "baseline_lr_v1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export lightweight Logistic Regression model metadata.")
    parser.add_argument(
        "--model-path",
        default=DEFAULT_MODEL_PATH.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative saved Spark ML model path.",
    )
    parser.add_argument(
        "--feature-processing",
        default=DEFAULT_FEATURE_PROCESSING_PATH.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative feature processing artifact path.",
    )
    parser.add_argument(
        "--model-summary",
        default=DEFAULT_MODEL_SUMMARY_PATH.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative baseline model summary artifact path.",
    )
    parser.add_argument(
        "--output-path",
        default=DEFAULT_OUTPUT_PATH.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative local serving metadata output path.",
    )
    parser.add_argument(
        "--artifact-path",
        default=DEFAULT_ARTIFACT_PATH.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative sanitized metadata summary artifact path.",
    )
    parser.add_argument("--model-version", default=DEFAULT_MODEL_VERSION)
    return parser.parse_args()


def resolve_repo_path(path_text: str) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        raise ValueError("path arguments must be repo-relative")
    return PROJECT_ROOT / path


def relative_path(path: Path) -> str:
    return path.relative_to(PROJECT_ROOT).as_posix()


def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")


def read_feature_order(path: Path) -> list[str]:
    features: list[str] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if str(row.get("used_in_model", "")).lower() == "true":
                feature_name = row.get("feature_name")
                if feature_name:
                    features.append(feature_name)
    if not features:
        raise ValueError("No model features found in feature processing artifact")
    return features


def find_stage(model_path: Path, stage_keyword: str) -> Path:
    stages_dir = model_path / "stages"
    matches = sorted(path for path in stages_dir.iterdir() if path.is_dir() and stage_keyword in path.name)
    if len(matches) != 1:
        raise ValueError(f"Expected exactly one saved {stage_keyword} stage")
    return matches[0]


def read_single_parquet_row(data_dir: Path) -> dict[str, Any]:
    if pq is None:
        raise RuntimeError("pyarrow is required to read saved Spark ML metadata") from PYARROW_IMPORT_ERROR
    parquet_files = sorted(data_dir.glob("part-*.parquet"))
    if len(parquet_files) != 1:
        raise ValueError(f"Expected exactly one Parquet part under {relative_path(data_dir)}")
    rows = pq.read_table(parquet_files[0]).to_pylist()
    if len(rows) != 1:
        raise ValueError(f"Expected exactly one metadata row under {relative_path(data_dir)}")
    return rows[0]


def read_imputation_values(model_path: Path, feature_order: list[str]) -> dict[str, float]:
    imputer_stage = find_stage(model_path, "Imputer")
    row = read_single_parquet_row(imputer_stage / "data")
    imputation_values = {}
    for feature_name in feature_order:
        if feature_name not in row:
            raise ValueError(f"Missing imputation value for feature: {feature_name}")
        imputation_values[feature_name] = float(row[feature_name])
    return imputation_values


def read_logistic_regression_params(model_path: Path) -> tuple[list[float], float, int]:
    lr_stage = find_stage(model_path, "LogisticRegression")
    row = read_single_parquet_row(lr_stage / "data")
    coefficient_matrix = row.get("coefficientMatrix") or {}
    coefficients = coefficient_matrix.get("values")
    intercept_vector = row.get("interceptVector") or {}
    intercept_values = intercept_vector.get("values")
    feature_count = int(row.get("numFeatures"))
    if not isinstance(coefficients, list) or len(coefficients) != feature_count:
        raise ValueError("Coefficient vector length does not match numFeatures")
    if not isinstance(intercept_values, list) or len(intercept_values) != 1:
        raise ValueError("Expected one Logistic Regression intercept value")
    return [float(value) for value in coefficients], float(intercept_values[0]), feature_count


def build_metadata(args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, Any]]:
    model_path = resolve_repo_path(args.model_path)
    feature_processing_path = resolve_repo_path(args.feature_processing)
    model_summary_path = resolve_repo_path(args.model_summary)
    output_path = resolve_repo_path(args.output_path)

    baseline_summary = read_json(model_summary_path)
    feature_order = read_feature_order(feature_processing_path)
    imputation_values = read_imputation_values(model_path, feature_order)
    coefficients, intercept, coefficient_feature_count = read_logistic_regression_params(model_path)
    threshold = float(baseline_summary.get("threshold", 0.5))

    if coefficient_feature_count != len(feature_order):
        raise ValueError("Feature order length does not match coefficient vector length")

    generated_at = datetime.now(timezone.utc).date().isoformat()
    metadata = {
        "generated_at_date": generated_at,
        "model_version": args.model_version,
        "model_type": "LogisticRegression",
        "source_model_path": relative_path(model_path),
        "feature_order": feature_order,
        "feature_count": len(feature_order),
        "coefficients": coefficients,
        "intercept": intercept,
        "threshold": threshold,
        "imputation_values": imputation_values,
        "decision_mapping": {
            "1": "likely_to_buy",
            "0": "not_likely_to_buy",
        },
    }
    artifact = {
        "generated_at_date": generated_at,
        "phase": "Phase 7B: Manual Feature-Input Prediction",
        "status": "success",
        "model_version": args.model_version,
        "model_type": "LogisticRegression",
        "local_model_metadata_path": relative_path(output_path),
        "local_model_metadata_excluded_from_git": True,
        "feature_count": len(feature_order),
        "coefficient_count": len(coefficients),
        "intercept_exported": True,
        "threshold": threshold,
        "imputation_values_exported": True,
        "api_runs_spark_per_request": False,
        "api_runs_spark_ml_inference_per_request": False,
        "privacy_validation": {
            "artifact_level": "aggregate_model_metadata_summary",
            "real_client_ids_persisted": False,
            "row_level_real_scores_persisted": False,
            "raw_feature_rows_persisted": False,
            "raw_query_text_persisted": False,
            "product_names_persisted": False,
            "local_paths_persisted": False,
            "passed": True,
        },
    }
    return metadata, artifact


def main() -> int:
    args = parse_args()
    output_path = resolve_repo_path(args.output_path)
    artifact_path = resolve_repo_path(args.artifact_path)
    metadata, artifact = build_metadata(args)
    write_json(output_path, metadata)
    write_json(artifact_path, artifact)
    print("Model metadata export completed.")
    print(f"Local metadata output: {relative_path(output_path)}")
    print(f"Sanitized artifact: {relative_path(artifact_path)}")
    print(f"Feature count: {metadata['feature_count']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
