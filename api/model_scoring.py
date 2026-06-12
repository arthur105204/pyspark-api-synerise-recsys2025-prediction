"""Lightweight Logistic Regression scoring for manual feature input."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


class ModelMetadataNotFoundError(RuntimeError):
    """Raised when local lightweight model metadata is unavailable."""


class InvalidModelMetadataError(RuntimeError):
    """Raised when local lightweight model metadata is malformed."""


class InvalidFeatureValueError(ValueError):
    """Raised when a manual feature value cannot be converted to a number."""


@dataclass(frozen=True)
class ManualPredictionResult:
    prediction_score: float
    prediction_label: int
    decision: str
    model_version: str
    missing_features_filled: list[str]
    used_feature_count: int


@dataclass(frozen=True)
class LightweightLogisticRegressionModel:
    model_version: str
    feature_order: list[str]
    coefficients: list[float]
    intercept: float
    threshold: float
    imputation_values: dict[str, float]

    @classmethod
    def from_file(cls, path: str | Path) -> "LightweightLogisticRegressionModel":
        metadata_path = Path(path)
        if not metadata_path.exists():
            raise ModelMetadataNotFoundError("model metadata is not available")
        with metadata_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return cls.from_payload(payload)

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "LightweightLogisticRegressionModel":
        feature_order = payload.get("feature_order")
        coefficients = payload.get("coefficients")
        imputation_values = payload.get("imputation_values")
        if not isinstance(feature_order, list) or not all(isinstance(item, str) for item in feature_order):
            raise InvalidModelMetadataError("model metadata must include feature_order")
        if not isinstance(coefficients, list):
            raise InvalidModelMetadataError("model metadata must include coefficients")
        if len(feature_order) != len(coefficients):
            raise InvalidModelMetadataError("feature_order and coefficients must have the same length")
        if not isinstance(imputation_values, dict):
            raise InvalidModelMetadataError("model metadata must include imputation_values")
        try:
            return cls(
                model_version=str(payload["model_version"]),
                feature_order=list(feature_order),
                coefficients=[float(value) for value in coefficients],
                intercept=float(payload["intercept"]),
                threshold=float(payload.get("threshold", 0.5)),
                imputation_values={feature: float(imputation_values[feature]) for feature in feature_order},
            )
        except KeyError as exc:
            raise InvalidModelMetadataError(f"model metadata missing required field: {exc.args[0]}") from exc
        except (TypeError, ValueError) as exc:
            raise InvalidModelMetadataError("model metadata contains non-numeric model values") from exc

    def predict(self, features: Mapping[str, Any]) -> ManualPredictionResult:
        values: list[float] = []
        missing_features_filled: list[str] = []
        for feature_name in self.feature_order:
            if feature_name in features and features[feature_name] is not None:
                raw_value = features[feature_name]
            else:
                raw_value = self.imputation_values[feature_name]
                missing_features_filled.append(feature_name)
            values.append(_to_float(feature_name, raw_value))

        z_value = self.intercept + sum(value * coefficient for value, coefficient in zip(values, self.coefficients))
        prediction_score = _sigmoid(z_value)
        prediction_label = int(prediction_score >= self.threshold)
        return ManualPredictionResult(
            prediction_score=prediction_score,
            prediction_label=prediction_label,
            decision="likely_to_buy" if prediction_label == 1 else "not_likely_to_buy",
            model_version=self.model_version,
            missing_features_filled=missing_features_filled,
            used_feature_count=len(self.feature_order),
        )


def _to_float(feature_name: str, value: Any) -> float:
    try:
        numeric_value = float(value)
    except (TypeError, ValueError) as exc:
        raise InvalidFeatureValueError(f"feature '{feature_name}' must be numeric") from exc
    if not math.isfinite(numeric_value):
        raise InvalidFeatureValueError(f"feature '{feature_name}' must be finite")
    return numeric_value


def _sigmoid(value: float) -> float:
    if value >= 0:
        exponent = math.exp(-value)
        return 1.0 / (1.0 + exponent)
    exponent = math.exp(value)
    return exponent / (1.0 + exponent)
