from __future__ import annotations

import math

import pytest

from api.model_scoring import (
    InvalidFeatureValueError,
    LightweightLogisticRegressionModel,
    ModelMetadataNotFoundError,
)


def fake_payload() -> dict[str, object]:
    return {
        "model_version": "fake_lr_v1",
        "feature_order": ["feature_a", "feature_b"],
        "coefficients": [0.5, -0.2],
        "intercept": 0.1,
        "threshold": 0.5,
        "imputation_values": {"feature_a": 0.0, "feature_b": 1.0},
    }


def test_sigmoid_scoring_correctness() -> None:
    model = LightweightLogisticRegressionModel.from_payload(fake_payload())
    result = model.predict({"feature_a": 2.0, "feature_b": 1.0})
    expected_score = 1 / (1 + math.exp(-0.9))
    assert result.prediction_score == pytest.approx(expected_score)
    assert result.prediction_label == 1
    assert result.decision == "likely_to_buy"


def test_missing_feature_fill() -> None:
    model = LightweightLogisticRegressionModel.from_payload(fake_payload())
    result = model.predict({"feature_a": 0.0})
    expected_score = 1 / (1 + math.exp(-(-0.1)))
    assert result.prediction_score == pytest.approx(expected_score)
    assert result.prediction_label == 0
    assert result.decision == "not_likely_to_buy"
    assert result.missing_features_filled == ["feature_b"]
    assert result.used_feature_count == 2


def test_prediction_output_bounds_and_label_values() -> None:
    model = LightweightLogisticRegressionModel.from_payload(fake_payload())
    result = model.predict({})
    assert 0 <= result.prediction_score <= 1
    assert result.prediction_label in {0, 1}
    assert result.decision in {"likely_to_buy", "not_likely_to_buy"}


def test_invalid_feature_value_raises_clear_error() -> None:
    model = LightweightLogisticRegressionModel.from_payload(fake_payload())
    with pytest.raises(InvalidFeatureValueError, match="feature_a"):
        model.predict({"feature_a": "not_numeric"})


def test_missing_model_metadata_file_raises_clear_error(tmp_path) -> None:
    with pytest.raises(ModelMetadataNotFoundError):
        LightweightLogisticRegressionModel.from_file(tmp_path / "missing.json")
