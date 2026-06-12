from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from api.config import ApiSettings
from api.main import app, get_settings
from tests.test_serving_repository import create_test_db


def write_fake_metadata(path: Path) -> None:
    payload = {
        "model_version": "fake_lr_v1",
        "feature_order": ["feature_a", "feature_b"],
        "coefficients": [0.5, -0.2],
        "intercept": 0.1,
        "threshold": 0.5,
        "imputation_values": {"feature_a": 0.0, "feature_b": 1.0},
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def make_client(tmp_path: Path, include_metadata: bool = True) -> TestClient:
    db_path = tmp_path / "scores.sqlite"
    metadata_path = tmp_path / "model_metadata.json"
    create_test_db(db_path)
    if include_metadata:
        write_fake_metadata(metadata_path)

    def override_settings() -> ApiSettings:
        return ApiSettings(
            score_db_path=str(db_path),
            model_metadata_path=str(metadata_path),
            model_version="fake_lr_v1",
        )

    app.dependency_overrides[get_settings] = override_settings
    return TestClient(app)


def test_manual_prediction_success(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    try:
        response = client.post("/predict", json={"features": {"feature_a": 2, "feature_b": 1}})
        assert response.status_code == 200
        payload = response.json()
        assert 0 <= payload["prediction_score"] <= 1
        assert payload["prediction_label"] in {0, 1}
        assert payload["decision"] == "likely_to_buy"
        assert payload["model_version"] == "fake_lr_v1"
        assert payload["missing_features_filled"] == []
        assert payload["used_feature_count"] == 2
    finally:
        app.dependency_overrides.clear()


def test_manual_prediction_fills_missing_features(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    try:
        response = client.post("/predict", json={"features": {"feature_a": 0}})
        assert response.status_code == 200
        payload = response.json()
        assert payload["decision"] == "not_likely_to_buy"
        assert payload["missing_features_filled"] == ["feature_b"]
    finally:
        app.dependency_overrides.clear()


def test_manual_prediction_missing_metadata_returns_service_error(tmp_path: Path) -> None:
    client = make_client(tmp_path, include_metadata=False)
    try:
        response = client.post("/predict", json={"features": {"feature_a": 1}})
        assert response.status_code == 503
        assert response.json() == {"detail": "model metadata is not available"}
    finally:
        app.dependency_overrides.clear()


def test_manual_prediction_invalid_feature_returns_validation_error(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    try:
        response = client.post("/predict", json={"features": {"feature_a": "bad_value"}})
        assert response.status_code == 422
        assert "feature_a" in response.json()["detail"]
    finally:
        app.dependency_overrides.clear()
