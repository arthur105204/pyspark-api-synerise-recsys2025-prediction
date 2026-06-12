from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from api.config import ApiSettings
from api.main import app, get_settings
from tests.test_serving_repository import create_test_db


def make_client(tmp_path: Path) -> TestClient:
    db_path = tmp_path / "scores.sqlite"
    create_test_db(db_path)

    def override_settings() -> ApiSettings:
        return ApiSettings(score_db_path=str(db_path), model_version="baseline_lr_v1")

    app.dependency_overrides[get_settings] = override_settings
    return TestClient(app)


def test_health_endpoint(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    try:
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok", "service": "purchase-propensity-api"}
    finally:
        app.dependency_overrides.clear()


def test_metadata_endpoint(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    try:
        response = client.get("/metadata")
        assert response.status_code == 200
        payload = response.json()
        assert payload["task"] == "purchase_propensity"
        assert payload["target_window_days"] == 30
        assert payload["model_version"] == "baseline_lr_v1"
        assert payload["score_source"] == "batch_scoring"
        assert payload["api_mode"] == "lookup"
    finally:
        app.dependency_overrides.clear()


def test_score_lookup_success(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    try:
        response = client.get("/scores/client_test_001")
        assert response.status_code == 200
        payload = response.json()
        assert payload["client_id"] == "client_test_001"
        assert 0 <= payload["prediction_score"] <= 1
        assert payload["prediction_label"] in {0, 1}
        assert payload["model_version"] == "baseline_lr_v1"
        assert payload["scored_at"]
    finally:
        app.dependency_overrides.clear()


def test_score_lookup_missing_client(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    try:
        response = client.get("/scores/client_missing")
        assert response.status_code == 404
        assert response.json() == {"detail": "client_id not found"}
    finally:
        app.dependency_overrides.clear()


def test_score_lookup_missing_database_returns_service_error(tmp_path: Path) -> None:
    missing_db_path = tmp_path / "missing.sqlite"

    def override_settings() -> ApiSettings:
        return ApiSettings(score_db_path=str(missing_db_path), model_version="baseline_lr_v1")

    app.dependency_overrides[get_settings] = override_settings
    client = TestClient(app)
    try:
        response = client.get("/scores/client_test_001")
        assert response.status_code == 503
        assert response.json() == {"detail": "score database is not available"}
    finally:
        app.dependency_overrides.clear()
