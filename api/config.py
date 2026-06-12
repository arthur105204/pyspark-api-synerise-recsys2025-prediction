"""Configuration helpers for the purchase propensity API."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCORE_DB_PATH = "data/serving/purchase_propensity_scores.sqlite"
DEFAULT_MODEL_VERSION = "baseline_lr_v1"


@dataclass(frozen=True)
class ApiSettings:
    score_db_path: str = DEFAULT_SCORE_DB_PATH
    model_version: str = DEFAULT_MODEL_VERSION
    service_name: str = "purchase-propensity-api"
    task: str = "purchase_propensity"
    target_window_days: int = 30

    @property
    def resolved_score_db_path(self) -> Path:
        path = Path(self.score_db_path)
        if path.is_absolute():
            return path
        return PROJECT_ROOT / path


def get_settings() -> ApiSettings:
    return ApiSettings(
        score_db_path=os.getenv("SCORE_DB_PATH", DEFAULT_SCORE_DB_PATH),
        model_version=os.getenv("API_MODEL_VERSION", DEFAULT_MODEL_VERSION),
    )
