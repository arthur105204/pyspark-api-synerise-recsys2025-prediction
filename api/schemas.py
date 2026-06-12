"""Pydantic schemas for API responses."""

from __future__ import annotations

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str
    service: str


class ScoreResponse(BaseModel):
    client_id: str
    prediction_score: float = Field(ge=0.0, le=1.0)
    prediction_label: int = Field(ge=0, le=1)
    model_version: str
    scored_at: str


class ManualPredictionRequest(BaseModel):
    features: dict[str, float | int | str | None] = Field(default_factory=dict)


class ManualPredictionResponse(BaseModel):
    prediction_score: float = Field(ge=0.0, le=1.0)
    prediction_label: int = Field(ge=0, le=1)
    decision: str
    model_version: str
    missing_features_filled: list[str]
    used_feature_count: int


class MetadataResponse(BaseModel):
    task: str
    target_window_days: int
    model_version: str
    score_source: str
    api_mode: str
