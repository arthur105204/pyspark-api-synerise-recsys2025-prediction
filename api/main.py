"""FastAPI lookup service for purchase propensity scores."""

from __future__ import annotations

from fastapi import Depends, FastAPI, HTTPException

from api.config import ApiSettings, get_settings
from api.repository import ScoreDatabaseNotFoundError, ScoreRepository
from api.schemas import HealthResponse, MetadataResponse, ScoreResponse


app = FastAPI(title="Purchase Propensity API", version="0.1.0")


def get_repository(settings: ApiSettings = Depends(get_settings)) -> ScoreRepository:
    return ScoreRepository(settings.resolved_score_db_path)


@app.get("/health", response_model=HealthResponse)
def health(settings: ApiSettings = Depends(get_settings)) -> HealthResponse:
    return HealthResponse(status="ok", service=settings.service_name)


@app.get("/metadata", response_model=MetadataResponse)
def metadata(settings: ApiSettings = Depends(get_settings)) -> MetadataResponse:
    return MetadataResponse(
        task=settings.task,
        target_window_days=settings.target_window_days,
        model_version=settings.model_version,
        score_source="batch_scoring",
        api_mode="lookup",
    )


@app.get("/scores/{client_id}", response_model=ScoreResponse)
def get_score(client_id: str, repository: ScoreRepository = Depends(get_repository)) -> ScoreResponse:
    try:
        record = repository.get_score(client_id)
    except ScoreDatabaseNotFoundError as exc:
        raise HTTPException(status_code=503, detail="score database is not available") from exc
    if record is None:
        raise HTTPException(status_code=404, detail="client_id not found")
    return ScoreResponse(
        client_id=record.client_id,
        prediction_score=record.prediction_score,
        prediction_label=record.prediction_label,
        model_version=record.model_version,
        scored_at=record.scored_at,
    )
