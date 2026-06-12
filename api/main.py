"""FastAPI lookup service for purchase propensity scores."""

from __future__ import annotations

from fastapi import Depends, FastAPI, HTTPException

from api.config import ApiSettings, get_settings
from api.model_scoring import (
    InvalidFeatureValueError,
    InvalidModelMetadataError,
    LightweightLogisticRegressionModel,
    ModelMetadataNotFoundError,
)
from api.repository import ScoreDatabaseNotFoundError, ScoreRepository
from api.schemas import HealthResponse, ManualPredictionRequest, ManualPredictionResponse, MetadataResponse, ScoreResponse


app = FastAPI(title="Purchase Propensity API", version="0.1.0")


def get_repository(settings: ApiSettings = Depends(get_settings)) -> ScoreRepository:
    return ScoreRepository(settings.resolved_score_db_path)


def get_manual_model(settings: ApiSettings = Depends(get_settings)) -> LightweightLogisticRegressionModel:
    try:
        return LightweightLogisticRegressionModel.from_file(settings.resolved_model_metadata_path)
    except ModelMetadataNotFoundError as exc:
        raise HTTPException(status_code=503, detail="model metadata is not available") from exc
    except InvalidModelMetadataError as exc:
        raise HTTPException(status_code=503, detail="model metadata is invalid") from exc


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
        api_mode="lookup_and_manual_prediction",
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


@app.post("/predict", response_model=ManualPredictionResponse)
def predict(
    request: ManualPredictionRequest,
    model: LightweightLogisticRegressionModel = Depends(get_manual_model),
) -> ManualPredictionResponse:
    try:
        result = model.predict(request.features)
    except InvalidFeatureValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return ManualPredictionResponse(
        prediction_score=result.prediction_score,
        prediction_label=result.prediction_label,
        decision=result.decision,
        model_version=result.model_version,
        missing_features_filled=result.missing_features_filled,
        used_feature_count=result.used_feature_count,
    )
