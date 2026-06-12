# Purchase Propensity API

This Phase 7A API serves purchase propensity scores from a local SQLite lookup store created from batch scoring output.

## Run Locally

```bash
uvicorn api.main:app --reload
```

Configuration uses safe defaults:

```text
SCORE_DB_PATH=data/serving/purchase_propensity_scores.sqlite
API_MODEL_VERSION=baseline_lr_v1
```

Do not commit `.env` files or the generated SQLite database.

The local Windows Spark runtime may fail while reading the Phase 6 Parquet score output. The limited demo export was completed in WSL with 100000 rows.

## Endpoints

```http
GET /health
```

```json
{
  "status": "ok",
  "service": "purchase-propensity-api"
}
```

```http
GET /metadata
```

```json
{
  "task": "purchase_propensity",
  "target_window_days": 30,
  "model_version": "baseline_lr_v1",
  "score_source": "batch_scoring",
  "api_mode": "lookup"
}
```

```http
GET /scores/client_test_001
```

```json
{
  "client_id": "client_test_001",
  "prediction_score": 0.8234,
  "prediction_label": 1,
  "model_version": "baseline_lr_v1",
  "scored_at": "2026-06-12T00:00:00+00:00"
}
```

Missing clients return:

```json
{
  "detail": "client_id not found"
}
```

## Limitations

The API is a local lookup MVP. It does not run Spark or model inference per request. The SQLite database is generated locally from batch scores and is ignored by git.

Manual feature-input prediction and a Streamlit/demo UI are planned later. They are not implemented in Phase 7A.
