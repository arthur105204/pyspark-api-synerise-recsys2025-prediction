# Purchase Propensity API

This Phase 7 API serves purchase propensity scores in two lightweight modes:

- Phase 7A: lookup by `client_id` from a local SQLite store created from batch scoring output.
- Phase 7B: manual feature-input prediction from exported Logistic Regression metadata.

## Run Locally

```bash
uvicorn api.main:app --reload
```

Configuration uses safe defaults:

```text
SCORE_DB_PATH=data/serving/purchase_propensity_scores.sqlite
MODEL_METADATA_PATH=data/serving/model_metadata/baseline_lr_v1.json
API_MODEL_VERSION=baseline_lr_v1
```

Do not commit `.env` files, the generated SQLite database, or local model metadata under `data/serving/`.

The local Windows Spark runtime may fail while reading the Phase 6 Parquet score output. The local demo serving DB contains 500000 rows; use WSL/Linux or a properly configured Spark runtime to refresh it:

```bash
python jobs/07_export_serving_scores.py --limit 500000
```

Export lightweight manual prediction metadata:

```bash
python jobs/07_export_model_metadata.py
```

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
  "api_mode": "lookup_and_manual_prediction"
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

```http
POST /predict
```

Example request with fake values:

```json
{
  "features": {
    "add_to_cart_count": 5,
    "product_buy_count": 1
  }
}
```

Example response:

```json
{
  "prediction_score": 0.72,
  "prediction_label": 1,
  "decision": "likely_to_buy",
  "model_version": "baseline_lr_v1",
  "missing_features_filled": ["search_query_count"],
  "used_feature_count": 36
}
```

## Limitations

The API is a local serving MVP. It does not run Spark or Spark ML inference per request. The SQLite database and lightweight model metadata are generated locally under `data/serving/` and are ignored by git.

Streamlit/demo UI is planned later. It is not implemented in Phase 7B.
