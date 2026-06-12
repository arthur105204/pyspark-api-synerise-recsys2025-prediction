# Serving Notes

This artifact contains sanitized Phase 7A serving notes.

Phase 7A implements a batch score lookup API by `client_id`. The API reads from a local SQLite database exported from Phase 6 batch scores. The API request path does not run Spark or Spark ML model inference.

The SQLite database is generated locally under `data/serving/` and is excluded from git. The current Windows runtime could not complete the score export because Spark failed while reading the local Parquet score output. The WSL/Linux demo export contains 500000 rows:

```bash
python jobs/07_export_serving_scores.py --limit 500000
```

Phase 7B implements manual feature-input prediction through `POST /predict`. The API reads lightweight Logistic Regression metadata from `data/serving/model_metadata/baseline_lr_v1.json`, which is local serving data and excluded from git. The API fills missing features with exported imputation values and computes the logistic sigmoid directly without Spark.

Tests use temporary SQLite data and fake model metadata only. No real client IDs, row-level real scores, raw feature rows, raw query text, or product names are committed.

Phase 7C demo UI and Phase 8 experiment tracking/tuning are planned but not implemented.
