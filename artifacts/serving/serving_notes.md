# Serving Notes

This artifact contains sanitized Phase 7A serving notes.

Phase 7A implements a batch score lookup API by `client_id`. The API reads from a local SQLite database exported from Phase 6 batch scores. The API request path does not run Spark or Spark ML model inference.

The SQLite database is generated locally under `data/serving/` and is excluded from git. The current Windows runtime could not complete the export because Spark failed while reading the local Parquet score output. The limited demo export succeeded in WSL with 100000 rows:

```bash
python jobs/07_export_serving_scores.py --limit 100000
```

Tests use temporary SQLite data with fake client ids only. No real client IDs, row-level real scores, raw query text, or product names are committed.

Phase 7B manual feature-input prediction, Phase 7C demo UI, and Phase 8 experiment tracking/tuning are planned but not implemented in Phase 7A.
