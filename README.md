# PySpark Customer Behavior Scoring API

This project builds a PySpark-based customer behavior scoring pipeline from the Synerise RecSys 2025 dataset. The intended final flow is raw event logs, PySpark processing, EDA, feature engineering, simple model training, batch prediction output, and a demo-ready serving layer that can return prediction results from batch scores or from manually entered feature values.

Current milestone: Phase 7: API Serving & Demo Interface.

## Folder Structure

```text
data/raw/synerise_dataset/        Raw Parquet dataset files, not committed to git
docs/project_report.md            Mentor-facing project report
jobs/00_inspect_raw_data.py        Raw data inspection job
jobs/01_eda_summary.py             Phase 1 EDA summary job
jobs/01b_target_feasibility_eda.py Phase 1.1 business target selection EDA job
jobs/02_preprocess_events.py       Phase 2 preprocessing job
jobs/03_build_features.py          Phase 3 feature engineering job
jobs/04_build_labels.py            Phase 4 label construction job
jobs/05_train_baseline_model.py    Phase 5 baseline modeling job
jobs/06_batch_score.py             Phase 6 batch scoring job
jobs/README.md                    Job usage notes
artifacts/metadata/               Small generated metadata summaries
artifacts/eda/                    Generated EDA summaries
artifacts/preprocessing/          Generated preprocessing validation summaries
artifacts/features/               Generated feature validation summaries
artifacts/labels/                 Generated label validation summaries
artifacts/modeling/               Generated aggregate modeling summaries
artifacts/scoring/                Generated aggregate scoring summaries
configs/pipeline_config.yaml      Pipeline target and path configuration
```

## Run the First Inspection Job

From the project root:

```powershell
python jobs/00_inspect_raw_data.py
```

The job writes:

```text
artifacts/metadata/raw_data_summary.json
```

## Run the EDA Job

Safe mode:

```powershell
python jobs/01_eda_summary.py
python jobs/01_eda_summary.py --safe-mode
```

Full-count mode:

```powershell
python jobs/01_eda_summary.py --full-count
```

The EDA job writes:

```text
artifacts/eda/eda_summary.json
artifacts/eda/table_overview.csv
artifacts/eda/event_table_overview.csv
artifacts/eda/product_table_overview.csv
artifacts/eda/column_overview.csv
```

Artifact meaning:

- `table_overview.csv`: compact general table metadata.
- `event_table_overview.csv`: user event table metrics.
- `product_table_overview.csv`: product metadata metrics.
- `column_overview.csv`: column-level metadata.
- `eda_summary.json`: full structured summary.
- `business_target_comparison.csv`: aggregate comparison of candidate MVP targets.
- `business_target_selection_summary.json`: structured target selection summary.

## Run Business Target Selection EDA

```powershell
python jobs/01b_target_feasibility_eda.py
```

This job compares purchase propensity, cart conversion, and purchase-based churn as candidate MVP scoring targets. It does not create final training labels.

## Run Preprocessing

Default preprocessing:

```powershell
python jobs/02_preprocess_events.py
```

Optional search preprocessing:

```powershell
python jobs/02_preprocess_events.py --include-search
```

The preprocessing job writes Spark Parquet outputs under:

```text
data/processed/events/add_to_cart/
data/processed/events/remove_from_cart/
data/processed/events/product_buy/
data/processed/product_properties_clean/
```

If optional search preprocessing is enabled, it also writes:

```text
data/processed/events/search_query/
```

Preprocessing artifacts are written to:

```text
artifacts/preprocessing/preprocessing_summary.json
artifacts/preprocessing/table_validation.csv
artifacts/preprocessing/duplicate_check_summary.csv
artifacts/preprocessing/product_metadata_validation.csv
artifacts/preprocessing/preprocessing_notes.md
```

`data/processed/` is ignored by git and should not be committed.

## Run Feature Engineering

Default feature engineering:

```powershell
python jobs/03_build_features.py
```

Feature engineering with explicit search features:

```powershell
python jobs/03_build_features.py --include-search
```

The feature job writes a Spark Parquet user-level feature table under:

```text
data/processed/features/user_behavior_features/
```

Feature artifacts are written to:

```text
artifacts/features/feature_summary.json
artifacts/features/feature_catalog.csv
artifacts/features/feature_validation.csv
artifacts/features/feature_notes.md
```

This phase creates features only. It does not create final labels, train models, create prediction outputs, or implement API serving.

## Run Label Construction

```powershell
python jobs/04_build_labels.py
```

The label job writes a Spark Parquet label table under:

```text
data/processed/labels/purchase_propensity_30d/
```

It also writes a training-ready dataset under:

```text
data/processed/training/purchase_propensity_30d/
```

Label artifacts are written to:

```text
artifacts/labels/label_summary.json
artifacts/labels/label_validation.csv
artifacts/labels/training_dataset_validation.csv
artifacts/labels/label_notes.md
```

This phase creates supervised labels and a training-ready dataset only. It does not train a model, create predictions, or implement API serving.

## Run Baseline Modeling

```powershell
python jobs/05_train_baseline_model.py
```

With explicit config:

```powershell
python jobs/05_train_baseline_model.py --config configs/pipeline_config.yaml
```

The baseline modeling job reads:

```text
data/processed/training/purchase_propensity_30d/
```

It writes the Spark ML model locally under:

```text
data/models/purchase_propensity_baseline/
```

Modeling artifacts are written to:

```text
artifacts/modeling/baseline_model_summary.json
artifacts/modeling/baseline_metrics.csv
artifacts/modeling/topk_metrics.csv
artifacts/modeling/feature_processing_summary.csv
artifacts/modeling/baseline_model_notes.md
```

`data/models/` is ignored by git and should not be committed. This phase trains and evaluates a baseline model only. It does not implement API serving or production batch scoring.

## Run Batch Scoring

```powershell
python jobs/06_batch_score.py
```

With explicit config:

```powershell
python jobs/06_batch_score.py --config configs/pipeline_config.yaml
```

The batch scoring job reads:

```text
data/processed/features/user_behavior_features/
data/models/purchase_propensity_baseline/
```

It writes the local score table under:

```text
data/processed/scoring/purchase_propensity_scores/
```

Scoring artifacts are written to:

```text
artifacts/scoring/scoring_summary.json
artifacts/scoring/score_distribution.csv
artifacts/scoring/scoring_validation.csv
artifacts/scoring/scoring_notes.md
```

`data/processed/` and `data/models/` are ignored by git and should not be committed. This phase creates batch scores only. It does not implement API serving or an online inference endpoint.

## Run API Serving / Lookup Layer

Phase 7A exports batch scores to a local SQLite lookup store. The current Windows runtime fails while Spark reads the local Parquet score output, while the WSL limited demo export succeeded with 100000 rows:

```powershell
python jobs/07_export_serving_scores.py --limit 100000
```

Run the API:

```powershell
uvicorn api.main:app --reload
```

Run API tests:

```powershell
python -m pytest tests/test_api_scores.py tests/test_serving_repository.py
```

The API lookup store is:

```text
data/serving/purchase_propensity_scores.sqlite
```

`data/serving/` is ignored by git and should not be committed. The API is a lookup layer over exported batch scores; it does not run Spark or model inference per request.

## Roadmap

Completed phases:

- Phase 0 raw inspection
- Phase 1 general EDA
- Phase 1.1 business target selection
- Phase 2 preprocessing
- Phase 3 feature engineering
- Phase 4 label construction
- Phase 5 baseline modeling
- Phase 6 batch scoring

Upcoming phases:

- Phase 7 API Serving & Demo Interface: Phase 7A supports batch score lookup by `client_id`; Phase 7B direct manual feature-input prediction and Phase 7C demo UI are planned next.
- Phase 8 Experiment Tracking & Hyperparameter Tuning: train model variants offline with Spark, write sanitized aggregate experiment artifacts, and compare metrics and parameters in a reviewable dashboard or report view.
- Phase 9 Load/Stress Testing, Final Report, and Demo Packaging: test API lookup performance, document the architecture, and prepare the final mentor demo flow.

The next phase is not only an API lookup endpoint. The demo plan includes both `client_id` lookup and manual feature input. Training and tuning remain offline jobs and should not run inside API request paths or default UI actions.

## Current Status

- The project direction is documented as customer behavior scoring from event logs.
- Raw inspection, EDA, business target selection, preprocessing, feature engineering, label construction, baseline modeling, and batch scoring are implemented.
- The current MVP target is purchase propensity with a 30-day target window.
- Phase 4 label construction has generated aggregate validation artifacts and local processed Spark Parquet outputs.
- Phase 5 baseline modeling has generated a local Spark ML model and sanitized aggregate modeling artifacts.
- Phase 6 batch scoring has generated a local score table and sanitized aggregate scoring artifacts.
- Phase 7A lookup API scaffold, fake-data tests, and WSL limited demo SQLite export are implemented.
- Raw data and large generated outputs are ignored by git.

## Agent Instructions

This repo includes `AGENTS.md` and a small set of Codex skills for data privacy, PySpark job structure, and git safety. These guardrails are intended to keep AI-assisted development scoped, reviewable, and safe.

## Next Step

Review the Phase 7A lookup API against the generated SQLite database before starting Phase 7B manual feature-input prediction or Phase 7C demo UI.
