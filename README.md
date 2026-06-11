# PySpark Customer Behavior Scoring API

This project builds a PySpark-based customer behavior scoring pipeline from the Synerise RecSys 2025 dataset. The intended final flow is raw event logs, PySpark processing, EDA, feature engineering, simple model training, batch prediction output, and an API that can return prediction results for a user or client id.

Current milestone: Phase 2: Preprocessing.

## Folder Structure

```text
data/raw/synerise_dataset/        Raw Parquet dataset files, not committed to git
docs/project_report.md            Mentor-facing project report
jobs/00_inspect_raw_data.py        Raw data inspection job
jobs/01_eda_summary.py             Phase 1 EDA summary job
jobs/01b_target_feasibility_eda.py Phase 1.1 business target selection EDA job
jobs/02_preprocess_events.py       Phase 2 preprocessing job
jobs/README.md                    Job usage notes
artifacts/metadata/               Small generated metadata summaries
artifacts/eda/                    Generated EDA summaries
artifacts/preprocessing/          Generated preprocessing validation summaries
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

## Current Status

- The project direction is documented as customer behavior scoring from event logs.
- Raw inspection, EDA, business target selection, and preprocessing jobs are implemented.
- The current MVP target remains purchase propensity with a provisional 30-day target window.
- Feature engineering, final labels, modeling, batch scoring, and API code are intentionally not included in this milestone.
- Raw data and large generated outputs are ignored by git.

## Agent Instructions

This repo includes `AGENTS.md` and a small set of Codex skills for data privacy, PySpark job structure, and git safety. These guardrails are intended to keep AI-assisted development scoped, reviewable, and safe.

## Next Step

Review the preprocessing validation artifacts, then decide whether Phase 3 feature engineering can begin.
