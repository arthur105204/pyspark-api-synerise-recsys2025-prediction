# PySpark Customer Behavior Scoring API

This project builds a PySpark-based customer behavior scoring pipeline from the Synerise RecSys 2025 dataset. The intended final flow is raw event logs, PySpark processing, EDA, feature engineering, simple model training, batch prediction output, and an API that can return prediction results for a user or client id.

Current milestone: Phase 1.1: Business Target Selection EDA.

## Folder Structure

```text
data/raw/synerise_dataset/        Raw Parquet dataset files, not committed to git
docs/project_report.md            Mentor-facing project report
jobs/00_inspect_raw_data.py        Raw data inspection job
jobs/01_eda_summary.py             Phase 1 EDA summary job
jobs/01b_target_feasibility_eda.py Phase 1.1 business target selection EDA job
jobs/README.md                    Job usage notes
artifacts/metadata/               Small generated metadata summaries
artifacts/eda/                    Generated EDA summaries
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

## Current Status

- The project direction is documented as customer behavior scoring from event logs.
- Raw data inspection and Phase 1 EDA summary jobs are implemented.
- Modeling and API code are intentionally not included in this milestone.
- Raw data and large generated outputs are ignored by git.

## Agent Instructions

This repo includes `AGENTS.md` and a small set of Codex skills for data privacy, PySpark job structure, and git safety. These guardrails are intended to keep AI-assisted development scoped, reviewable, and safe.

## Next Step

Review the EDA and business target selection findings, confirm the provisional purchase propensity target direction, then decide whether preprocessing can begin.
