# PySpark Customer Behavior Scoring API

This project builds a PySpark-based customer behavior scoring pipeline from the Synerise RecSys 2025 dataset. The intended final flow is raw event logs, PySpark processing, EDA, feature engineering, simple model training, batch prediction output, and an API that can return prediction results for a user or client id.

Current milestone: project foundation and raw data inspection.

## Folder Structure

```text
data/raw/synerise_dataset/        Raw Parquet dataset files, not committed to git
docs/project_report.md            Mentor-facing project report
jobs/00_inspect_raw_data.py        Raw data inspection job
jobs/README.md                    Job usage notes
artifacts/metadata/               Small generated metadata summaries
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

## Current Status

- The project direction is documented as customer behavior scoring from event logs.
- Raw data inspection is the only implemented job.
- Modeling and API code are intentionally not included in this milestone.
- Raw data and large generated outputs are ignored by git.

## Next Step

Run the inspection job, review the verified schemas and row-count status, then decide whether churn prediction or propensity scoring should be the first modeling task.
