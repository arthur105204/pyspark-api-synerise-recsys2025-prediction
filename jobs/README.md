# Jobs

This folder contains small project jobs that can be run locally during each milestone.

## `00_inspect_raw_data.py`

This job inspects the raw files under `data/raw/` before EDA, preprocessing, modeling, or API work.

It does the following:

- starts a PySpark `SparkSession` in local mode when PySpark is available
- lists discovered raw files and file sizes
- infers file format from file extensions
- tries to read Spark-supported files such as CSV, JSON, and Parquet
- handles `.gz` files carefully
- detects `.tar.gz` archives and reports that archive extraction or member-specific handling is needed
- lists archive member names, sizes, inferred formats, and whether each member is a Spark-readable candidate
- prints schemas for readable files
- skips expensive row counts for large files
- writes metadata to `artifacts/metadata/raw_data_summary.json`

The job does not modify files under `data/raw/`.

## How to Run

From the project root:

```powershell
python jobs/00_inspect_raw_data.py
```

If your environment uses a specific Python executable, run the same script with that executable.

## Expected Output

The terminal output will show:

- each discovered file
- file size
- inferred format
- whether Spark could read it
- schema when available
- row count when the file is small enough to count safely
- a clear error or note when a file is not directly readable

The JSON summary is written to:

```text
artifacts/metadata/raw_data_summary.json
```

## Metadata Privacy

The inspection job writes sanitized metadata only. It should not persist absolute local paths, raw row samples, client ids, query text, product names, or environment-specific details.

## Interpreting `.gz` Files

Spark can often read files like `.csv.gz` or `.json.gz` directly because the underlying format is clear.

A `.tar.gz` file is different. It is an archive that may contain many files. Spark does not read a `.tar.gz` archive directly as a table, so the next step is to inspect the archive members and then extract or stream the specific inner files needed for Spark ingestion.

## Before Moving to EDA

Review:

- whether the raw package contains Spark-readable files
- which inner tables are event logs and which are metadata
- whether schemas and row counts are available
- whether the next step should extract all Parquet files or only selected files needed for the MVP

## `01_eda_summary.py`

This job runs Phase 1 EDA on the Spark-readable Parquet files under `data/raw/synerise_dataset/`.

It produces sanitized table-level summaries only:

- table schemas
- file sizes
- Spark readability status
- row count or row count status
- approximate distinct counts for key columns
- null counts for important columns
- timestamp parse success/failure counts
- timestamp min/max
- product metadata price summary

It does not write raw row samples, actual client ids, query text, or product names.

### How to Run

Safe mode is the default:

```powershell
python jobs/01_eda_summary.py
python jobs/01_eda_summary.py --safe-mode
```

Full-count mode computes exact row counts for all readable tables and may take longer:

```powershell
python jobs/01_eda_summary.py --full-count
```

If `python` is not available on PATH, use the project environment's Python executable with the same script and arguments.

### EDA Outputs

The job writes:

```text
artifacts/eda/eda_summary.json
artifacts/eda/table_overview.csv
artifacts/eda/event_table_overview.csv
artifacts/eda/product_table_overview.csv
artifacts/eda/column_overview.csv
```

Output meaning:

- `eda_summary.json`: nested table-level EDA metadata.
- `table_overview.csv`: compact general table metadata.
- `event_table_overview.csv`: event-table metrics for user activity tables.
- `product_table_overview.csv`: product metadata metrics.
- `column_overview.csv`: one row per column with null and distinct-count metadata where applicable.

Before moving to preprocessing or feature engineering, review whether core columns are reliable, timestamps parse cleanly, purchase events support a label, and product metadata is useful for later features.

## `01b_target_feasibility_eda.py`

This job runs Phase 1.1 business target selection EDA for the first MVP scoring target.

It produces aggregate-only summaries:

- purchase date range
- candidate target windows
- purchase frequency per purchasing client
- business target comparison for purchase propensity, cart conversion, and purchase-based churn
- candidate churn/non-churn balance
- active cohort comparison
- leakage prevention notes

It does not create final labels, feature tables, client-level outputs, raw row samples, actual client ids, query text, or product names.

### How to Run

```powershell
python jobs/01b_target_feasibility_eda.py
```

### Business Target Selection Outputs

The job writes:

```text
artifacts/eda/business_target_selection_summary.json
artifacts/eda/business_target_comparison.csv
artifacts/eda/target_feasibility_summary.json
artifacts/eda/churn_window_balance.csv
artifacts/eda/purchase_frequency_summary.csv
artifacts/eda/active_cohort_comparison.csv
artifacts/eda/target_feasibility_notes.md
```

Before moving to preprocessing, review the recommended MVP target, target window, eligible cohort definition, positive rate, repeat-purchase behavior, and leakage rules.
