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
