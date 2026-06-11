---
name: pyspark-job
description: Use when creating or modifying PySpark scripts under jobs/ or any Spark-based data pipeline code. Enforces clear goal, inputs, outputs, guardrails, safe large-table behavior, and reproducible artifacts.
---

# PySpark Job Skill

## Purpose

Create PySpark jobs that are understandable, reproducible, safe for large data, and easy to report.

## Apply This Skill When

- Creating a new script under `jobs/`.
- Modifying an existing Spark job.
- Reading raw Parquet/CSV/JSON data.
- Producing metadata, EDA, preprocessing, feature, label, or scoring artifacts.

## Required Job Structure

Each job should clearly define:

- Goal
- Input paths
- Output paths
- CLI arguments if needed
- SparkSession setup
- Main transformations/actions
- Error handling
- Artifact writing
- Terminal summary

## Guardrails

- Use Spark local mode first unless explicitly instructed otherwise.
- Do not modify raw data.
- Do not collect raw rows to driver.
- Do not persist raw row samples.
- Do not call expensive actions accidentally.
- Treat `count()`, `collect()`, `show()`, and `write()` as Spark actions.
- Large-table counts must be deliberate:
  - default safe mode, or
  - explicit `--full-count`, or
  - documented EDA strategy.
- Handle errors per table/file where possible.
- Do not silently skip files.
- Do not add modeling/API logic to EDA or inspection jobs.

## Output Artifact Rules

Artifacts must be structured and sanitized.

Preferred formats:

- JSON for nested metadata.
- CSV for tables/overviews.
- Markdown only for human-facing summaries.

Before writing artifacts, apply the `data-privacy` rules.

## Documentation Update

When a job is added or changed, update:

- `jobs/README.md` with command and output.
- `docs/project_report.md` only with mentor-facing findings if relevant.
- `PLAN.md` status if the phase changes.

## Verification Checklist

Before finishing:

- The job runs locally or has a clearly documented reason why not.
- Output directories are created automatically.
- Outputs are written to the expected locations.
- Large table behavior is controlled.
- No raw row-level data is persisted.
- Errors are clear and neutral.
- The script can be rerun safely.

## Definition of Done

A PySpark job is done when it has clear inputs/outputs, controlled Spark actions, sanitized artifacts, documented run commands, and no scope creep into later phases.
