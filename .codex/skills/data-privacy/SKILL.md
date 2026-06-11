---
name: data-privacy
description: Use when creating or modifying metadata, reports, artifacts, logs, summaries, or any output derived from project data. Ensures outputs are sanitized and do not expose raw data, PII-like values, local paths, or environment-sensitive details.
---

# Data Privacy Skill

## Purpose

Keep project outputs safe to commit and safe to share with a mentor.

## Apply This Skill When

- Writing metadata JSON/CSV artifacts.
- Updating `docs/project_report.md`.
- Updating README files with data findings.
- Logging Spark job outputs.
- Creating EDA summaries.
- Preparing artifacts for commit.

## Do Not Persist

- Raw row-level samples.
- Actual `client_id` values.
- Actual search query values.
- Actual product `name` values.
- Absolute local paths.
- Drive-letter paths.
- Local usernames.
- `project_root`.
- `raw_data_dir`.
- Full exception stack traces in committed artifacts.
- `.env`, secrets, tokens, credentials.
- Raw data files.
- Local DB/cache/log files.

## Allowed Outputs

Allowed committed artifacts may include sanitized table-level metadata:

- file name
- repo-relative path
- file size
- inferred format
- schema field names and types
- row count or row count status
- null counts for columns
- distinct counts
- timestamp min/max only if needed and non-sensitive
- neutral technical notes
- short error summaries without stack traces or local paths

## Path Rule

Use only repo-relative paths, for example:

```text
data/raw/synerise_dataset/product_buy.parquet
```

Never persist absolute local or machine-specific paths.

## Reporting Rule

Use neutral technical wording.

Good:
"The dataset is provided as a compressed archive and requires an ingestion step before Spark can read the inner Parquet files."

Bad:
"The user did not extract the file."

## Verification Checklist

Before considering an artifact safe:

- Search for `absolute_path`.
- Search for `project_root`.
- Search for `raw_data_dir`.
- Search for drive-letter paths.
- Search for `.env`, `token`, `secret`, `password`.
- Confirm no raw row samples are persisted.
- Confirm no actual client IDs, query text, or product names are persisted.

## Definition of Done

The output is safe when it contains only project-relevant, sanitized, table-level information and no local environment details or row-level data.
