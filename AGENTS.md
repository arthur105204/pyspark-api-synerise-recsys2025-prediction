# AGENTS.md

## Project Context

This is a PySpark + API internship project for customer behavior scoring using the Synerise RecSys 2025 dataset.

The project must follow a documentation-first flow:

Problem statement
-> dataset understanding
-> data ingestion
-> EDA
-> preprocessing
-> feature engineering
-> label definition
-> modeling
-> batch scoring
-> API serving
-> load/stress test

## Source of Truth

- `docs/project_report.md` is the only mentor-facing report.
- `PLAN.md` tracks project phases, review gates, and Definition of Done.
- Job scripts live under `jobs/`.
- Generated metadata/artifacts must be sanitized before commit.
- Raw data must stay under ignored data folders and must not be committed.

## Non-Negotiable Rules

- Do not commit raw data.
- Do not persist raw row-level samples.
- Do not persist actual `client_id` values, search query values, or product name values.
- Do not persist absolute local paths, `project_root`, `raw_data_dir`, drive letters, usernames, or local environment details in committed artifacts.
- Do not write AI/Codex process logs, prompt history, or personal workflow issues into project docs.
- Do not use `git add .`.
- Do not move to the next phase without explicit review or instruction.
- Keep changes small and reversible.

## Skill Routing

Use the skills only when relevant:

- Use `data-privacy` when creating or modifying metadata, reports, artifacts, logging, serialization, or anything derived from data.
- Use `pyspark-job` when creating or modifying scripts under `jobs/` or any Spark-based data pipeline code.
- Use `git-safety` before staging, committing, pushing, or preparing a commit summary.

Do not copy skill content into this file. Each skill contains its own detailed checklist.

## Reporting Rules

When updating `docs/project_report.md`, write in a mentor-facing style:

- Objective
- Input
- Process
- Output
- Result
- Interpretation
- Next decision or mentor review point

Use neutral technical language. Do not document informal workflow mistakes or AI process details.

## Phase Discipline

Every phase should have:

- Goal
- Guardrails
- Verification/Test Steps
- Definition of Done
- Review Questions
- Status

If a requested task is outside the current phase, mention it as a future step instead of implementing it.
