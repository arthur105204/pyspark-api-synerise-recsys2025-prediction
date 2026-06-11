---
name: git-safety
description: Use before staging, committing, pushing, or preparing commit summaries. Prevents raw data, secrets, local paths, unsanitized metadata, and large artifacts from being committed.
---

# Git Safety Skill

## Purpose

Prevent accidental commits of raw data, sensitive metadata, local environment details, secrets, logs, or large artifacts.

## Apply This Skill When

- Preparing a commit.
- Staging files.
- Reviewing `git status`.
- Reviewing `git diff`.
- Pushing changes.
- Writing commit summaries.

## Non-Negotiable Rules

- Never use `git add .`.
- Stage files explicitly.
- Do not commit raw data.
- Do not commit unsanitized metadata.
- Do not commit `.env`, secrets, tokens, credentials.
- Do not commit local DB/cache/log files.
- Do not commit Spark temp files.
- Do not commit notebook checkpoints.
- Do not commit model artifacts unless explicitly approved.
- Do not commit row-level samples.

## Expected Safe Files

Usually safe to commit:

- `AGENTS.md`
- `PLAN.md`
- `README.md`
- `docs/project_report.md`
- `jobs/*.py`
- `jobs/README.md`
- `.gitignore`
- sanitized small metadata artifacts such as `artifacts/metadata/raw_data_summary.json`

Only commit metadata if it is sanitized and small.

## Files/Folders to Exclude

Do not stage:

- `data/raw/`
- `data/processed/`
- `data/serving/`
- `.env`
- `.env.*`
- `logs/`
- `*.db`
- `*.sqlite`
- `*.parquet`
- `*.csv` if it contains row-level data
- Spark temp directories
- model artifacts
- cache folders

## Required Commands

Before staging:

```bash
git status --short
git diff
```

Stage explicitly, for example:

```bash
git add AGENTS.md
git add PLAN.md
git add docs/project_report.md
git add jobs/00_inspect_raw_data.py
git add jobs/README.md
git add README.md
git add .gitignore
git add artifacts/metadata/raw_data_summary.json
```

Before commit:

```bash
git diff --cached --name-only
git diff --cached
```

Search for unsafe metadata:

```bash
grep -R "absolute_path\|project_root\|raw_data_dir" AGENTS.md PLAN.md README.md docs jobs artifacts/metadata || true
grep -R "drive-letter paths" AGENTS.md PLAN.md README.md docs jobs artifacts/metadata || true
```

PowerShell equivalent:

```powershell
Select-String -Path AGENTS.md,PLAN.md,README.md,docs/project_report.md,jobs/*.py,jobs/README.md,artifacts/metadata/*.json -Pattern "absolute_path|project_root|raw_data_dir"
```

## Commit Message Rule

Use short, specific commit messages, for example:

```bash
git commit -m "chore: add agent guardrails and project tracking"
```

## Definition of Done

A commit is safe when:

- only explicit files are staged
- no raw data is staged
- no secrets are staged
- no absolute paths or local environment details are staged
- metadata artifacts are sanitized
- `git diff --cached` has been reviewed
