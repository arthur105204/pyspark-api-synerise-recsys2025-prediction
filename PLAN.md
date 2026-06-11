# PySpark Customer Behavior Scoring Project Plan

## 0. Project Tracking Rules

- This file tracks project progress and review gates.
- `docs/project_report.md` is the mentor-facing report.
- Each phase must be reviewed before moving to the next phase.
- Every phase must define Goal, Guardrails, Verification/Test Steps, Definition of Done, Review Questions, and Status.
- No phase should silently expand scope.
- No raw data, row-level samples, secrets, absolute local paths, or environment-specific metadata should be committed.
- AI-generated code must be reviewed through git diff before commit.
- Avoid `git add .`; stage files explicitly.

## 1. Current Summary

- Project direction selected: customer behavior scoring from event logs.
- Dataset selected: Synerise RecSys 2025.
- Mentor-facing report created: `docs/project_report.md`.
- Raw data inspection job created: `jobs/00_inspect_raw_data.py`.
- Job README created: `jobs/README.md`.
- Root README created.
- Raw metadata artifact generated: `artifacts/metadata/raw_data_summary.json`.
- Metadata artifact was sanitized before commit so it contains table-level metadata only.
- Stable foundation commit created and pushed: `abf41f3 docs: add project foundation and raw data inspection`.

## 2. Current Status

| Area | Status | Notes |
| --- | --- | --- |
| Problem framing | Done | Stored in `docs/project_report.md` |
| Dataset inspection | Done | Raw metadata generated from Spark-readable Parquet files |
| Metadata privacy | Done | Sanitized metadata contains no absolute local paths or row-level samples |
| EDA | Completed | Full-count EDA artifacts generated and refined for readability |
| Business target selection EDA | Completed pending review | Purchase propensity, cart conversion, and churn compared; next phase candidate is preprocessing |
| Feature engineering | Not started | Requires EDA review first |
| Modeling | Not started | Requires label decision first |
| API | Not started | Requires prediction table first |
| Commit/push | Done | Stable foundation commit pushed to `origin/master` |

## 3. Phase Plan

### Phase 0: Project Foundation and Raw Data Inspection

Goal:
Create a documentation-first foundation and inspect available raw data safely.

Guardrails:

- Only one mentor-facing report: `docs/project_report.md`.
- Do not implement EDA/model/API yet.
- Do not commit raw data.
- Do not persist raw row samples.
- Do not persist absolute local paths or local environment details.
- Do not use `git add .`.

Verification/Test Steps:

- Run raw data inspection job.
- Confirm `artifacts/metadata/raw_data_summary.json` exists.
- Confirm metadata contains schemas and table-level metadata only.
- Search for absolute paths and sensitive fields:
  - `absolute_path`
  - `project_root`
  - `raw_data_dir`
  - drive-letter path patterns
- Review `git status --short`.

Definition of Done:

- `docs/project_report.md` exists and starts from problem statement.
- `jobs/00_inspect_raw_data.py` exists and runs locally.
- Sanitized metadata is generated.
- Raw data is ignored by git.
- No personal workflow issues or AI process logs are in the report.
- Stable files are committed and pushed.

Review Questions:

- Is the report mentor-facing?
- Is the metadata safe to commit?
- Are raw files ignored?
- Are the core columns visible from schema?
- Is the next milestone clearly EDA?

Status:
Done. Foundation files were committed and pushed. Next phase is EDA.

### Phase 1: EDA

Goal:
Understand the verified tables, schemas, event volumes, timestamp ranges, missing values, and table relationships before creating features.

Guardrails:

- Do not train models.
- Do not build API.
- Do not create feature table yet.
- Do not persist row-level samples.
- Avoid expensive full scans unless explicitly controlled.
- Large table counts should use a deliberate strategy, not accidental `count()` calls.

Verification/Test Steps:

- Create and run `jobs/01_eda_summary.py`. Done in safe mode and full-count mode.
- Generate:
  - `artifacts/eda/eda_summary.json`
  - `artifacts/eda/table_overview.csv`
  - `artifacts/eda/column_overview.csv`
- Verify each event table has:
  - schema
  - row count or count status
  - distinct client count if available
  - timestamp range if available
  - null counts for important columns
- Update `docs/project_report.md` with actual EDA findings.

Generated artifacts:

- `artifacts/eda/eda_summary.json`
- `artifacts/eda/table_overview.csv`
- `artifacts/eda/event_table_overview.csv`
- `artifacts/eda/product_table_overview.csv`
- `artifacts/eda/column_overview.csv`

Readability refinement:
The wide sparse overview was replaced by compact and schema-specific views:

- `table_overview.csv` for compact general table metadata.
- `event_table_overview.csv` for user event tables.
- `product_table_overview.csv` for product metadata.
- `column_overview.csv` for column-level metadata.

Review checklist:

- Are `client_id`, `timestamp`, and `sku` reliable core columns?
- Are timestamps parseable?
- Confirm the timestamp parsing result.
- Confirm the null-count result.
- Are there enough purchase events and active clients for target labeling?
- Are there useful category/price fields for propensity?
- Duplicate and skew checks are not yet available; decide whether they belong in Phase 2 preprocessing or targeted follow-up EDA.
- Should the next phase be preprocessing or more EDA?

Definition of Done:

- EDA job runs locally.
- EDA artifacts are generated.
- Report includes EDA objective, input, process, output, findings, interpretation, and review points.
- MVP target selection is supported but remains provisional until review.

Review Questions:

- Is `client_id` the correct user key?
- Is `timestamp` parseable and reliable?
- Does `product_buy` support the provisional target definition?
- Does `product_properties` contain useful category/price metadata?
- Should the MVP use purchase propensity, cart conversion, or churn?

Status:
Completed. Full-count mode completed, exact row counts are available for all six tables, and the next phase candidate is preprocessing.

### Phase 1.1: Business Target Selection EDA

Goal:
Compare purchase propensity, cart conversion, and purchase-based churn before selecting a provisional MVP target.

Guardrails:

- Do not create final training labels.
- Do not write client-level outputs.
- Do not persist raw row samples, raw client ids, query text, or product names.
- Do not implement preprocessing, feature engineering, modeling, batch scoring, or API.
- Keep outputs aggregate-only and sanitized.

Generated artifacts:

- `artifacts/eda/target_feasibility_summary.json`
- `artifacts/eda/business_target_selection_summary.json`
- `artifacts/eda/business_target_comparison.csv`
- `artifacts/eda/churn_window_balance.csv`
- `artifacts/eda/purchase_frequency_summary.csv`
- `artifacts/eda/active_cohort_comparison.csv`
- `artifacts/eda/target_feasibility_notes.md`

Decision needed:
Decide whether the MVP target should use purchase propensity with a provisional 30-day target window, or whether the 45-day purchase propensity or cart conversion alternatives should be reviewed first.

Review checklist:

- Which business target should be MVP?
- Which target window is most reasonable: 14, 30, or 45 days?
- Which eligible cohort should be used?
- Is the positive rate acceptable for baseline modeling?
- Are there enough repeat purchasers?
- Is the target directly useful for an API scoring use case?
- Are leakage rules clear?
- Should Phase 2 proceed to preprocessing?

Status:
Completed pending review. Business target selection EDA recommends purchase propensity as the provisional MVP target with a 30-day target window, pending preprocessing validation.

### Phase 2: Preprocessing

Goal:
Clean and standardize raw event tables for reliable feature engineering.

Guardrails:

- Do not train model yet.
- Do not create API.
- Do not mix future target data into feature windows.
- Do not drop records silently.
- Every cleaning rule must be documented with rationale.

Verification/Test Steps:

- Parse timestamp columns.
- Validate null handling for `client_id`, `timestamp`, `sku`, `url`, `query`, and metadata fields where applicable.
- Check duplicate strategy.
- Check join readiness between event tables and `product_properties`.
- Write clean intermediate outputs only if needed.

Definition of Done:

- Preprocessing rules are documented.
- Cleaned tables or transformations are reproducible.
- Data leakage risks are identified.
- Report explains input columns, cleaning logic, and output schema.

Review Questions:

- Which rows are invalid?
- Which nulls are acceptable?
- Should timestamp be converted to date/time fields?
- Should product metadata be joined before or after aggregation?

Status:
Not started.

### Phase 3: Feature Engineering

Goal:
Create a user-level feature table from event logs.

Guardrails:

- Every feature must have a source table, input columns, transformation logic, and rationale.
- Do not create high-cardinality one-hot features without review.
- Do not include target-window information in feature-window data.
- Do not create features that cannot be explained.

Verification/Test Steps:

- Create user-level aggregation by `client_id`.
- Candidate feature groups:
  - event counts
  - recency features
  - frequency features
  - diversity features
  - cart/buy/remove ratios
  - category/price features if product metadata supports them
  - time-window features if timestamp is reliable
- Generate feature dictionary.
- Validate row count and null/default handling.

Definition of Done:

- Feature table exists.
- Feature dictionary is documented.
- Report lists feature groups, source tables, input columns, output columns, logic, and rationale.
- Feature table is ready for label definition/modeling.

Review Questions:

- Which features are most meaningful for churn?
- Which features are useful for propensity?
- Are any features leaking future behavior?
- Are high-cardinality fields controlled?

Status:
Not started.

### Phase 4: Label Definition

Goal:
Define the first prediction target clearly.

Guardrails:

- Label must be explainable.
- Feature window and target window must be separated.
- Do not finalize churn label without timestamp validation.
- Do not use target behavior as input feature.

Verification/Test Steps:

- Define candidate churn label.
- Define candidate propensity label if needed.
- Check class distribution.
- Check whether enough users have usable labels.
- Document label logic precisely.

Definition of Done:

- First modeling task is selected.
- Label definition is documented.
- Label distribution is computed.
- Data leakage checks are documented.

Review Questions:

- Is churn prediction still the best MVP?
- Is propensity easier or more natural from the data?
- Is the class distribution usable?
- Does the label match the business problem?

Status:
Not started.

### Phase 5: Baseline Modeling

Goal:
Train a simple, explainable baseline model.

Guardrails:

- Start simple before advanced models.
- Document all feature columns and parameters.
- Do not over-tune before baseline metrics are understood.
- Use clear train/validation split logic.
- Do not report metrics without explaining what they mean.

Verification/Test Steps:

- Train baseline Logistic Regression or another simple model.
- Optionally compare with Random Forest or GBT later.
- Record:
  - input table
  - label
  - feature columns
  - categorical encoding
  - numerical preprocessing
  - model parameters
  - split strategy
  - metrics
  - observations

Definition of Done:

- Baseline model trains successfully.
- Metrics are generated.
- Report includes results and interpretation.
- Fine-tuning options are documented.

Review Questions:

- Is the baseline better than naive prediction?
- Are features meaningful?
- Is the split valid?
- What should be tuned next?

Status:
Not started.

### Phase 6: Batch Scoring

Goal:
Generate a prediction table for users.

Guardrails:

- API should not call Spark directly per request.
- Predictions should be generated offline/batch.
- Prediction schema must be stable.
- Model version and scoring timestamp should be included.

Verification/Test Steps:

- Generate prediction table with:
  - `client_id`
  - `prediction_score`
  - `prediction_label`
  - `risk_level`
  - `model_version`
  - `scored_at`
- Validate no duplicate client ids.
- Validate score ranges.

Definition of Done:

- Batch prediction table exists.
- Schema is documented.
- Report includes scoring input, process, and output.

Review Questions:

- Is the prediction schema enough for API?
- Should predictions be stored in local file, SQLite, Postgres, or Redis later?
- Are model versions tracked?

Status:
Not started.

### Phase 7: API Serving

Goal:
Expose prediction lookup by user/client id.

Guardrails:

- API reads from prediction table, not raw data.
- API does not run Spark per request.
- Response schema must be stable.
- Handle missing users clearly.
- Do not expose sensitive data.

Verification/Test Steps:

- Implement endpoint:
  - `GET /users/{client_id}/prediction`
- Test valid user.
- Test missing user.
- Test invalid input.
- Add minimal API tests.

Definition of Done:

- API returns prediction response.
- Error cases are handled.
- README documents how to run API.
- Report includes example request/response.

Review Questions:

- Is the API contract stable?
- Is lookup fast enough?
- Should cache be added?

Status:
Not started.

### Phase 8: Load/Stress Test and Cache

Goal:
Evaluate API behavior under repeated requests.

Guardrails:

- Do not optimize before measuring.
- Do not add Redis/cache unless a clear reason exists.
- Record test settings and results.

Verification/Test Steps:

- Use Locust, k6, or a simple load script.
- Measure:
  - RPS
  - average latency
  - p95 latency
  - error rate
- Compare no-cache vs cache if implemented.

Definition of Done:

- Load test result is recorded.
- Bottlenecks are noted.
- Report includes performance summary.

Review Questions:

- Is API performance enough for demo?
- Is cache necessary?
- What should be improved if this were production?

Status:
Not started.

## 4. Immediate Next Actions

1. Review refined Phase 1 EDA artifacts.
2. Review Phase 1.1 business target selection artifacts.
3. Decide target window and eligible cohort.
4. Confirm whether purchase propensity is approved as the provisional MVP target.
5. Decide whether Phase 2 should proceed to preprocessing.

## 5. Agent Instruction Setup

Current setup:

- `AGENTS.md` contains project-wide rules and skill routing.
- `.codex/skills/data-privacy/SKILL.md` handles privacy and metadata sanitization.
- `.codex/skills/pyspark-job/SKILL.md` handles Spark job structure and safe execution.
- `.codex/skills/git-safety/SKILL.md` handles commit/push safety.

Not created yet:

- EDA reporting skill
- Model experiment skill

Reason:
EDA and model experiment instructions remain as templates in `PLAN.md` until those workflows repeat enough to justify separate skills.

## 6. Commit Safety Checklist

Before every commit:

- Run `git status --short`.
- Run `git diff`.
- Stage files explicitly.
- Do not use `git add .`.
- Confirm no raw data is staged.
- Confirm no `.env`, logs, DB files, cache files, Spark temp files, or row-level samples are staged.
- Confirm metadata is sanitized.
- Commit only stable project files.

## 7. Progress Log

| Date | Phase | Status | Notes |
| --- | --- | --- | --- |
| 2026-06-10 | Phase 0 | Done | Foundation created, metadata sanitized, stable commit pushed |
| 2026-06-11 | Project tracking | In progress | `PLAN.md` created for phase gates before EDA |
| 2026-06-11 | Agent instruction setup | Done | Minimal AGENTS and three focused skills added |
| 2026-06-11 | Phase 1 | Completed | Full-count EDA job ran and generated sanitized EDA artifacts |
| 2026-06-11 | Phase 1.1 | Completed pending review | Business target selection EDA generated aggregate-only comparison artifacts |
