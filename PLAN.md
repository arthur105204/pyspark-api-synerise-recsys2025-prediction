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
| Business target selection EDA | Completed | Purchase propensity selected as provisional MVP target |
| Preprocessing | Completed pending review | Spark preprocessing outputs and sanitized validation artifacts generated |
| Feature engineering | Completed pending review | User-level feature table and sanitized feature artifacts generated |
| Label construction | Completed pending review | Spark label and training outputs generated with sanitized validation artifacts |
| Modeling | Completed pending review | Baseline Spark ML model and sanitized aggregate metrics generated |
| Batch scoring | Completed pending review | Spark score table and sanitized aggregate scoring artifacts generated |
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
- Do not create final labels yet.
- Do not create feature tables yet.
- Do not mix future target data into feature windows.
- Do not drop records silently.
- Every cleaning rule must be documented with rationale.

Generated outputs:

- `data/processed/events/add_to_cart/`
- `data/processed/events/remove_from_cart/`
- `data/processed/events/product_buy/`
- `data/processed/events/search_query/`
- `data/processed/product_properties_clean/`

Generated artifacts:

- `artifacts/preprocessing/preprocessing_summary.json`
- `artifacts/preprocessing/table_validation.csv`
- `artifacts/preprocessing/duplicate_check_summary.csv`
- `artifacts/preprocessing/product_metadata_validation.csv`
- `artifacts/preprocessing/preprocessing_notes.md`

Configuration:

- `configs/pipeline_config.yaml`

Verification/Test Steps:

- Parse timestamp columns.
- Validate null handling for `client_id`, `timestamp`, `sku`, `url`, `query`, and metadata fields where applicable.
- Check duplicate strategy.
- Check join readiness between event tables and `product_properties`.
- Write clean intermediate outputs with Spark Parquet writer.
- Confirm `data/processed/` remains ignored by git.
- Confirm artifacts contain aggregate validation only.

Definition of Done:

- Preprocessing rules are documented.
- Cleaned tables or transformations are reproducible.
- Data leakage risks are identified.
- Report explains input columns, cleaning logic, and output schema.
- Processed outputs are created locally under `data/processed/`.
- Sanitized preprocessing artifacts are generated.
- No final labels, feature tables, model training, batch scoring, or API code is added.

Review Questions:

- Which rows are invalid?
- Which nulls are acceptable?
- Should timestamp be converted to date/time fields?
- Should product metadata be joined before or after aggregation?
- Are processed event timestamps valid?
- Are duplicate rates acceptable?
- Is the product metadata dedup rule acceptable?
- Are processed tables ready for feature engineering?
- Is the 30-day purchase propensity target setup still accepted?
- Are leakage rules documented clearly?
- Should optional large tables be included now or deferred?

Status:
Completed pending review. Default preprocessing plus optional search preprocessing completed successfully. `page_visit` remains deferred. Spark writes succeeded for processed event and product outputs. Validation artifacts report zero invalid rows and zero timestamp parse failures for processed event tables.

### Phase 3: Feature Engineering

Goal:
Create a leakage-safe user-level feature table from processed event logs.

Guardrails:

- Every feature must have a source table, input columns, transformation logic, and rationale.
- Do not create high-cardinality one-hot features without review.
- Do not include target-window information in feature-window data.
- Do not create features that cannot be explained.
- Do not create final labels yet.
- Do not train models.
- Do not create batch predictions or API outputs.

Planned/generated outputs:

- `data/processed/features/user_behavior_features/`

Planned/generated artifacts:

- `artifacts/features/feature_summary.json`
- `artifacts/features/feature_catalog.csv`
- `artifacts/features/feature_validation.csv`
- `artifacts/features/feature_notes.md`

Configuration:

- `configs/pipeline_config.yaml` stores `cutoff_date` and `target_end` for leakage-safe feature construction.

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
- Confirm no final label column is created.
- Confirm Spark writer is used for feature Parquet output.
- Confirm artifacts contain aggregate validation only.

Definition of Done:

- Feature table exists.
- Feature dictionary is documented.
- Report lists feature groups, source tables, input columns, output columns, logic, and rationale.
- Feature table is ready for label definition/modeling.
- Feature artifacts are generated under `artifacts/features/`.
- `docs/project_report.md` includes actual feature engineering findings from generated artifacts.
- No final label table, model training, batch scoring, or API code is added.

Review Questions:

- Are the feature groups sufficient for a baseline purchase propensity model?
- Are recency/count/ratio/product metadata features valid?
- Is the eligible cohort indicator correct?
- Are null/fill strategies acceptable?
- Are duplicate handling assumptions acceptable?
- Are any features leaking future behavior?
- Are high-cardinality fields controlled?
- Should Phase 4 proceed to label construction?

Status:
Completed pending review. The feature engineering job generated `data/processed/features/user_behavior_features/` and sanitized artifacts under `artifacts/features/`. The feature table has 2,810,342 rows, 36 features, and an eligible purchase propensity cohort count of 2,149,796. Search features were included and `page_visit` remains deferred.

### Phase 4: Label Definition

Goal:
Define the first prediction target clearly and create a training-ready dataset.

Guardrails:

- Label must be explainable.
- Feature window and target window must be separated.
- Use the purchase propensity 30-day target selected in Phase 1.1.
- Do not use target behavior as input feature.
- Do not train or evaluate a model.
- Do not create predictions, batch scoring outputs, or API code.

Planned outputs:

- `data/processed/labels/purchase_propensity_30d/`
- `data/processed/training/purchase_propensity_30d/`

Planned artifacts:

- `artifacts/labels/label_summary.json`
- `artifacts/labels/label_validation.csv`
- `artifacts/labels/training_dataset_validation.csv`
- `artifacts/labels/label_notes.md`

Verification/Test Steps:

- Build labels with `jobs/04_build_labels.py`.
- Use eligible clients from `is_eligible_purchase_propensity = 1`.
- Define positive label from `product_buy` events in the target window.
- Check class distribution.
- Check whether enough users have usable labels.
- Document label logic precisely.
- Confirm label row count equals eligible cohort count.
- Confirm positive count is consistent with Phase 1.1.
- Confirm no null labels and only `0`/`1` values.
- Confirm no duplicate `client_id` in label or training data.
- Confirm no model, prediction, batch scoring, or API output is created.

Definition of Done:

- First modeling task is selected.
- Label definition is documented.
- Label distribution is computed.
- Data leakage checks are documented.
- Label table exists under `data/processed/labels/purchase_propensity_30d/`.
- Training-ready dataset exists under `data/processed/training/purchase_propensity_30d/`.
- Sanitized label artifacts are generated.
- Report includes actual label construction findings from artifacts.

Review Questions:

- Does the purchase propensity label match the business problem?
- Is the class distribution usable?
- Does the positive count match Phase 1.1 expectations?
- Is the target-window boundary rule acceptable?
- Should Phase 5 proceed to baseline modeling?

Status:
Completed pending review. `jobs/04_build_labels.py` generated the purchase propensity 30-day label table, training-ready dataset, and sanitized artifacts under `artifacts/labels/`. The label table has 2,149,796 rows, 93,614 positive labels, 2,056,182 negative labels, and a 0.043546 positive rate. The training-ready dataset has 2,149,796 rows and 36 features.

### Phase 5: Baseline Modeling

Goal:
Train a simple, explainable baseline model.

Guardrails:

- Start simple before advanced models.
- Document all feature columns and parameters.
- Do not over-tune before baseline metrics are understood.
- Use clear train/validation split logic.
- Do not report metrics without explaining what they mean.
- Use Spark ML / PySpark for modeling.
- Do not implement API serving.
- Do not create production batch scoring outputs.

Planned outputs:

- `data/models/purchase_propensity_baseline/`

Planned artifacts:

- `artifacts/modeling/baseline_model_summary.json`
- `artifacts/modeling/baseline_metrics.csv`
- `artifacts/modeling/topk_metrics.csv`
- `artifacts/modeling/feature_processing_summary.csv`
- `artifacts/modeling/baseline_model_notes.md`

Verification/Test Steps:

- Train baseline Logistic Regression or another simple model.
- Use median imputation for numeric features.
- Use class weights for label imbalance unless runtime constraints require disabling them.
- Compute ROC-AUC and PR-AUC.
- Compute confusion matrix at threshold `0.5`.
- Compute TopK precision, recall, and lift at `1%`, `5%`, and `10%`.
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
- Model is saved under `data/models/purchase_propensity_baseline/`.
- Metrics are generated.
- Sanitized modeling artifacts are generated.
- Report includes results and interpretation.
- Fine-tuning options are documented.
- No API serving, production scoring endpoint, or row-level prediction artifact is created.

Review Questions:

- Is the baseline better than naive prediction?
- Are features meaningful?
- Is the split valid?
- Are ROC-AUC, PR-AUC, and TopK metrics acceptable for the MVP?
- Is class weighting helpful enough to keep enabled?
- What should be tuned next?

Status:
Completed pending review. `jobs/05_train_baseline_model.py` generated the baseline Spark ML model and sanitized aggregate artifacts under `artifacts/modeling/`. The model used 36 features, median imputation, class weights, and an 80/20 split with seed `42`. Test ROC-AUC is 0.840501 and PR-AUC is 0.254436.

### Phase 6: Batch Scoring

Goal:
Generate a prediction table for users.

Guardrails:

- API should not call Spark directly per request.
- Predictions should be generated offline/batch.
- Prediction schema must be stable.
- Model version and scoring timestamp should be included.
- Use Spark ML / PySpark for scoring.
- Do not implement API serving.
- Do not create an online inference endpoint.
- Do not commit model binaries or row-level score data.

Planned outputs:

- `data/processed/scoring/purchase_propensity_scores/`

Planned artifacts:

- `artifacts/scoring/scoring_summary.json`
- `artifacts/scoring/score_distribution.csv`
- `artifacts/scoring/scoring_validation.csv`
- `artifacts/scoring/scoring_notes.md`

Verification/Test Steps:

- Generate prediction table with:
  - `client_id`
  - `prediction_score`
  - `prediction_label`
  - `model_version`
  - `scored_at`
- Load trained Spark ML model from `data/models/purchase_propensity_baseline/`.
- Score eligible clients from `data/processed/features/user_behavior_features/`.
- Validate no duplicate client ids.
- Validate score ranges.
- Validate prediction labels are only `0` or `1`.
- Validate no label or target-window metadata is included in score output.
- Generate aggregate scoring artifacts only.

Definition of Done:

- Batch prediction table exists.
- Schema is documented.
- Report includes scoring input, process, and output.
- Output row count equals eligible scoring cohort count.
- Scoring validation artifacts are generated.
- Leakage validation passes.
- No API serving, online endpoint, model binary commit, or row-level score artifact is created.

Review Questions:

- Is the prediction schema enough for API?
- Should predictions be stored in local file, SQLite, Postgres, or Redis later?
- Are model versions tracked?
- Are score buckets and threshold useful for the first API lookup?

Status:
Completed pending review. `jobs/06_batch_score.py` generated `data/processed/scoring/purchase_propensity_scores/` and sanitized artifacts under `artifacts/scoring/`. The job scored 2,149,796 eligible clients, produced 516,759 predicted positives at threshold 0.5, and passed validation and leakage checks.

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

1. Review generated scoring artifacts under `artifacts/scoring/`.
2. Confirm score output row count, score range, duplicate checks, and schema.
3. Decide whether Phase 7 should proceed to API serving.

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
| 2026-06-12 | Phase 2 | Completed | Preprocessing pipeline generated local processed tables and sanitized validation artifacts |
| 2026-06-12 | Phase 3 | Completed pending review | Feature engineering generated user-level features and sanitized validation artifacts |
