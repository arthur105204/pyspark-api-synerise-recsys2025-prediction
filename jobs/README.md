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

## `01c_validate_target_and_feature_assumptions.py`

This job runs targeted aggregate-only EDA to validate business assumptions behind the current purchase propensity MVP.

It checks:

- whether target-window purchases usually have prior add-to-cart history
- how the current active cohort differs from a cart-only conversion cohort
- whether product metadata is stable at SKU level
- whether selected count, recency, and ratio features show directional relationship with the target
- whether search count has useful aggregate signal or appears noisy

It does not modify preprocessing, modeling, batch scoring, API, or demo logic. It does not train models, rescore users, write raw client IDs, write raw query text, write product names, or output row-level examples.

### How to Run

```powershell
python jobs/01c_validate_target_and_feature_assumptions.py
```

The default `--engine auto` tries Spark first. On local Windows environments where Spark cannot read local Parquet because of Hadoop native filesystem issues, the job falls back to PyArrow for the same aggregate-only EDA outputs.

Explicit engine options:

```powershell
python jobs/01c_validate_target_and_feature_assumptions.py --engine spark
python jobs/01c_validate_target_and_feature_assumptions.py --engine pyarrow
```

### Target Validation Outputs

The job writes:

```text
artifacts/target_validation/purchase_path_summary.csv
artifacts/target_validation/cohort_overlap_summary.csv
artifacts/target_validation/product_metadata_consistency.csv
artifacts/target_validation/feature_target_relationship.csv
artifacts/target_validation/search_signal_summary.csv
artifacts/target_validation/target_assumption_validation_summary.md
```

Before continuing modeling/API/demo review, review whether the current target should remain loosely framed as purchase propensity or be described more precisely as 30-day purchase prediction for prior cart/purchase users.

## `01d_compare_candidate_problem_framings.py`

This job compares candidate MVP problem framings with aggregate-only EDA.

It compares:

- current prior cart/purchase cohort
- cart-only conversion cohort
- search/cart/buy active-user cohort
- buy-only repeat-purchase cohort
- search-only diagnostic cohort
- all-active page-visit framing as a documented future extension

It does not change the target, modify modeling code, train models, rerun batch scoring, change API behavior, write raw client IDs, write raw query text, write product names, or output row-level examples.

### How to Run

```powershell
python jobs/01d_compare_candidate_problem_framings.py
```

### Problem Framing Outputs

The job writes:

```text
artifacts/problem_framing/candidate_cohort_comparison.csv
artifacts/problem_framing/candidate_target_balance.csv
artifacts/problem_framing/candidate_overlap_matrix.csv
artifacts/problem_framing/candidate_feature_availability.csv
artifacts/problem_framing/candidate_processing_cost_estimate.csv
artifacts/problem_framing/problem_framing_recommendation.md
```

The current recommendation is to keep the implemented MVP target but rename/reframe it as:

```text
30-day purchase prediction for prior cart/purchase users
```

## `02_preprocess_events.py`

This job runs Phase 2 preprocessing for the purchase propensity MVP direction.

It standardizes raw event tables into clean intermediate Parquet outputs using Spark:

- parses `timestamp` into `event_ts`
- derives `event_date`
- adds `event_type`
- standardizes common event columns
- validates nulls and timestamp parse failures
- computes aggregate duplicate checks
- creates deduplicated product metadata with `sku`, `category`, and `price`

It does not create final labels, feature tables, model inputs, batch predictions, or API outputs.

### How to Run

Default preprocessing processes the primary event tables and product metadata:

```powershell
python jobs/02_preprocess_events.py
```

Optional search preprocessing:

```powershell
python jobs/02_preprocess_events.py --include-search
```

Optional page-visit preprocessing:

```powershell
python jobs/02_preprocess_events.py --include-search --include-page-visit
```

WSL/Linux uses the same command from the project root:

```bash
python jobs/02_preprocess_events.py
```

`--sample-fraction` is for local development only and should not be used for final preprocessing.

### Runtime Note

The preprocessing job writes processed Parquet outputs with Spark `DataFrameWriter`. On Windows local Spark, Parquet writes may require a proper Hadoop/winutils setup. If local Windows Spark writer fails, run the job in WSL/Linux or configure Hadoop properly.

### Preprocessing Outputs

Default processed data outputs:

```text
data/processed/events/add_to_cart/
data/processed/events/remove_from_cart/
data/processed/events/product_buy/
data/processed/product_properties_clean/
```

Optional processed data outputs:

```text
data/processed/events/search_query/
data/processed/events/page_visit/
```

Sanitized aggregate artifacts:

```text
artifacts/preprocessing/preprocessing_summary.json
artifacts/preprocessing/table_validation.csv
artifacts/preprocessing/duplicate_check_summary.csv
artifacts/preprocessing/product_metadata_validation.csv
artifacts/preprocessing/preprocessing_notes.md
```

`data/processed/` is ignored by default and should not be committed.

## `03_build_features.py`

This job runs Phase 3 feature engineering for the provisional purchase propensity MVP direction.

It reads processed Spark Parquet tables from `data/processed/` and creates user-level aggregate features before the configured cutoff date.

Default inputs:

- `data/processed/events/add_to_cart/`
- `data/processed/events/remove_from_cart/`
- `data/processed/events/product_buy/`
- `data/processed/product_properties_clean/`

Search input is included automatically when available, or explicitly with `--include-search`:

- `data/processed/events/search_query/`

`page_visit` is not used in this phase.

### How to Run

```powershell
python jobs/03_build_features.py
```

Explicit search features:

```powershell
python jobs/03_build_features.py --include-search
```

WSL/Linux uses the same command from the project root:

```bash
python jobs/03_build_features.py --include-search
```

The job reads target/cutoff decisions from:

```text
configs/pipeline_config.yaml
```

### Feature Outputs

The job writes the user-level feature table to:

```text
data/processed/features/user_behavior_features/
```

Sanitized aggregate artifacts:

```text
artifacts/features/feature_summary.json
artifacts/features/feature_catalog.csv
artifacts/features/feature_validation.csv
artifacts/features/feature_notes.md
```

This job does not create final labels, train models, create batch predictions, or implement API serving. Feature data under `data/processed/` is ignored by default and should not be committed.

### Feature Engineering Runtime Note

The feature job uses Spark to read processed Parquet inputs and Spark `DataFrameWriter` to write the feature table. On Windows local Spark, reading or writing local Parquet directories may require a proper Hadoop/winutils setup. If local Windows Spark fails, run the job in WSL/Linux or configure Hadoop properly.

## `04_build_labels.py`

This job runs Phase 4 label construction for the purchase propensity 30-day task.

Inputs:

- `data/processed/features/user_behavior_features/`
- `data/processed/events/product_buy/`
- `configs/pipeline_config.yaml`

Label definition:

- Eligible clients come from `is_eligible_purchase_propensity = 1` in the Phase 3 feature table.
- Positive label: eligible client has at least one `product_buy` event in the target window.
- Negative label: eligible client has no `product_buy` event in the target window.
- Boundary rule: `event_ts >= cutoff_date` and `event_ts < date_add(target_end, 1)`.

The job aggregates target-window purchases to one row per `client_id`, so multiple target-window purchases increase `target_event_count` but keep the final `label` binary.

### How to Run

```powershell
python jobs/04_build_labels.py
```

With explicit config:

```powershell
python jobs/04_build_labels.py --config configs/pipeline_config.yaml
```

WSL/Linux uses the same command from the project root:

```bash
python jobs/04_build_labels.py
```

### Label Outputs

The job writes the label table to:

```text
data/processed/labels/purchase_propensity_30d/
```

The job writes the training-ready dataset to:

```text
data/processed/training/purchase_propensity_30d/
```

Sanitized aggregate artifacts:

```text
artifacts/labels/label_summary.json
artifacts/labels/label_validation.csv
artifacts/labels/training_dataset_validation.csv
artifacts/labels/label_notes.md
```

Latest validated output:

- label row count: 2,149,796
- positive labels: 93,614
- negative labels: 2,056,182
- positive rate: 0.043546
- training dataset row count: 2,149,796
- feature count used: 36

This job does not train a model, evaluate a model, create predictions, create batch scoring outputs, or implement API serving. Label and training data under `data/processed/` is ignored by default and should not be committed.

### Label Construction Runtime Note

The label job uses Spark to read processed Parquet inputs and Spark `DataFrameWriter` to write the label and training tables. On Windows local Spark, reading or writing local Parquet directories may require a proper Hadoop/winutils setup. If local Windows Spark fails, run the job in WSL/Linux or configure Hadoop properly.

## `05_train_baseline_model.py`

This job runs Phase 5 baseline modeling for the purchase propensity 30-day task.

Inputs:

- `data/processed/training/purchase_propensity_30d/`
- `configs/pipeline_config.yaml`

Modeling approach:

- Spark ML `LogisticRegression`
- deterministic 80/20 train/test split
- seed: `42`
- median imputation for numeric model inputs
- class weights enabled by default for class imbalance

Feature preparation:

- Uses numeric feature columns from the Phase 4 training dataset.
- Excludes `client_id`, `label`, `target_window_start`, `target_window_end`, `target_event_count`, prediction columns, and label-like metadata.
- Uses Spark ML `Imputer`, `VectorAssembler`, and `LogisticRegression`.

### How to Run

```powershell
python jobs/05_train_baseline_model.py
```

With explicit config:

```powershell
python jobs/05_train_baseline_model.py --config configs/pipeline_config.yaml
```

Optional arguments:

```powershell
python jobs/05_train_baseline_model.py --input-path data/processed/training/purchase_propensity_30d --model-output data/models/purchase_propensity_baseline --artifacts-base artifacts --sample-fraction 1.0 --seed 42
```

WSL/Linux uses the same command from the project root:

```bash
python jobs/05_train_baseline_model.py
```

### Model Outputs

The job writes the Spark ML model to:

```text
data/models/purchase_propensity_baseline/
```

Sanitized aggregate artifacts:

```text
artifacts/modeling/baseline_model_summary.json
artifacts/modeling/baseline_metrics.csv
artifacts/modeling/topk_metrics.csv
artifacts/modeling/feature_processing_summary.csv
artifacts/modeling/baseline_model_notes.md
```

Metrics include train/test row counts, positive rates, ROC-AUC, PR-AUC, confusion matrix at threshold `0.5`, and TopK precision/recall/lift at `1%`, `5%`, and `10%`.

TopK metrics are computed with Spark ordering plus `limit()` for each K slice, then aggregated. The job does not use a global Spark window ranking step for TopK and does not write row-level predictions.

Latest validated output:

- train rows: 1,720,719
- test rows: 429,077
- train positive rate: 0.043488
- test positive rate: 0.043778
- feature count used: 36
- class weighting enabled: true
- ROC-AUC: 0.840501
- PR-AUC: 0.254436
- threshold `0.5` confusion matrix: TP 13,637; FP 89,245; TN 321,048; FN 5,147
- Top 1%: precision 0.480541; recall 0.109774; lift 10.976839
- Top 5%: precision 0.296215; recall 0.338320; lift 6.766350
- Top 10%: precision 0.215205; recall 0.491589; lift 4.915851

This job does not create API serving code, production batch scoring outputs, row-level prediction artifacts, or client-level exports. Model data under `data/models/` is ignored by default and should not be committed.

### Baseline Modeling Runtime Note

The modeling job uses Spark to read the Phase 4 training Parquet dataset and Spark ML to write the model. On Windows local Spark, reading or writing local Parquet/model directories may require a proper Hadoop/winutils setup. If local Windows Spark fails, run the job in WSL/Linux or configure Hadoop properly.

## `05b_threshold_analysis.py`

This E2 job evaluates threshold decision policies for the existing temporal baseline model.

Inputs:

- `data/models/purchase_propensity_baseline_temporal/`
- `data/processed/training/e1_valid_2022_11_09/purchase_propensity_30d/`
- `artifacts/modeling/e1_temporal_validation/topk_metrics.csv`

It loads the trained Spark ML model, scores the temporal validation snapshot in memory, computes aggregate threshold metrics for thresholds `0.01` through `0.99`, compares fixed-threshold policies with TopK policies, and writes sanitized aggregate-only artifacts.

### How to Run

```bash
python jobs/05b_threshold_analysis.py
```

Optional explicit paths:

```bash
python jobs/05b_threshold_analysis.py --model-input data/models/purchase_propensity_baseline_temporal --validation-input data/processed/training/e1_valid_2022_11_09/purchase_propensity_30d --artifact-dir artifacts/modeling/e1_temporal_validation
```

Outputs:

```text
artifacts/modeling/e1_temporal_validation/threshold_metrics.csv
artifacts/modeling/e1_temporal_validation/threshold_summary.json
artifacts/modeling/e1_temporal_validation/threshold_review.md
```

This job does not retrain the model, modify features, tune hyperparameters, persist row-level predictions, write raw client IDs, write raw query text, or write product names.

## `05c_calibration_analysis.py`

This E3 job evaluates whether the existing temporal baseline model scores can be interpreted as probabilities or should be treated mainly as ranking scores.

Inputs:

- `data/models/purchase_propensity_baseline_temporal/`
- `data/processed/training/e1_valid_2022_11_09/purchase_propensity_30d/`

It loads the trained Spark ML model, scores the temporal validation snapshot in memory, creates 10 equal-width score buckets, computes aggregate calibration gaps, and writes sanitized aggregate-only artifacts.

### How to Run

```bash
python jobs/05c_calibration_analysis.py
```

Optional explicit paths:

```bash
python jobs/05c_calibration_analysis.py --model-input data/models/purchase_propensity_baseline_temporal --validation-input data/processed/training/e1_valid_2022_11_09/purchase_propensity_30d --artifact-dir artifacts/modeling/e3_calibration
```

Outputs:

```text
artifacts/modeling/e3_calibration/calibration_curve.csv
artifacts/modeling/e3_calibration/calibration_summary.json
artifacts/modeling/e3_calibration/calibration_review.md
```

This job does not retrain the model, fit a calibration model, modify features, change labels, change thresholds, persist row-level predictions, write raw client IDs, write raw query text, or write product names.

## `05d_feature_ablation.py`

This E4 job measures feature-family contribution using temporal validation.

Inputs:

- `data/processed/training/e1_train_2022_10_10/purchase_propensity_30d/`
- `data/processed/training/e1_valid_2022_11_09/purchase_propensity_30d/`

It trains one Logistic Regression model per ablation, each time removing one existing feature family, then evaluates ROC-AUC, PR-AUC, and TopK precision/lift on the E1 temporal validation snapshot.

Feature families:

- add-to-cart activity
- product-buy activity
- remove-from-cart activity
- search activity
- recency features
- ratio features
- product metadata features
- cohort indicator
- overall activity (`active_days_count`)

### How to Run

```bash
python jobs/05d_feature_ablation.py
```

Optional explicit paths:

```bash
python jobs/05d_feature_ablation.py --train-input data/processed/training/e1_train_2022_10_10/purchase_propensity_30d --validation-input data/processed/training/e1_valid_2022_11_09/purchase_propensity_30d --artifact-dir artifacts/modeling/e4_feature_ablation
```

Outputs:

```text
artifacts/modeling/e4_feature_ablation/feature_ablation_results.csv
artifacts/modeling/e4_feature_ablation/feature_ablation_summary.json
artifacts/modeling/e4_feature_ablation/feature_ablation_review.md
```

This job does not add features, change labels, change cohorts, tune hyperparameters, calibrate scores, benchmark new model classes, persist row-level predictions, write raw client IDs, write raw query text, write product names, or write model binaries.

## `05e_feature_redundancy_followup.py`

This E4 follow-up job investigates whether `active_days_count` absorbs signal from cart, buy, search, and recency features.

Inputs:

- `data/processed/training/e1_train_2022_10_10/purchase_propensity_30d/`
- `data/processed/training/e1_valid_2022_11_09/purchase_propensity_30d/`

It runs the same class-weighted Logistic Regression setup as E1/E4 for combined ablations:

- baseline
- remove overall activity
- remove overall activity plus add-to-cart activity
- remove overall activity plus product-buy activity
- remove overall activity plus add-to-cart and product-buy activity
- remove overall activity plus recency features
- remove overall activity plus recency, add-to-cart, and product-buy activity

It also computes an aggregate feature redundancy audit by family, including pairwise correlation where feasible, correlation with `active_days_count`, null rates, and variance statistics.

### How to Run

```bash
python jobs/05e_feature_redundancy_followup.py
```

Outputs:

```text
artifacts/modeling/e4_feature_ablation_followup/combined_ablation_results.csv
artifacts/modeling/e4_feature_ablation_followup/feature_redundancy_audit.csv
artifacts/modeling/e4_feature_ablation_followup/combined_ablation_review.md
```

This job does not add features, change labels, change cohorts, tune hyperparameters, calibrate scores, benchmark new model classes, persist row-level predictions, write raw client IDs, write raw query text, write product names, or write model binaries.

## `05f_feature_rationalization_audit.py`

This audit combines E4 ablation evidence with variance, constant-feature detection, correlation structure, and business meaning before any E5 feature expansion.

Inputs:

- `data/processed/training/e1_train_2022_10_10/purchase_propensity_30d/`
- `artifacts/modeling/e4_feature_ablation/feature_ablation_summary.json`

It detects zero-variance features, skips unsafe correlations involving constant features, computes a feature redundancy matrix, and classifies each feature family into exactly one rationalization category:

- `KEEP_CORE`
- `KEEP_SUPPORTING`
- `REVIEW_REDUNDANCY`
- `REMOVE_CONSTANT`
- `REMOVE_CANDIDATE`

### How to Run

```bash
python jobs/05f_feature_rationalization_audit.py
```

Outputs:

```text
artifacts/modeling/feature_rationalization/feature_variance_audit.csv
artifacts/modeling/feature_rationalization/feature_redundancy_matrix.csv
artifacts/modeling/feature_rationalization/feature_decision_matrix.csv
artifacts/modeling/feature_rationalization/feature_rationalization_review.md
```

This job does not retrain a model, add features, change labels, change cohorts, calibrate scores, persist row-level predictions, write raw client IDs, write raw query text, write product names, or write model binaries.

## `05g_train_baseline_v21.py`

This job trains the first experimental Baseline v2 model, Baseline V2-1.

Inputs:

- `configs/baseline_v21_features.json`
- `data/processed/training/e1_train_2022_10_10/purchase_propensity_30d/`
- `data/processed/training/e1_valid_2022_11_09/purchase_propensity_30d/`

V2-1 removes only high-confidence defective features:

- `is_eligible_purchase_propensity`
- `buy_to_cart_ratio`
- `remove_to_cart_ratio`
- `cart_minus_remove_count`
- `search_to_cart_ratio`

It keeps the same temporal split, Logistic Regression model class, class weighting, median imputation, and TopK evaluation pattern as E1. It does not remove rolling-window features in V2-1; it only writes a rolling-window review artifact.

### How to Run

```bash
python jobs/05g_train_baseline_v21.py
```

Outputs:

```text
data/models/purchase_propensity_baseline_v21/
artifacts/modeling/baseline_v2/v21_window_review.csv
artifacts/modeling/baseline_v2/v21_evaluation.md
```

This job does not add new engineered features, redesign feature families, modify labels, modify temporal splits, tune hyperparameters, calibrate scores, overwrite E1 artifacts, persist row-level predictions, write raw client IDs, write raw query text, or write product names.

## `05h_train_baseline_v22.py`

This job trains Baseline V2-2, a rolling-window reduction experiment built on top of Baseline V2-1.

Inputs:

- `configs/baseline_v22_features.json`
- `data/processed/training/e1_train_2022_10_10/purchase_propensity_30d/`
- `data/processed/training/e1_valid_2022_11_09/purchase_propensity_30d/`

V2-2 keeps the same temporal split, Logistic Regression model class, class weighting, median imputation, and TopK evaluation pattern as E1/V2-1. It removes the V2-1 high-confidence defective features and the standalone 60-day rolling count features:

- `add_to_cart_count_60d`
- `product_buy_count_60d`
- `remove_from_cart_count_60d`
- `search_query_count_60d`

It keeps total count, 30-day count, and 90-day count features for this isolated experiment. It does not add trend features, transition features, cadence features, sequence features, ratio features, search redesign, purchase redesign, remove-from-cart redesign, new labels, new temporal splits, calibration, hyperparameter tuning, or a new model class.

### How to Run

```bash
python jobs/05h_train_baseline_v22.py
```

Outputs:

```text
data/models/purchase_propensity_baseline_v22/
artifacts/modeling/baseline_v2/v22_window_selection_review.csv
artifacts/modeling/baseline_v2/v22_window_selection_review.md
artifacts/modeling/baseline_v2/v22_summary.json
artifacts/modeling/baseline_v2/v22_evaluation.md
```

This job does not overwrite E1 or V2-1 artifacts, persist row-level predictions, write raw client IDs, write raw query text, write product names, or write row-level examples.

## `05i_train_baseline_v23a.py`

This job trains Baseline V2-3a, a search quick-win redesign experiment built on top of Baseline V2-2.

Inputs:

- `configs/baseline_v23a_features.json`
- `data/processed/training/e1_train_2022_10_10/purchase_propensity_30d/`
- `data/processed/training/e1_valid_2022_11_09/purchase_propensity_30d/`

V2-3a keeps the same temporal split, Logistic Regression model class, class weighting, median imputation, and TopK evaluation pattern as E1/V2-1/V2-2. It replaces the remaining raw search count/day/recency representation with three quick-win features:

- `search_count_bucket`
- `search_recency_bucket`
- `recent_search_flag`

It does not add search-to-cart transition features, normalized search intensity, trend features, session features, query semantics, query embeddings, category transitions, new labels, new temporal splits, calibration, hyperparameter tuning, or a new model class.

### How to Run

```bash
python jobs/05i_train_baseline_v23a.py
```

Outputs:

```text
data/models/purchase_propensity_baseline_v23a/
artifacts/modeling/baseline_v2/v23a_search_feature_review.md
artifacts/modeling/baseline_v2/v23a_feature_definitions.md
artifacts/modeling/baseline_v2/v23a_summary.json
artifacts/modeling/baseline_v2/v23a_evaluation.md
```

This job does not overwrite previous baselines, persist row-level predictions, write raw client IDs, write raw query text, write product names, or write row-level examples.

## `05j_train_baseline_v23b.py`

This job trains Baseline V2-3b, a minimal search-to-cart transition experiment built on top of Baseline V2-2.

Inputs:

- `configs/baseline_v23b_features.json`
- `data/processed/training/e1_train_2022_10_10/purchase_propensity_30d/`
- `data/processed/training/e1_valid_2022_11_09/purchase_propensity_30d/`
- `data/processed/events/search_query/`
- `data/processed/events/add_to_cart/`

V2-3b keeps the same temporal split, Logistic Regression model class, class weighting, median imputation, and TopK evaluation pattern as E1/V2-1/V2-2. It keeps V2-2 raw search features and adds only:

- `search_before_cart_count`
- `search_to_cart_rate`
- `recent_search_then_cart_flag`

It does not add raw query text features, query embeddings, session features, trend features, category transitions, new labels, new temporal splits, calibration, hyperparameter tuning, or a new model class.

### How to Run

```bash
python jobs/05j_train_baseline_v23b.py
```

Outputs:

```text
data/models/purchase_propensity_baseline_v23b/
artifacts/modeling/baseline_v2/v23b_feature_definitions.md
artifacts/modeling/baseline_v2/v23b_transition_feature_distribution.csv
artifacts/modeling/baseline_v2/v23b_transition_positive_rate.csv
artifacts/modeling/baseline_v2/v23b_transition_coefficients.csv
artifacts/modeling/baseline_v2/v23b_summary.json
artifacts/modeling/baseline_v2/v23b_evaluation.md
```

This job does not overwrite previous baselines, persist row-level predictions, write raw client IDs, write raw query text, write product names, row-level transitions, or row-level examples.

## `05k_train_baseline_v23c.py`

This job trains Baseline V2-3c, a transition feature pruning experiment built on top of Baseline V2-2.

Inputs:

- `configs/baseline_v23c_features.json`
- `data/processed/training/e1_train_2022_10_10/purchase_propensity_30d/`
- `data/processed/training/e1_valid_2022_11_09/purchase_propensity_30d/`
- `data/processed/events/search_query/`
- `data/processed/events/add_to_cart/`

V2-3c keeps the same temporal split, Logistic Regression model class, class weighting, median imputation, and TopK evaluation pattern as E1/V2-1/V2-2. It adds only:

- `recent_search_then_cart_flag`

It intentionally excludes the other V2-3b transition features:

- `search_before_cart_count`
- `search_to_cart_rate`

### How to Run

```bash
python jobs/05k_train_baseline_v23c.py
```

Outputs:

```text
data/models/purchase_propensity_baseline_v23c/
artifacts/modeling/baseline_v2/v23c_feature_selection.md
artifacts/modeling/baseline_v2/v23c_feature_definition.md
artifacts/modeling/baseline_v2/v23c_feature_distribution.csv
artifacts/modeling/baseline_v2/v23c_positive_rate.csv
artifacts/modeling/baseline_v2/v23c_coefficient.csv
artifacts/modeling/baseline_v2/v23c_summary.json
artifacts/modeling/baseline_v2/v23c_evaluation.md
```

This job does not overwrite previous baselines, persist row-level predictions, write raw client IDs, write raw query text, write product names, row-level transitions, or row-level examples.

## `05l_train_e6_velocity.py`

This E6 job runs an additive trend/velocity feature experiment on top of the frozen Baseline V2-2 feature set.

Inputs:

- `configs/e6_velocity_features.json`
- `data/processed/training/e1_train_2022_10_10/purchase_propensity_30d/`
- `data/processed/training/e1_valid_2022_11_09/purchase_propensity_30d/`
- processed event tables under `data/processed/events/`

E6 adds only derived velocity features:

- `cart_velocity_30d_vs_90d`
- `cart_delta_30d_90d`
- `buy_velocity_30d_vs_90d`
- `buy_delta_30d_90d`
- `search_velocity_30d_vs_90d`
- `activity_intensity_ratio`

It does not modify V2-2 features, labels, temporal splits, preprocessing outputs, production models, model architecture, sequence features, graph features, search-to-cart transitions, query text features, or embeddings.

### How to Run

```bash
python jobs/05l_train_e6_velocity.py
```

Outputs:

```text
data/models/purchase_propensity_e6_velocity/
artifacts/modeling/e6_trend_velocity/E6_feature_definitions.md
artifacts/modeling/e6_trend_velocity/e6_feature_distribution.csv
artifacts/modeling/e6_trend_velocity/e6_model_evaluation.md
artifacts/modeling/e6_trend_velocity/e6_ablation_analysis.md
artifacts/modeling/e6_trend_velocity/e6_ablation_results.csv
artifacts/modeling/e6_trend_velocity/e6_summary.json
```

Adoption requires PR-AUC improvement of at least 0.5% or Lift@5% improvement of at least 0.5% versus V2-2. Otherwise, E6 remains investigational.

## `05m_e6_pruning.py`

This E6.1 job prunes the E6 trend/velocity feature set to identify the smallest useful subset.

Inputs:

- `configs/e6_velocity_features.json`
- `data/processed/training/e1_train_2022_10_10/purchase_propensity_30d/`
- `data/processed/training/e1_valid_2022_11_09/purchase_propensity_30d/`
- processed event tables under `data/processed/events/`

The job keeps Baseline V2-2 features unchanged and compares only predefined E6 feature subsets:

- full E6
- E6 without `activity_intensity_ratio`
- E6 without search velocity
- buy plus cart velocity only
- buy velocity only

It does not create new features, modify V2-2 features, change labels, change temporal splits, tune thresholds, calibrate scores, change the model class, or persist row-level predictions.

### How to Run

```bash
python jobs/05m_e6_pruning.py
```

Outputs:

```text
artifacts/modeling/e6_trend_velocity/e6_pruning_evaluation.md
artifacts/modeling/e6_trend_velocity/e6_feature_ablation_summary.csv
artifacts/modeling/e6_trend_velocity/e6_pruning_summary.json
```

The decision rule keeps only E6 features that contribute at least 0.2% PR-AUC or improve Lift@5%, with preference for the simplest model that preserves TopK gain.

## `05n_train_baseline_v24.py`

This job trains Baseline V2-4, a consolidated model candidate using the frozen Baseline V2-2 feature set plus the selected E6.1 velocity features.

Inputs:

- `configs/baseline_v24_features.json`
- `artifacts/modeling/e6_trend_velocity/e6_summary.json`
- `data/processed/training/e1_train_2022_10_10/purchase_propensity_30d/`
- `data/processed/training/e1_valid_2022_11_09/purchase_propensity_30d/`
- processed event tables under `data/processed/events/`

V2-4 adds only:

- `cart_velocity_30d_vs_90d`
- `cart_delta_30d_90d`
- `buy_velocity_30d_vs_90d`
- `buy_delta_30d_90d`
- `search_velocity_30d_vs_90d`

It excludes the noisy E6 feature:

- `activity_intensity_ratio`

The job compares:

- Baseline V2-2
- V2-2 plus full E6
- V2-4, using the E6.1 pruned feature set

It keeps the same temporal split, Logistic Regression model class, class weighting, median imputation, and TopK evaluation pattern as V2-2/E6. It does not modify V2-2 features, add new features beyond E6.1, change labels, change temporal splits, tune thresholds, calibrate scores, add sequence/graph features, or persist row-level predictions.

### How to Run

```bash
python jobs/05n_train_baseline_v24.py
```

Outputs:

```text
data/models/purchase_propensity_baseline_v24/
artifacts/modeling/baseline_v2/v24_consolidation_evaluation.md
artifacts/modeling/baseline_v2/v24_feature_comparison.csv
artifacts/modeling/baseline_v2/v24_ablation_summary.md
artifacts/modeling/baseline_v2/v24_summary.json
```

If V2-4 improves PR-AUC or Lift@5 versus V2-2, it is marked as a candidate for production merge. The final production merge should still be reviewed before serving export or API changes.

## `05o_e9_final_benchmark.py`

This E9 job performs the final model selection benchmark between the frozen V2-2 baseline and the V2-4 candidate model.

Inputs:

- `configs/baseline_v24_features.json`
- `data/models/purchase_propensity_baseline_v22/`
- `data/models/purchase_propensity_baseline_v24/`
- `data/processed/training/e1_valid_2022_11_09/purchase_propensity_30d/`
- processed event tables under `data/processed/events/`

The job loads existing trained Spark ML models and scores the temporal validation snapshot in memory. It computes:

- ROC-AUC and PR-AUC
- Precision@1%, Precision@5%, Precision@10%
- Lift@1%, Lift@5%, Lift@10%
- segment metrics for high/low activity users and new/returning users
- TopK overlap between V2-2 and V2-4
- aggregate score distribution summaries

It does not retrain models, change features, change preprocessing, change labels, tune thresholds, calibrate scores, change model architecture, persist row-level predictions, or write raw client IDs.

### How to Run

```bash
python jobs/05o_e9_final_benchmark.py
```

Outputs:

```text
artifacts/modeling/baseline_v2/v2_e9_final_benchmark_report.md
artifacts/modeling/baseline_v2/v2_e9_segment_analysis.csv
artifacts/modeling/baseline_v2/v2_e9_topk_overlap_analysis.csv
artifacts/modeling/baseline_v2/v2_e9_score_distribution_summary.json
```

The final report recommends `PROMOTE V2-4` or `KEEP V2-2` based only on PR-AUC, Lift@5, and segment stability.

## `06_batch_score.py`

This job runs Phase 6 batch scoring for the purchase propensity 30-day task.

Inputs:

- `data/processed/features/user_behavior_features/`
- `data/models/purchase_propensity_baseline/`
- `configs/pipeline_config.yaml`

Scoring approach:

- Loads the Phase 5 Spark ML `PipelineModel`.
- Reads the Phase 3 feature table.
- Filters to clients where `is_eligible_purchase_propensity = 1`.
- Extracts class-1 probability as `prediction_score`.
- Creates `prediction_label` with the configured score threshold.

### How to Run

```powershell
python jobs/06_batch_score.py
```

With explicit config:

```powershell
python jobs/06_batch_score.py --config configs/pipeline_config.yaml
```

Optional arguments:

```powershell
python jobs/06_batch_score.py --feature-input data/processed/features/user_behavior_features --model-input data/models/purchase_propensity_baseline --score-output data/processed/scoring/purchase_propensity_scores --artifacts-base artifacts --model-version baseline_lr_v1 --score-threshold 0.5
```

WSL/Linux uses the same command from the project root:

```bash
python jobs/06_batch_score.py
```

### Score Output

The job writes the Spark Parquet score table to:

```text
data/processed/scoring/purchase_propensity_scores/
```

Score columns:

- `client_id`
- `prediction_score`
- `prediction_label`
- `model_version`
- `scored_at`

Sanitized aggregate artifacts:

```text
artifacts/scoring/scoring_summary.json
artifacts/scoring/score_distribution.csv
artifacts/scoring/scoring_validation.csv
artifacts/scoring/scoring_notes.md
```

Validation checks include row counts, duplicate `client_id`, score range, prediction label values, model version, score timestamp, model load status, score write status, and exclusion of labels or target-window metadata from the scoring output.

Latest validated output:

- input feature rows: 2,810,342
- eligible scoring rows: 2,149,796
- score rows: 2,149,796
- model version: `baseline_lr_v1`
- score threshold: 0.5
- predicted positives: 516,759
- predicted positive rate: 0.240376
- minimum prediction score: 0.000000
- maximum prediction score: 1.000000
- average prediction score: 0.350679
- validation checks: all pass
- leakage validation: pass

This job does not implement API serving, create an online endpoint, retrain a model, write row-level score artifacts for commit, or commit model binaries. Score data under `data/processed/` and model data under `data/models/` are ignored by default and should not be committed.

### Batch Scoring Runtime Note

The scoring job uses Spark to read feature Parquet inputs, load the Spark ML model, and write score Parquet outputs. On Windows local Spark, reading or writing local Parquet/model directories may require a proper Hadoop/winutils setup. If local Windows Spark fails, run the job in WSL/Linux or configure Hadoop properly.

## Phase 7/8 Jobs

The following jobs support or are planned for the next product/demo phases.

### `07_export_serving_scores.py`

Phase 7A job for exporting Phase 6 batch scores to a local SQLite lookup store for the Phase 7 API.

Input:

- `data/processed/scoring/purchase_propensity_scores/`

Output:

- `data/serving/purchase_propensity_scores.sqlite`

SQLite table:

```sql
CREATE TABLE scores (
  client_id TEXT PRIMARY KEY,
  prediction_score REAL NOT NULL,
  prediction_label INTEGER NOT NULL,
  model_version TEXT NOT NULL,
  scored_at TEXT NOT NULL
);
```

The job creates an index on `client_id` for lookup.

### How to Run

Full export:

```powershell
python jobs/07_export_serving_scores.py
```

Local API/cache demo export:

```powershell
python jobs/07_export_serving_scores.py --limit 500000
```

Optional arguments:

```powershell
python jobs/07_export_serving_scores.py --input-path data/processed/scoring/purchase_propensity_scores --output-db data/serving/purchase_propensity_scores.sqlite --limit 500000 --batch-size 10000
```

WSL/Linux uses the same command from the project root:

```bash
python jobs/07_export_serving_scores.py --limit 500000
```

The generated SQLite database is local serving data and should not be committed. The job does not create row-level artifacts under `artifacts/`.

### Serving Export Runtime Note

The serving export job uses Spark to read the Phase 6 scoring Parquet output and Python SQLite writes for the local lookup store. The local API/cache demo DB contains 500000 rows. On Windows local Spark, reading local Parquet directories may require a proper Hadoop/winutils setup; run refresh exports in WSL/Linux or configure Hadoop properly.

### `07_export_model_metadata.py`

Phase 7B job for exporting lightweight Logistic Regression metadata for direct manual feature-input prediction.

Input:

- `data/models/purchase_propensity_baseline/`
- `artifacts/modeling/feature_processing_summary.csv`
- `artifacts/modeling/baseline_model_summary.json`

Local output:

- `data/serving/model_metadata/baseline_lr_v1.json`

Sanitized artifact:

- `artifacts/serving/model_metadata_summary.json`

Local metadata content:

- feature order
- imputation values
- coefficients
- intercept
- chosen threshold

This export should be sanitized and small enough for API/UI use. It should not include raw rows, real client IDs, raw predictions, model training data, or local runtime paths. The API/UI can use this metadata to compute a logistic sigmoid score without loading Spark or running Spark ML inside a request path.

Run:

```powershell
python jobs/07_export_model_metadata.py
```

### `08_train_model_variants.py`

Planned Phase 8 job for offline Spark model variant training and experiment comparison.

Planned variant parameters:

- `regParam`
- `elasticNetParam`
- `maxIter`
- threshold
- class weighting on/off

Planned artifacts:

- sanitized aggregate metrics under `artifacts/experiments/`
- model version metadata
- parameter summary
- ROC-AUC and PR-AUC
- TopK precision, recall, and lift
- confusion matrix
- selected threshold or mentor review status

This job should run offline. It should not be called by API request paths or by the demo UI as a default action, and it should not commit raw predictions, real client IDs, model binaries, or row-level experiment output.
