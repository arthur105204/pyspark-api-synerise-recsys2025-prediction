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
