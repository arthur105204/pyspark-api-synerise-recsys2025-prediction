# PySpark Customer Behavior Scoring Project

## 1. Problem Statement

Customer behavior scoring means converting customer activity records into user-level signals that can support prediction. Event logs can be aggregated into features such as visit frequency, cart activity, purchase behavior, search activity, recency, and interaction trends.

The project aims to build a customer behavior scoring pipeline from event logs. The pipeline will use PySpark to inspect, process, and aggregate behavioral data into user-level features. The MVP will focus on a simple prediction task first, then later expose prediction results through an API.

The planned final outcome is a practical pipeline that can process customer behavior data, create user-level features, train a simple model, generate batch prediction results, and support API lookup by user/client id.

## 2. Why This Project Matters

Behavior scoring is useful because it turns raw interaction history into prediction-ready information. It can support customer churn prediction, propensity scoring, campaign targeting, personalized user analysis, and a user-level prediction API.

From an engineering perspective, the project is also useful because it practices scalable data processing on event logs, including schema inspection, joins, aggregations, feature creation, batch scoring, and later API serving.

## 3. Project Direction

Chosen direction:
Customer behavior scoring from event logs.

Expected final direction:

```text
Raw event logs
-> PySpark processing
-> EDA
-> preprocessing
-> feature engineering
-> model training
-> batch prediction table
-> API lookup
```

The first version stays MVP-focused before adding advanced recommendation, embedding, or challenge-reproduction methods.

## 4. Dataset Understanding

Dataset selected:
Synerise RecSys 2025.

Raw files currently found in `data/raw/`:

| File | Size | Inferred format | Spark-readable | Verified schema | Row count status |
| --- | ---: | --- | --- | --- | --- |
| `data/raw/synerise_dataset/add_to_cart.parquet` | 100.54 MB | Parquet | Yes | `client_id: long`, `timestamp: string`, `sku: long` | 7,541,117 rows |
| `data/raw/synerise_dataset/page_visit.parquet` | 1.87 GB | Parquet | Yes | `client_id: long`, `timestamp: string`, `url: long` | 199,451,980 rows |
| `data/raw/synerise_dataset/product_buy.parquet` | 30.05 MB | Parquet | Yes | `client_id: long`, `timestamp: string`, `sku: long` | 2,318,502 rows |
| `data/raw/synerise_dataset/product_properties.parquet` | 64.24 MB | Parquet | Yes | `sku: long`, `category: long`, `price: long`, `name: string` | 1,534,050 rows |
| `data/raw/synerise_dataset/remove_from_cart.parquet` | 34.84 MB | Parquet | Yes | `client_id: long`, `timestamp: string`, `sku: long` | 2,688,894 rows |
| `data/raw/synerise_dataset/search_query.parquet` | 335.74 MB | Parquet | Yes | `client_id: long`, `timestamp: string`, `query: string` | 13,223,769 rows |

What is known so far:

- The dataset is currently available as six Spark-readable Parquet files.
- The main event tables use `client_id` and `timestamp`.
- Product-related event tables use `sku`.
- Product metadata is available in `product_properties.parquet` with `sku`, `category`, `price`, and `name`.
- The EDA job was rerun in full-count mode, and all six Spark-readable Parquet tables were counted successfully.
- The metadata artifact is intentionally sanitized. It records table-level schema and size information only, without absolute local paths or row-level data.

What still needs to be verified:

- Duplicate behavior, skew, and unusual values: To be verified during preprocessing or targeted follow-up EDA.
- Target/label availability: To be verified.
- Whether the provisional purchase propensity target remains best after preprocessing validation.

Potential entities to look for during inspection:

- client/user
- event
- item/product
- timestamp
- target/label if available

Initial schemas, exact row counts, timestamp parsing, and tracked null counts are verified by EDA. Duplicate/skew checks and modeling labels remain TBD.

## 5. Why PySpark Is Used

The dataset may be processable with local tools, but PySpark is used here to practice scalable data processing patterns such as reading event logs, schema inspection, aggregation, joins, window-based features, and batch scoring.

Spark concepts planned for this project include:

- reading event logs
- schema inspection
- DataFrame transformations
- joins
- aggregations
- window-based features
- batch scoring

## 6. Planned Pipeline

| Stage | Objective | Input | Process | Output | What to review |
| ----- | --------- | ----- | ------- | ------ | -------------- |
| Problem statement | Define the prediction-oriented project goal | Internship direction and dataset context | Frame event logs as user-level scoring inputs | Clear behavior scoring objective | Whether the project direction is useful and explainable |
| Dataset understanding | Identify available data and possible entities | Files under `data/raw/` | Inventory files, inspect archive members, identify formats | Raw data summary and dataset notes | Whether required entities appear to exist |
| Data ingestion | Make Spark-readable input tables available | Parquet files under `data/raw/synerise_dataset/` | Read Parquet with Spark and inspect schemas | Spark DataFrames or staged raw tables | Schema, row counts, data types, and table relationships |
| EDA | Understand data quality and behavior patterns | Spark-readable raw tables | Count rows, inspect nulls, timestamp ranges, event distributions, skew | EDA statistics and observations | Data quality issues and useful behavior signals |
| Preprocessing | Clean and standardize source tables | Raw Spark DataFrames | Normalize timestamps, handle duplicates/nulls, filter invalid records | Clean intermediate tables | Cleaning rules and possible information loss |
| Feature engineering | Build user-level predictive signals | Clean event and metadata tables | Aggregate events by user, create recency/frequency/activity features | User-level feature table | Feature meaning, leakage risk, and null defaults |
| Label definition | Define the first supervised target | Event timelines and feature table | Choose churn or propensity logic, separate feature and label windows | Training dataset with label | Whether the label is valid and mentor-approved |
| Modeling | Train a simple baseline model | Labeled training table | Encode features, split train/validation, train baseline model | Model artifact, metrics, and result table | Model validity, metrics, and tuning priorities |
| Batch scoring | Generate prediction outputs for serving | Feature table and trained model | Score users in batch and store latest predictions | Batch prediction table | Prediction schema and refresh logic |
| API serving | Return scores by user/client id | Batch prediction table | Implement lookup endpoint and response validation | API endpoint | Request/response format and error cases |
| Load/stress test | Check API behavior under repeated requests | Running API | Run basic repeated request tests | Latency and stability notes | Whether performance is acceptable for demo |

## 7. Project Decisions

### Decision 1: Problem Direction

Chosen:
Customer behavior scoring from event logs.

Reason:
This matches the dataset structure and the internship requirement to practice PySpark preprocessing, modeling, and API prediction serving.

Alternatives considered:

- Pure recommendation
- Universal behavioral embedding
- Churn/propensity scoring

Default choice:
Use business target selection EDA to choose the MVP target. Current provisional choice is purchase propensity with a 30-day target window.

### Decision 2: Dataset

Chosen:
Synerise RecSys 2025.

Reason:
The dataset contains customer behavior event logs, timestamps, product metadata, and enough size/complexity to justify PySpark practice.

Limitation:
The dataset is not Spark-only. It may be processed with other local tools, but PySpark is used to practice scalable data processing patterns.

### Decision 3: Processing Approach

Chosen:
Use PySpark local mode first.

Reason:
Local mode is easier to develop and explain, while still using Spark concepts such as schema inspection, DataFrame operations, joins, aggregations, and window-based features.

### Decision 4: MVP Scope

MVP:

- Raw data inspection
- EDA
- User-level feature table
- Purchase propensity model
- Batch prediction table
- Simple API lookup

Out of scope for MVP:

- Deep learning embeddings
- Full RecSys challenge reproduction
- Real-time Spark serving
- Distributed cluster deployment

### Decision 5: Data Packaging and Ingestion

Chosen:
Inspect the available raw files first, then use the Spark-readable Parquet tables as ingestion inputs.

Reason:
Spark reads structured files such as Parquet, CSV, and JSON. The current dataset files are available as Parquet tables, so they can be inspected directly in local Spark mode. If the dataset is later provided as an archive, archive handling should happen before Spark table ingestion.

## 8. Detailed Reporting Template for Future Stages

This section defines how future project updates should be documented for mentor review.

### 8.1 EDA Reporting Template

For each EDA result, document:

- Question
- Input table/columns
- PySpark operation used
- Result
- Interpretation
- Follow-up decision

Example EDA questions:

- How many events are in each table?
- How many unique clients exist?
- What is the timestamp range?
- Which event type dominates?
- Are there missing values?
- Are there skewed users/items?

### 8.2 Feature Engineering Reporting Template

For each feature group, document:

- Feature group name
- Source table
- Input columns
- Transformation logic
- Output columns
- Why the feature may help prediction
- Null/default handling

Example format:

| Feature group | Source | Input columns | Output columns | Logic | Reason |
| ------------- | ------ | ------------- | -------------- | ----- | ------ |
| TBD | TBD | TBD | TBD | TBD | TBD |

### 8.3 Modeling Reporting Template

For each model experiment, document:

- Experiment name
- Prediction task
- Label definition
- Training input
- Feature columns
- Categorical encoding
- Numerical preprocessing
- Model selected
- Hyperparameters
- Train/validation split
- Metrics
- Results
- Observations
- Possible fine-tuning
- Mentor review questions

Example format:

| Item | Detail |
| ---- | ------ |
| Task | Churn prediction |
| Label | TBD |
| Input table | TBD |
| Features | TBD |
| Model | TBD |
| Parameters | TBD |
| Metrics | TBD |
| Result | TBD |
| Fine-tuning options | TBD |

### 8.4 API Reporting Template

For API implementation later, document:

- Endpoint
- Input parameters
- Prediction source table
- Output schema
- Example response
- Error cases
- Load/stress test result
- Cache strategy if used

## EDA Milestone Summary

Objective:
Understand the verified raw tables, core columns, timestamp quality, missing values, and table relationships before preprocessing or feature engineering.

Input tables:

- `add_to_cart`
- `page_visit`
- `product_buy`
- `product_properties`
- `remove_from_cart`
- `search_query`

Process:
The EDA job reads the raw Parquet tables with PySpark in local mode, computes sanitized table-level metrics, and writes structured outputs without row-level samples. The job was rerun in full-count mode.

Output artifacts:

- `artifacts/eda/eda_summary.json`
- `artifacts/eda/table_overview.csv`
- `artifacts/eda/event_table_overview.csv`
- `artifacts/eda/product_table_overview.csv`
- `artifacts/eda/column_overview.csv`

The EDA artifacts were split into compact general overview and schema-specific views to avoid sparse columns across heterogeneous input tables.

Key findings:

- All six raw tables are Spark-readable.
- Core event tables contain `client_id` and `timestamp`.
- Product event tables contain `sku`.
- `product_properties` contains `sku`, `category`, `price`, and `name`.
- Null counts are zero for tracked core columns in this EDA run.
- Timestamp parsing succeeded for all non-null timestamp values in the event tables.
- All six Spark-readable Parquet tables were counted successfully in full-count mode.
- The earlier deferred row counts for `page_visit` and `search_query` were caused by the safe-mode strategy, not by Spark readability limitations.

Compact table overview:

| Table | Row count | Main columns | Role in pipeline |
| --- | ---: | --- | --- |
| `page_visit` | 199,451,980 | `client_id`, `timestamp`, `url` | High-volume browsing behavior |
| `search_query` | 13,223,769 | `client_id`, `timestamp`, `query` | Search behavior signal |
| `add_to_cart` | 7,541,117 | `client_id`, `timestamp`, `sku` | Cart intent behavior |
| `remove_from_cart` | 2,688,894 | `client_id`, `timestamp`, `sku` | Cart reversal behavior |
| `product_buy` | 2,318,502 | `client_id`, `timestamp`, `sku` | Purchase behavior and target selection input |
| `product_properties` | 1,534,050 | `sku`, `category`, `price`, `name` | Product metadata for category/price features |

### Finding: Event Tables Are Spark-Readable

Input:
All six raw Parquet tables.

Process:
PySpark `read.parquet` was applied to each expected table path.

Result:
All six tables returned `spark_readable = true` in `artifacts/eda/table_overview.csv`.

Interpretation:
The project can proceed using Spark DataFrames directly from the raw Parquet files.

Next decision:
Confirm whether preprocessing should operate directly from these raw tables or write cleaned intermediate outputs.

### Finding: Core Event Columns Are Available

Input:
Schemas from `add_to_cart`, `page_visit`, `product_buy`, `remove_from_cart`, and `search_query`.

Process:
The EDA job recorded schema field names and data types for each table.

Result:
Event tables include `client_id` and `timestamp`. Product-related event tables include `sku`; page visits include `url`; search behavior includes `query`.

Interpretation:
`client_id` is a strong candidate user key, `timestamp` is the event-time field, and `sku` supports product-level joins for cart and purchase behavior.

Next decision:
Review whether `client_id`, `timestamp`, and `sku` should be accepted as the core preprocessing columns.

### Finding: Full-Count Row Counts Are Available

Input:
`row_count` and `row_count_status` from `artifacts/eda/table_overview.csv`.

Process:
The EDA job was rerun with `--full-count`, which deliberately computes exact row counts for every readable table.

Result:
All six row counts are available: `page_visit` (199,451,980), `search_query` (13,223,769), `add_to_cart` (7,541,117), `remove_from_cart` (2,688,894), `product_buy` (2,318,502), and `product_properties` (1,534,050).

Interpretation:
The largest table is `page_visit`, followed by `search_query`. Earlier deferred counts were safe-mode behavior, not a Spark readability limitation.

Next decision:
Use these volumes to plan preprocessing actions carefully, especially for `page_visit`.

### Finding: Timestamp Parsing Is Clean

Input:
`timestamp` columns in the event tables.

Process:
The EDA job parsed timestamps with PySpark `to_timestamp`, then counted parse successes and failures.

Result:
All event tables with `timestamp` had zero null timestamps and zero timestamp parse failures. The observed timestamp range spans from 2022-06-23 to 2022-12-08.

Interpretation:
Timestamp fields appear suitable for time-window analysis, churn-window definition, and recency/frequency features.

Next decision:
Choose the feature window and target window strategy before label definition.

### Finding: Product Metadata Is Useful for Features

Input:
`product_properties` columns `sku`, `category`, `price`, and `name`.

Process:
The EDA job computed row count, approximate distinct counts, null counts, and safe numeric price summary statistics without outputting product names.

Result:
`product_properties` has 1,534,050 rows, approximately 1,369,522 distinct SKUs, approximately 6,347 categories, zero nulls in tracked metadata columns, and price values from 0 to 99 with average 47.8629 and median 47.0.

Interpretation:
Product metadata can support category and price-based feature groups after preprocessing.

Next decision:
Decide whether product metadata should be joined before aggregation or after event-level cleaning.

Open questions:

- Are approximate distinct client counts sufficient for planning, or should exact distinct counts be computed for selected tables?
- Should duplicate and skew checks be added before or during preprocessing?
- Should purchase propensity remain the MVP target after preprocessing validation?
- What time window should define features and labels?

Mentor review points:

- Confirm whether `client_id`, `timestamp`, and `sku` are acceptable core columns.
- Confirm whether full-count EDA is enough to begin preprocessing.
- Review whether product category and price should be used in the MVP feature set.
- Review whether the first modeling task should be purchase propensity.

Next decision before feature engineering:
Move to preprocessing only after the core columns, timestamp strategy, and first target direction are reviewed.

### Business Target Selection EDA

Objective:
Compare multiple business scoring targets that fit the available e-commerce behavior logs before selecting the first MVP target.

The goal of this EDA step is not to force a churn prediction task, but to compare multiple business scoring targets that fit the available e-commerce behavior logs. The first MVP target should be selected based on data coverage, label balance, implementation complexity, and business actionability.

Candidate targets compared:

- Purchase propensity: which active clients are likely to purchase in the target window.
- Cart conversion: among clients who showed cart intent, who will convert to purchase in the target window.
- Purchase-based churn: among clients who purchased before, who will not purchase again in the target window.

Input:
Primary inputs were `product_buy` and `add_to_cart`. `page_visit` and `search_query` were skipped by default for this decision step because the default target comparison can be computed from purchase and cart behavior without scanning the largest table.

Process:
The job evaluated 14-day, 30-day, and 45-day target windows using aggregate-only PySpark operations. It computed eligible clients, positive clients, negative clients, positive rate, implementation complexity, business value, and simple recommendation ranks. It did not write final labels or client-level outputs.

Target window comparison:

| Target days | History start | Cutoff date | Target end | History days |
| ---: | --- | --- | --- | ---: |
| 14 | 2022-06-23 | 2022-11-25 | 2022-12-08 | 155 |
| 30 | 2022-06-23 | 2022-11-09 | 2022-12-08 | 139 |
| 45 | 2022-06-23 | 2022-10-25 | 2022-12-08 | 124 |

Business target comparison:

| Target | Window | Eligible clients | Positive clients | Negative clients | Positive rate | Complexity | Business value | Rank |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: |
| Purchase propensity | 14 | 2,418,073 | 57,754 | 2,360,319 | 0.023884 | medium | high | 2 |
| Purchase propensity | 30 | 2,149,796 | 93,614 | 2,056,182 | 0.043546 | medium | high | 1 |
| Purchase propensity | 45 | 1,925,028 | 112,154 | 1,812,874 | 0.058261 | medium | high | 1 |
| Cart conversion | 14 | 2,114,526 | 47,724 | 2,066,802 | 0.022570 | low to medium | high | 4 |
| Cart conversion | 30 | 1,872,709 | 75,415 | 1,797,294 | 0.040271 | low to medium | high | 4 |
| Cart conversion | 45 | 1,672,381 | 89,320 | 1,583,061 | 0.053409 | low to medium | high | 3 |
| Purchase-based churn | 14 | 826,947 | 778,744 | 48,203 | 0.941710 | low | medium | 5 |
| Purchase-based churn | 30 | 739,219 | 661,550 | 77,669 | 0.894931 | low | medium | 5 |
| Purchase-based churn | 45 | 668,496 | 576,651 | 91,845 | 0.862609 | low | medium | 5 |

Business meaning:

- Purchase propensity is broad and suitable for ranking active users by likelihood to buy.
- Cart conversion is action-oriented for cart recovery, but its eligible cohort is narrower than purchase propensity.
- Purchase-based churn is clear and simple, but the positive class means churn and is highly dominant, which can make interpretation and baseline modeling less balanced.

Implementation complexity:

- Purchase propensity: medium, because it requires an active-user cohort and future purchase label.
- Cart conversion: low to medium, because the cohort is clearly defined by add-to-cart behavior.
- Purchase-based churn: low, because it uses only purchase history, but it has narrower business scope and stronger imbalance.

Recommended MVP target:
Based on the comparison, the recommended MVP target is purchase propensity with a 30-day target window because it has broad data coverage, high business actionability, and a better overall recommendation rank than the churn alternatives. The 45-day purchase propensity option has the same overall rank and a higher positive rate, so it remains a strong alternative for review. This decision remains provisional until preprocessing validates duplicate handling, time-window boundaries, and leakage-safe feature construction.

Provisional label definition:
Eligible clients have at least 1 `add_to_cart` or `product_buy` event before `cutoff_date`. Positive clients have at least 1 purchase from `cutoff_date` to `target_end`. Negative clients have no purchase in that target window.

Why other targets are alternatives, not discarded:

- Cart conversion remains useful if the MVP should focus specifically on cart recovery.
- Purchase-based churn remains useful for retention analysis, but it is narrower and more imbalanced in this dataset.
- The 45-day purchase propensity window remains a strong alternative if the team prefers a less sparse positive class.

Leakage prevention rule:
Features must only use events before `cutoff_date`. Labels must only use purchase events from `cutoff_date` to `target_end`. No target-window behavior should be used in feature calculations.

Open questions before preprocessing/feature engineering:

- Should the MVP use the recommended 30-day purchase propensity target or the 45-day purchase propensity alternative?
- Should the eligible cohort include only add-to-cart and purchase behavior, or later include search/page activity?
- Should duplicate and skew checks be added in preprocessing or targeted follow-up EDA?
- Should class imbalance handling be planned during modeling?

## Preprocessing Milestone Summary

Objective:
Prepare clean, standardized intermediate tables for later feature engineering and label construction while keeping the current MVP target as purchase propensity.

Input tables:

- `add_to_cart`
- `remove_from_cart`
- `product_buy`
- `search_query`
- `product_properties`

Deferred input:

- `page_visit` was deferred because it is the largest table and is not required for the initial purchase propensity preprocessing path.

Process:
The preprocessing job reads raw Parquet tables with PySpark, parses event timestamps, creates `event_ts` and `event_date`, adds an `event_type`, validates required columns, checks aggregate duplicate rates, and writes cleaned intermediate Parquet outputs with Spark. Product metadata is cleaned into modeling-safe columns only: `sku`, `category`, and `price`.

Output data:

- `data/processed/events/add_to_cart/`
- `data/processed/events/remove_from_cart/`
- `data/processed/events/product_buy/`
- `data/processed/events/search_query/`
- `data/processed/product_properties_clean/`

These processed data outputs are local pipeline outputs and are ignored by git.

Output artifacts:

- `artifacts/preprocessing/preprocessing_summary.json`
- `artifacts/preprocessing/table_validation.csv`
- `artifacts/preprocessing/duplicate_check_summary.csv`
- `artifacts/preprocessing/product_metadata_validation.csv`
- `artifacts/preprocessing/preprocessing_notes.md`

Validation results:

| Table | Input rows | Valid rows | Invalid rows | Timestamp parse failures | Write status |
| --- | ---: | ---: | ---: | ---: | --- |
| `add_to_cart` | 7,541,117 | 7,541,117 | 0 | 0 | success |
| `remove_from_cart` | 2,688,894 | 2,688,894 | 0 | 0 | success |
| `product_buy` | 2,318,502 | 2,318,502 | 0 | 0 | success |
| `search_query` | 13,223,769 | 13,223,769 | 0 | 0 | success |
| `product_properties` | 1,534,050 | 1,534,050 | 0 | not applicable | success |

Duplicate handling:

| Table | Duplicate key | Duplicate extra rows | Duplicate extra row rate |
| --- | --- | ---: | ---: |
| `add_to_cart` | `client_id`, `event_ts`, `sku` | 161,493 | 0.021415 |
| `remove_from_cart` | `client_id`, `event_ts`, `sku` | 153,588 | 0.057119 |
| `product_buy` | `client_id`, `event_ts`, `sku` | 380,903 | 0.164288 |
| `search_query` | `client_id`, `event_ts` | 787,135 | 0.059524 |

Interpretation:
The duplicate checks are aggregate diagnostics only. No duplicate row samples are persisted. Duplicate handling should be reviewed before feature engineering chooses whether repeated identical events should count as repeated behavior or be collapsed.

Product metadata handling:

`product_properties` has 1,534,050 input rows and 1,534,050 distinct SKUs in the preprocessing artifact. No duplicated SKU rows were detected in this run. The clean product metadata output excludes product names and keeps only `sku`, `category`, and `price`. The documented deterministic fallback rule is to group by `sku` and keep the most frequent category/price pair, with ties sorted by category and price.

Leakage-safe target setup:

The pipeline configuration records the provisional MVP target as purchase propensity with a 30-day target window. Preprocessing does not create final labels. Later feature construction must use only events before the cutoff date, while labels must use purchase behavior only in the target window.

Open questions:

- Should duplicate event rows be counted as repeated activity or deduplicated before feature aggregation?
- Should `search_query` remain included for Phase 3 features even though raw query text is not retained in shared artifacts?
- Should `page_visit` stay deferred for the MVP or be included after resource review?
- Are `sku`, `category`, and `price` sufficient product metadata columns for the first feature set?

Next decision before feature engineering:
Approve the cleaned event/product tables and duplicate handling strategy before moving to Phase 3 feature engineering.

## Feature Engineering Milestone Summary

Objective:
Create leakage-safe user-level feature tables for the provisional purchase propensity task.

Input processed tables:

- `data/processed/events/add_to_cart/`
- `data/processed/events/remove_from_cart/`
- `data/processed/events/product_buy/`
- `data/processed/events/search_query/`
- `data/processed/product_properties_clean/`

Deferred input:

- `page_visit` remains deferred and is not used in the Phase 3 feature table.

Leakage rule:
Features use events before the configured cutoff date, `2022-11-09`. Target-window events from `2022-11-09` through `2022-12-08` are not used in feature calculations. This phase does not create final labels.

Feature groups generated:

- Activity count features for add-to-cart, remove-from-cart, purchase, and search activity.
- Recency features using days since the last event of each processed event type.
- Distinct interaction features such as distinct SKU counts and distinct search days.
- Ratio features such as buy-to-cart, remove-to-cart, and search-to-cart ratios.
- Product metadata features using category and price from cleaned product metadata.
- Windowed 30-day, 60-day, and 90-day lookback count features.
- Eligible cohort indicator for the later purchase propensity label construction step.

Output feature table:

- `data/processed/features/user_behavior_features/`

Output artifacts:

- `artifacts/features/feature_summary.json`
- `artifacts/features/feature_catalog.csv`
- `artifacts/features/feature_validation.csv`
- `artifacts/features/feature_notes.md`

Validation results:

- Feature rows: 2,810,342
- Eligible cohort count: 2,149,796
- Eligible cohort rate: 0.764959
- Feature count: 36
- Search features included: True
- `page_visit` included: False
- Label-like columns detected: none

Selected feature validation examples:

| Feature | Null rate | Min | Max | Average |
| --- | ---: | ---: | ---: | ---: |
| `add_to_cart_count` | 0.000000 | 0 | 1,560 | 1.998546 |
| `product_buy_count` | 0.000000 | 0 | 644 | 0.626121 |
| `search_query_count` | 0.000000 | 0 | 3,264 | 3.602982 |
| `active_days_count` | 0.000000 | 1 | 125 | 1.629863 |
| `is_eligible_purchase_propensity` | 0.000000 | 0 | 1 | 0.764959 |

Null and fill strategy:
Count-style features are filled with 0. Recency features remain null when a client has no event of that type. Ratio features remain null when the denominator is 0.

Duplicate handling assumption:
Features are computed from processed rows as-is. Phase 2 duplicate diagnostics remain part of the review context, and the final duplicate policy is deferred for review before label construction or modeling.

Interpretation:
The generated feature table covers all clients observed in the processed event inputs before the cutoff. The eligible cohort indicator identifies clients with add-to-cart or purchase activity before cutoff, but it is not a final label. This keeps Phase 3 scoped to feature creation only.

Open questions before Phase 4 label construction:

- Should duplicate event rows count as repeated behavior, or should specific duplicate keys be collapsed before modeling?
- Are the current 36 features sufficient for a baseline purchase propensity model?
- Should `page_visit` remain deferred for the MVP?
- Should missing recency and ratio values stay null or be filled during modeling preparation?

Next decision before modeling:
Approve the feature table and duplicate/null assumptions, then proceed to Phase 4 label construction.

## 9. Risks and Open Questions

- Compressed/archive file handling: if the dataset is provided as an archive in another environment, the inner Parquet files must be made available before Spark table ingestion.
- Schema interpretation: column names and data types are initially verified, but table relationships and business meaning still need EDA review.
- Missing or unclear label: the first prediction target still needs to be selected from the available data.
- Possible data leakage: label windows and feature windows must be separated carefully.
- High-cardinality columns: user, item, category, or event attributes may require careful encoding or aggregation.
- Large file handling: full row counts and schema inference may be expensive on large files.
- First modeling task: purchase propensity is the provisional MVP target, pending preprocessing validation.

## 10. Current Milestone

Current milestone:
Phase 3: Feature Engineering.

Done in this milestone:

- feature engineering job
- user-level feature table generated locally
- sanitized feature validation artifacts
- pipeline configuration updated with cutoff and target dates
- README update
- jobs README update
- report feature engineering findings update
- PLAN phase status update

Not included yet:

- final label tables
- modeling
- API

## 11. Next Steps

1. Review feature engineering artifacts.
2. Decide duplicate handling before label construction and modeling.
3. Confirm whether missing recency and ratio values should stay null for modeling preparation.
4. Decide whether `page_visit` should remain deferred for the MVP.
5. Move to label construction only after feature review.
