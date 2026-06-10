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
| `data/raw/synerise_dataset/page_visit.parquet` | 1.87 GB | Parquet | Yes | `client_id: long`, `timestamp: string`, `url: long` | Skipped for safety because file is larger than 250 MB |
| `data/raw/synerise_dataset/product_buy.parquet` | 30.05 MB | Parquet | Yes | `client_id: long`, `timestamp: string`, `sku: long` | 2,318,502 rows |
| `data/raw/synerise_dataset/product_properties.parquet` | 64.24 MB | Parquet | Yes | `sku: long`, `category: long`, `price: long`, `name: string` | 1,534,050 rows |
| `data/raw/synerise_dataset/remove_from_cart.parquet` | 34.84 MB | Parquet | Yes | `client_id: long`, `timestamp: string`, `sku: long` | 2,688,894 rows |
| `data/raw/synerise_dataset/search_query.parquet` | 335.74 MB | Parquet | Yes | `client_id: long`, `timestamp: string`, `query: string` | Skipped for safety because file is larger than 250 MB |

What is known so far:

- The dataset is currently available as six Spark-readable Parquet files.
- The main event tables use `client_id` and `timestamp`.
- Product-related event tables use `sku`.
- Product metadata is available in `product_properties.parquet` with `sku`, `category`, `price`, and `name`.
- Some row counts were skipped by the inspection job because large files are above the configured 250 MB safe-count threshold.
- The metadata artifact is intentionally sanitized. It records table-level schema and size information only, without absolute local paths or row-level data.

What still needs to be verified:

- Full row counts for `page_visit.parquet` and `search_query.parquet`: To be verified when needed.
- Timestamp parsing format and time range: To be verified.
- Null counts, duplicate behavior, and unusual values: To be verified during EDA.
- Target/label availability: To be verified.
- Whether churn or propensity is the best first modeling task: To be verified after EDA.

Potential entities to look for during inspection:

- client/user
- event
- item/product
- timestamp
- target/label if available

Initial schemas are verified by the raw inspection job. Data quality details and modeling labels remain TBD after EDA.

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
Start with churn prediction as MVP, keep propensity scoring as an extension.

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
- Churn model
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

## 9. Risks and Open Questions

- Compressed/archive file handling: if the dataset is provided as an archive in another environment, the inner Parquet files must be made available before Spark table ingestion.
- Schema interpretation: column names and data types are initially verified, but table relationships and business meaning still need EDA review.
- Missing or unclear label: the first prediction target still needs to be selected from the available data.
- Possible data leakage: label windows and feature windows must be separated carefully.
- High-cardinality columns: user, item, category, or event attributes may require careful encoding or aggregation.
- Large file handling: full row counts and schema inference may be expensive on large files.
- First modeling task: churn prediction is the default MVP, but propensity scoring may be easier if the dataset provides clearer event outcomes.

## 10. Current Milestone

Current milestone:
Project foundation and raw data inspection.

Done in this milestone:

- one report file
- raw data inspection job
- README update
- `.gitignore` update

Not included yet:

- EDA
- preprocessing
- feature engineering
- modeling
- API

## 11. Next Steps

1. Run raw data inspection.
2. Review file inventory, schema availability, and readability.
3. Update dataset understanding based on inspection output.
4. Decide the first modeling task.
5. Move to the EDA milestone.
