# Baseline V2-3a Feature Definitions

## Scope

Baseline V2-3a implements only search quick-win features. It keeps the V2-2 temporal split, labels, model class, median imputation, class weighting, and evaluation unchanged.

## Raw Search Features Replaced Relative To V2-2

- `search_query_count`
- `distinct_search_days`
- `days_since_last_search_query`
- `search_query_count_30d`
- `search_query_count_90d`

## New Features

| Feature | Type | Definition | Rationale | Null handling | Leakage review |
|---|---|---|---|---|---|
| `search_count_bucket` | Integer bucket | 0 searches = 0; 1 search = 1; 2-5 searches = 2; 6-20 searches = 3; >20 searches = 4. | Reduces heavy-tail sensitivity and keeps search intensity interpretable. | Null search count is treated as 0. | Uses pre-cutoff aggregate search count only. |
| `search_recency_bucket` | Integer bucket | No search = 0; <= 16 days = 1; <= 41 days = 2; <= 72 days = 3; > 72 days = 4. | Tests whether freshness is more robust than raw recency. Thresholds are derived from the temporal training snapshot. | Null recency or no search is bucket 0. | Uses pre-cutoff search recency only. |
| `recent_search_flag` | Binary indicator | 1 if the user searched within 30 days before cutoff, else 0. | Business-friendly recent-intent flag aligned with the existing 30-day window. | Null recency or no search is 0. | Uses pre-cutoff search recency only. |

## Explicit Non-Goals

- No search-to-cart transition features.
- No normalized search intensity ratios.
- No trend features.
- No session features.
- No query semantics.
- No query embeddings.
- No category transition features.
- No model class, label, temporal split, threshold, or calibration changes.

## Privacy

The new features use only numeric pre-cutoff aggregate search counts and recency. No raw query text, raw client IDs, product names, row-level examples, or row-level predictions are persisted.
