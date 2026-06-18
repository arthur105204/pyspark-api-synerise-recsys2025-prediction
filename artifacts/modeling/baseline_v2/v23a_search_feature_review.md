# Baseline V2-3a Search Feature Review

## Scope

This review documents the current search feature distribution before training Baseline V2-3a. It is aggregate-only and does not include raw search text or row-level examples.

## Current Search Features

| Feature | Non-null rate | Distinct | Mean | Stddev | Min | P50 | P90 | P95 | P99 | Max | Zero rate | Interpretation |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| search_query_count | 1.000000 | 592 | 3.170777 | 14.9715 | 0 | 0.0 | 8.0 | 15.0 | 49.0 | 2482 | 0.717323 | Heavy-tailed count feature |
| distinct_search_days | 1.000000 | 79 | 0.558373 | 1.633368 | 0 | 0.0 | 1.0 | 2.0 | 7.0 | 104 | 0.717323 | Sparse recency/day feature |
| days_since_last_search_query | 0.282677 | 110 | 45.066157 | 31.765646 | 1 | 41.0 | 92.0 | 99.0 | 107.0 | 109 | 0.000000 | Sparse recency/day feature |
| search_query_count_30d | 1.000000 | 274 | 0.930721 | 5.460077 | 0 | 0.0 | 1.0 | 5.0 | 20.0 | 773 | 0.885120 | Heavy-tailed count feature |
| search_query_count_60d | 1.000000 | 403 | 1.838887 | 9.430228 | 0 | 0.0 | 4.0 | 10.0 | 33.0 | 1401 | 0.812179 | Heavy-tailed count feature |
| search_query_count_90d | 1.000000 | 524 | 2.71772 | 13.02993 | 0 | 0.0 | 6.0 | 14.0 | 44.0 | 2217 | 0.748995 | Heavy-tailed count feature |

## V2-3a Bucket Thresholds

- `search_count_bucket`: 0, 1, 2-5, 6-20, >20 searches.
- `search_recency_bucket`: no search, <= 16 days, <= 41 days, <= 72 days, > 72 days.
- `recent_search_flag`: 1 when search recency is <= 30 days, else 0.

## Bucket Target Relationship On Training Snapshot

| Bucket feature | Bucket | Row count | Positive count | Positive rate |
|---|---:|---:|---:|---:|
| search_count_bucket | 0 | 1222017 | 34190 | 0.027978 |
| search_count_bucket | 1 | 92316 | 4105 | 0.044467 |
| search_count_bucket | 2 | 174878 | 9526 | 0.054472 |
| search_count_bucket | 3 | 152831 | 12187 | 0.079742 |
| search_count_bucket | 4 | 61539 | 10629 | 0.172720 |
| search_recency_bucket | 0 | 1222017 | 34190 | 0.027978 |
| search_recency_bucket | 1 | 121189 | 20386 | 0.168217 |
| search_recency_bucket | 2 | 124041 | 8580 | 0.069171 |
| search_recency_bucket | 3 | 117608 | 4652 | 0.039555 |
| search_recency_bucket | 4 | 118726 | 2829 | 0.023828 |
| recent_search_flag | 0 | 1507873 | 44223 | 0.029328 |
| recent_search_flag | 1 | 195708 | 26414 | 0.134966 |

## Interpretation

Current search features are behaviorally meaningful but noisy. Count features have high maximum values and search-day features overlap with general activity. V2-3a tests whether coarse buckets and a recent-search flag capture search intent more robustly than raw count/recency values.

## Privacy

This artifact contains aggregate feature statistics only. It does not include raw client IDs, raw query text, product names, row-level scores, row-level predictions, or row-level examples.
