# Baseline V2-3a Evaluation

## New Features

- `search_count_bucket`
- `search_recency_bucket`
- `recent_search_flag`

Raw search representation features replaced relative to V2-2:
- `search_query_count`
- `distinct_search_days`
- `days_since_last_search_query`
- `search_query_count_30d`
- `search_query_count_90d`

## Metric Comparison

| Metric | E1 temporal baseline | V2-1 | V2-2 | V2-3a | V2-3a vs V2-2 |
|---|---:|---:|---:|---:|---:|
| ROC-AUC | 0.835559 | 0.834273 | 0.834208 | 0.835559 | 0.16% |
| PR-AUC | 0.253155 | 0.254871 | 0.255374 | 0.253036 | -0.92% |
| Precision@1% | 0.478742 | 0.483766 | 0.484882 | 0.479719 | -1.06% |
| Precision@5% | 0.294837 | 0.296102 | 0.296158 | 0.294446 | -0.58% |
| Precision@10% | 0.213922 | 0.216174 | 0.216155 | 0.214578 | -0.73% |
| Lift@1% | 10.994062 | 11.109429 | 11.135066 | 11.016494 | -1.06% |
| Lift@5% | 6.770770 | 6.799825 | 6.801107 | 6.761797 | -0.58% |
| Lift@10% | 4.912611 | 4.964312 | 4.963885 | 4.927673 | -0.73% |

## Search Family Impact

Search quick-win features reduced at least one primary ranking metric enough to prefer V2-2 for now.

V2-3a tests only whether coarse search intensity, search freshness, and a recent-search flag are a better representation than raw search count/day/recency features. It does not test transition features, trend features, sessions, query semantics, query embeddings, or category-aware behavior.

## Complexity Added

- E1 feature count: 36
- V2-1 feature count: 31
- V2-2 feature count: 27
- V2-3a feature count: 25
- New quick-win features added: 3
- Raw search representation features replaced relative to V2-2: 5
- Train rows: 1,703,581
- Validation rows: 2,149,796
- Train positive rate: 0.041464
- Validation positive rate: 0.043546
- Model output: `data/models/purchase_propensity_baseline_v23a`

## Recommendation

Keep V2-2

## Privacy

Artifacts are aggregate-only. No raw client IDs, raw query text, product names, row-level scores, row-level prediction examples, or raw model internals are persisted in artifacts.
