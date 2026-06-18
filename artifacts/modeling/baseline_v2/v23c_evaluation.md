# Baseline V2-3c Evaluation

## Feature Change

Start from Baseline V2-2 and add only:

- `recent_search_then_cart_flag`

Removed from the V2-3b transition set:

- `search_before_cart_count`
- `search_to_cart_rate`

## Metric Comparison

| Metric | E1 temporal baseline | V2-2 | V2-3b | V2-3c | V2-3c vs V2-2 |
|---|---:|---:|---:|---:|---:|
| ROC-AUC | 0.835559 | 0.834208 | 0.834653 | 0.834634 | 0.05% |
| PR-AUC | 0.253155 | 0.255374 | 0.255093 | 0.255329 | -0.02% |
| Precision@1% | 0.478742 | 0.484882 | 0.485347 | 0.485487 | 0.12% |
| Precision@5% | 0.294837 | 0.296158 | 0.296679 | 0.296781 | 0.21% |
| Precision@10% | 0.213922 | 0.216155 | 0.215285 | 0.215439 | -0.33% |
| Lift@1% | 10.994062 | 11.135066 | 11.145748 | 11.148953 | 0.12% |
| Lift@5% | 6.770770 | 6.801107 | 6.813071 | 6.815421 | 0.21% |
| Lift@10% | 4.912611 | 4.963885 | 4.943909 | 4.947435 | -0.33% |

## Transition Signal Value

The single transition flag maintained temporal ranking metrics within a small tolerance relative to V2-2.

| Feature | Coefficient | Abs coefficient | Abs-coefficient rank among model features |
|---|---:|---:|---:|
| recent_search_then_cart_flag | 0.426632 | 0.426632 | 2 |

Distribution:

- Non-null rate: 1.000000
- Mean: 0.081686
- Zero rate: 0.918314
- Distinct values: 2

Positive-rate segmentation:

| Flag value | Row count | Positive count | Positive rate |
|---:|---:|---:|---:|
| 0 | 1564422 | 49380 | 0.031564 |
| 1 | 139159 | 21257 | 0.152753 |

## Redundancy Analysis

V2-3c removes the two noisier V2-3b transition features and keeps only the flag with the cleanest behavioral interpretation. This isolates whether recent search followed by cart activity is independently additive to the V2-2 baseline.

The experiment does not test full transition engineering, search family redesign, sessionization, query semantics, trend features, or category-aware behavior.

## Complexity Added

- E1 feature count: 36
- V2-2 feature count: 27
- V2-3b feature count: 30
- V2-3c feature count: 28
- New transition features added: 1
- Train rows: 1,703,581
- Validation rows: 2,149,796
- Train positive rate: 0.041464
- Validation positive rate: 0.043546
- Model output: `data/models/purchase_propensity_baseline_v23c`

## Recommendation

Investigate further

## Privacy

Artifacts are aggregate-only. No raw client IDs, raw query text, product names, row-level scores, row-level prediction examples, row-level transitions, or raw model internals are persisted in artifacts.
