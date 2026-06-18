# Baseline V2-3b Evaluation

## New Features

- `search_before_cart_count`
- `search_to_cart_rate`
- `recent_search_then_cart_flag`

## Metric Comparison

| Metric | E1 temporal baseline | V2-1 | V2-2 | V2-3b | V2-3b vs V2-2 |
|---|---:|---:|---:|---:|---:|
| ROC-AUC | 0.835559 | 0.834273 | 0.834208 | 0.834653 | 0.05% |
| PR-AUC | 0.253155 | 0.254871 | 0.255374 | 0.255093 | -0.11% |
| Precision@1% | 0.478742 | 0.483766 | 0.484882 | 0.485347 | 0.10% |
| Precision@5% | 0.294837 | 0.296102 | 0.296158 | 0.296679 | 0.18% |
| Precision@10% | 0.213922 | 0.216174 | 0.216155 | 0.215285 | -0.40% |
| Lift@1% | 10.994062 | 11.109429 | 11.135066 | 11.145748 | 0.10% |
| Lift@5% | 6.770770 | 6.799825 | 6.801107 | 6.813071 | 0.18% |
| Lift@10% | 4.912611 | 4.964312 | 4.963885 | 4.943909 | -0.40% |

## Search Family Analysis

Transition features maintained temporal ranking metrics within a small tolerance relative to V2-2.

### Coefficient Summary

| Feature | Coefficient | Abs coefficient | Abs-coefficient rank among model features |
|---|---:|---:|---:|
| search_before_cart_count | -0.007490 | 0.007490 | 22 |
| search_to_cart_rate | -0.107695 | 0.107695 | 5 |
| recent_search_then_cart_flag | 0.409962 | 0.409962 | 2 |

### Feature Distributions

| Feature | Non-null rate | Distinct | Mean | Stddev | Min | P50 | P90 | P95 | P99 | Max | Zero rate |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| search_before_cart_count | 1.000000 | 309 | 0.941821 | 5.666131 | 0 | 0.0 | 2.0 | 4.0 | 15.0 | 1027 | 0.796523 |
| search_to_cart_rate | 1.000000 | 2217 | 0.327606 | 0.212736 | 0.0 | 0.333333 | 0.666667 | 0.75 | 0.9 | 0.998521 | 0.135597 |
| recent_search_then_cart_flag | 1.000000 | 2 | 0.081686 | 0.273886 | 0 | 0.0 | 0.0 | 1.0 | 1.0 | 1 | 0.918314 |

### Positive-Rate Segmentation

| Feature | Bucket | Row count | Positive count | Positive rate |
|---|---:|---:|---:|---:|
| search_before_cart_count | 0 | 1356941 | 40645 | 0.029953 |
| search_before_cart_count | 1 | 140318 | 6406 | 0.045653 |
| search_before_cart_count | 2-3 | 100948 | 6954 | 0.068887 |
| search_before_cart_count | 4-10 | 75317 | 8936 | 0.118645 |
| search_before_cart_count | >10 | 30057 | 7696 | 0.256047 |
| search_to_cart_rate | (0,0.25] | 357143 | 15485 | 0.043358 |
| search_to_cart_rate | (0.25,0.50] | 858235 | 18859 | 0.021974 |
| search_to_cart_rate | (0.50,0.75] | 174506 | 10943 | 0.062708 |
| search_to_cart_rate | (0.75,1.0] | 82696 | 11152 | 0.134855 |
| search_to_cart_rate | 0 | 231001 | 14198 | 0.061463 |
| recent_search_then_cart_flag | 0 | 1564422 | 49380 | 0.031564 |
| recent_search_then_cart_flag | 1 | 139159 | 21257 | 0.152753 |

## Complexity Added

- E1 feature count: 36
- V2-1 feature count: 31
- V2-2 feature count: 27
- V2-3b feature count: 30
- New transition features added: 3
- Train rows: 1,703,581
- Validation rows: 2,149,796
- Train positive rate: 0.041464
- Validation positive rate: 0.043546
- Model output: `data/models/purchase_propensity_baseline_v23b`

## Recommendation

Investigate further

## Privacy

Artifacts are aggregate-only. No raw client IDs, raw query text, product names, row-level scores, row-level prediction examples, row-level transitions, or raw model internals are persisted in artifacts.
