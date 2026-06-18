# Baseline V2-2 Evaluation

## Features Removed

Baseline V2-2 starts from V2-1 and removes only standalone 60-day rolling count features.

All removed features:
- `is_eligible_purchase_propensity`
- `buy_to_cart_ratio`
- `remove_to_cart_ratio`
- `cart_minus_remove_count`
- `search_to_cart_ratio`
- `add_to_cart_count_60d`
- `product_buy_count_60d`
- `remove_from_cart_count_60d`
- `search_query_count_60d`

Rolling-window features removed in V2-2:
- `add_to_cart_count_60d`
- `product_buy_count_60d`
- `remove_from_cart_count_60d`
- `search_query_count_60d`

## Metric Comparison

| Metric | E1 temporal baseline | V2-1 | V2-2 | V2-2 vs V2-1 |
|---|---:|---:|---:|---:|
| ROC-AUC | 0.835559 | 0.834273 | 0.834208 | -0.01% |
| PR-AUC | 0.253155 | 0.254871 | 0.255374 | 0.20% |
| Precision@1% | 0.478742 | 0.483766 | 0.484882 | 0.23% |
| Precision@5% | 0.294837 | 0.296102 | 0.296158 | 0.02% |
| Precision@10% | 0.213922 | 0.216174 | 0.216155 | -0.01% |
| Lift@1% | 10.994062 | 11.109429 | 11.135066 | 0.23% |
| Lift@5% | 6.770770 | 6.799825 | 6.801107 | 0.02% |
| Lift@10% | 4.912611 | 4.964312 | 4.963885 | -0.01% |

## Interpretation

Rolling-window reduction improved the primary temporal ranking metrics relative to V2-1.

V2-2 evaluates representation simplification only. It does not test search redesign, purchase cadence, remove-from-cart sequences, transition features, trend features, ratios, calibration, or a new model class.

## Complexity Reduction

- E1 feature count: 36
- V2-1 feature count: 31
- V2-2 feature count: 27
- Total features removed vs E1: 9
- Rolling-window features removed vs V2-1: 4
- Train rows: 1,703,581
- Validation rows: 2,149,796
- Train positive rate: 0.041464
- Validation positive rate: 0.043546
- Model output: `data/models/purchase_propensity_baseline_v22`

## Recommendation

Adopt V2-2 as new experimental baseline

## Privacy

Artifacts are aggregate-only. No raw client IDs, raw query text, product names, row-level scores, row-level prediction examples, or raw model internals are persisted in artifacts.
