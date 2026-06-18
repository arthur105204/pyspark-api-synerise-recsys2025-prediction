# Baseline V2-1 Evaluation

## Changes Made

Baseline V2-1 removes only high-confidence defective features and keeps the E1 temporal split, preprocessing output, model class, class weighting, median imputation, and TopK evaluation pattern unchanged.

Removed features:
- `is_eligible_purchase_propensity`
- `buy_to_cart_ratio`
- `remove_to_cart_ratio`
- `cart_minus_remove_count`
- `search_to_cart_ratio`

No rolling-window features were removed in V2-1. Rolling windows are reviewed separately in `v21_window_review.csv`.

## Metric Comparison

| Metric | E1 temporal baseline | V2-1 | Relative change |
|---|---:|---:|---:|
| ROC-AUC | 0.835559 | 0.834273 | -0.15% |
| PR-AUC | 0.253155 | 0.254871 | 0.68% |
| Precision@1% | 0.478742 | 0.483766 | 1.05% |
| Precision@5% | 0.294837 | 0.296102 | 0.43% |
| Precision@10% | 0.213922 | 0.216174 | 1.05% |
| Lift@1% | 10.994062 | 11.109429 | 1.05% |
| Lift@5% | 6.770770 | 6.799825 | 0.43% |
| Lift@10% | 4.912611 | 4.964312 | 1.05% |

## Feature Count Comparison

- E1 candidate feature count before V2-1 removals: 36
- V2-1 feature count after removals: 31
- Features removed: 5

## Temporal Setup

- Train rows: 1,703,581
- Validation rows: 2,149,796
- Train positive rate: 0.041464
- Validation positive rate: 0.043546
- Model output: `data/models/purchase_propensity_baseline_v21`

## Interpretation

Removing the constant feature and raw ratio features improved performance.

This experiment does not prove that the removed feature concepts are useless in all forms. It only validates whether the current constant indicator and raw ratio implementations should remain in the baseline feature set.

## Recommendation

Adopt V2-1 as new baseline

## Privacy

Artifacts are aggregate-only. No raw client IDs, raw query text, product names, row-level scores, row-level prediction examples, or raw model internals are persisted in artifacts.
