# E6.1 Pruning Evaluation

## Goal

Identify the smallest E6 velocity feature subset that preserves the ranking gains observed in the full E6 experiment.

## Constraints

- Baseline V2-2 features are unchanged.
- Only previously-defined E6 features are compared.
- No new features, model classes, labels, temporal splits, threshold tuning, or calibration are introduced.
- Artifacts are aggregate-only and contain no row-level predictions.

## Metric Comparison

| Experiment | E6 features | ROC-AUC | PR-AUC | Precision@1% | Precision@5% | Precision@10% | Lift@5% | PR-AUC vs full E6 | Lift@5 vs full E6 | Decision |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| `full_e6` | 6 | 0.832638 | 0.257996 | 0.493860 | 0.298828 | 0.217220 | 6.862422 | 0.00% | 0.00% | REFERENCE |
| `without_activity_intensity_ratio` | 5 | 0.832395 | 0.258275 | 0.494046 | 0.299246 | 0.217202 | 6.872036 | 0.11% | 0.14% | KEEP_AS_CANDIDATE |
| `without_search_velocity` | 5 | 0.832813 | 0.257940 | 0.493209 | 0.298651 | 0.217132 | 6.858363 | -0.02% | -0.06% | KEEP_AS_CANDIDATE |
| `buy_plus_cart_velocity_only` | 4 | 0.832528 | 0.258317 | 0.494418 | 0.299126 | 0.217281 | 6.869259 | 0.12% | 0.10% | KEEP_AS_CANDIDATE |
| `buy_velocity_only` | 2 | 0.832198 | 0.257920 | 0.492464 | 0.298642 | 0.217248 | 6.858149 | -0.03% | -0.06% | DROP_FROM_MINIMAL_SET |

## Decision Rule

Keep only E6 features that contribute at least 0.2% PR-AUC or improve Lift@5. Prefer the simplest model with maximal TopK gain.

## Recommendation

Recommended E6.1 candidate: `without_activity_intensity_ratio`.

Included E6 features: `cart_velocity_30d_vs_90d,cart_delta_30d_90d,buy_velocity_30d_vs_90d,buy_delta_30d_90d,search_velocity_30d_vs_90d`.

This recommendation should be reviewed against the full E6 reference before freezing a production feature set.

## Privacy

This report contains only aggregate metrics and feature names. It does not include raw client IDs, raw query text, product names, row-level examples, or row-level predictions.
