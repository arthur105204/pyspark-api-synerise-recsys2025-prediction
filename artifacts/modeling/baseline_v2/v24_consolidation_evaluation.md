# V2-4 Consolidation Evaluation

## Goal

Compare Baseline V2-2, V2-2 plus full E6, and V2-2 plus the pruned E6.1 feature set.

## Constraints

- V2-2 features are unchanged.
- V2-4 adds only the selected E6.1 velocity features.
- No sequence, graph, transition, threshold tuning, calibration, or model architecture changes are introduced.
- Artifacts are aggregate-only.

## Metric Comparison

| Model | Total features | E6 features | ROC-AUC | PR-AUC | Precision@1% | Precision@5% | Precision@10% | Lift@1% | Lift@5% | Lift@10% | PR-AUC vs V2-2 | Lift@5 vs V2-2 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `baseline_v2_2` | 27 | 0 | 0.834208 | 0.255374 | 0.484882 | 0.296158 | 0.216155 | 11.135066 | 6.801107 | 4.963885 | 0.00% | 0.00% |
| `v2_2_plus_full_e6` | 33 | 6 | 0.832639 | 0.258004 | 0.493860 | 0.298828 | 0.217220 | 11.341231 | 6.862422 | 4.988347 | 1.03% | 0.90% |
| `baseline_v2_4_e6_1` | 32 | 5 | 0.832396 | 0.258279 | 0.494046 | 0.299246 | 0.217202 | 11.345504 | 6.872036 | 4.987920 | 1.14% | 1.04% |

## Decision

CANDIDATE_FOR_PRODUCTION_MERGE

Decision rule: if E6.1 improves PR-AUC or Lift@5 versus V2-2, mark as candidate for production merge. Prefer the smallest feature set with stable TopK improvement.

## V2-4 Feature Change

Added E6.1 features:

- `cart_velocity_30d_vs_90d`
- `cart_delta_30d_90d`
- `buy_velocity_30d_vs_90d`
- `buy_delta_30d_90d`
- `search_velocity_30d_vs_90d`

Excluded noisy E6 feature:

- `activity_intensity_ratio`

## Experiment Context

- Train rows: 1,703,581
- Validation rows: 2,149,796
- Train positive rate: 0.041464
- Validation positive rate: 0.043546
- V2-4 model output: `data/models/purchase_propensity_baseline_v24`

## Privacy

No row-level predictions, raw client IDs, raw query text, product names, or row-level examples are written to artifacts.
