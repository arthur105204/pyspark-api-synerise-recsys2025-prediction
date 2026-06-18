# E6 Model Evaluation

## Comparison: V2-2 Baseline vs V2-2 + E6 Features

| Metric | V2-2 baseline | V2-2 + E6 | Relative change |
|---|---:|---:|---:|
| ROC-AUC | 0.834208 | 0.832639 | -0.19% |
| PR-AUC | 0.255374 | 0.258004 | 1.03% |
| Precision@1% | 0.484882 | 0.493860 | 1.85% |
| Precision@5% | 0.296158 | 0.298828 | 0.90% |
| Precision@10% | 0.216155 | 0.217220 | 0.49% |
| Lift@1% | 11.135066 | 11.341231 | 1.85% |
| Lift@5% | 6.801107 | 6.862422 | 0.90% |
| Lift@10% | 4.963885 | 4.988347 | 0.49% |

## Adoption Decision

ADOPT_FOR_REVIEW

Adoption requires PR-AUC improvement >= 0.5% or Lift@5% improvement >= 0.5%. Otherwise, E6 remains investigational and is not adopted into production.

## Experiment Context

- Base production candidate: Baseline V2-2.
- E6 model feature count: 33
- Train rows: 1,703,581
- Validation rows: 2,149,796
- Train positive rate: 0.041464
- Validation positive rate: 0.043546
- Experimental model output: `data/models/purchase_propensity_e6_velocity`

## Privacy

Only aggregate metrics are written. No row-level scores, raw client IDs, raw query text, or product names are persisted in artifacts.
