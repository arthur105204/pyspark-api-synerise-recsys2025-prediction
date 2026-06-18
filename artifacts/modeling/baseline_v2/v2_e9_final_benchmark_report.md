# E9 Final Benchmark Report

## Scope

E9 compares existing trained V2-2 and V2-4 models on the temporal validation snapshot. No model retraining, feature redesign, preprocessing change, or model architecture change is performed.

## Overall Metrics

| Model | ROC-AUC | PR-AUC | Precision@1% | Precision@5% | Precision@10% | Lift@1% | Lift@5% | Lift@10% |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `baseline_v2_2` | 0.834208 | 0.255386 | 0.484882 | 0.296158 | 0.216155 | 11.135066 | 6.801107 | 4.963885 |
| `baseline_v2_4` | 0.832395 | 0.258281 | 0.494046 | 0.299246 | 0.217202 | 11.345504 | 6.872036 | 4.987920 |

## Overall Delta

- PR-AUC change: 1.13%
- Lift@5 change: 1.04%

## Segment Stability

- Activity segmentation: high_activity if active_days_count > validation median (1.000000); otherwise low_activity
- Lifecycle segmentation: returning_users if pre-cutoff product_buy_count > 0; otherwise new_users
- Time-slice analysis: The temporal validation dataset is a single cutoff snapshot, so row-level prediction time slices are not available without generating additional cutoffs.

| Segment type | Segment | V2-2 PR-AUC | V2-4 PR-AUC | V2-2 Lift@5 | V2-4 Lift@5 | Lift@5 delta |
|---|---|---:|---:|---:|---:|---:|
| `activity_segment` | `high_activity` | 0.3385841211416069 | 0.3433256788854232 | 4.329286 | 4.398272 | 1.59% |
| `activity_segment` | `low_activity` | 0.06068421580750967 | 0.05754392084170889 | 3.850815 | 3.277115 | -14.90% |
| `lifecycle_segment` | `new_users` | 0.04524889411238538 | 0.04565317769868006 | 5.498891 | 5.552826 | 0.98% |
| `lifecycle_segment` | `returning_users` | 0.29178274274666005 | 0.29520361707925113 | 4.044852 | 4.113090 | 1.69% |

## Ranking Stability

| K | Overlap count | Overlap rate | V2-2 only | V2-4 only |
|---|---:|---:|---:|---:|
| 1% | 19617 | 0.912503 | 1881 | 1881 |
| 5% | 99558 | 0.926207 | 7932 | 7932 |
| 10% | 197234 | 0.917453 | 17746 | 17746 |

## Score Distribution

- V2-2 score mean: 0.339876
- V2-4 score mean: 0.336241
- Mean score delta, V2-4 minus V2-2: -0.003636

## Risk Review

The final decision is based only on PR-AUC, Lift@5, and segment stability. ROC-AUC is reported but is secondary for this imbalanced ranking use case.

## Decision

PROMOTE V2-4

V2-4 improves PR-AUC and Lift@5 overall; segment Lift@5 regressions require monitoring: low_activity.

## Privacy

Artifacts contain aggregate metrics only. No raw client IDs, raw query text, product names, row-level examples, or row-level prediction files are written.
