# E2 Threshold Search & Decision Policy Review

## Scope
This review uses the existing temporal Logistic Regression model and the temporal validation snapshot. It does not retrain the model, modify features, tune hyperparameters, or persist row-level predictions.

## Validation Population
- Validation rows: 2,149,796
- Positives: 93,614
- Negatives: 2,056,182
- Positive rate: 0.043546

## Key Operating Points
| Operating point | Threshold | Precision | Recall | F1 | Population targeted | Lift |
|---|---:|---:|---:|---:|---:|---:|
| Maximum F1 | 0.79 | 0.277695 | 0.370404 | 0.317419 | 0.058083 | 6.377130 |
| Lowest threshold with precision >= 20% | 0.65 | 0.203696 | 0.514784 | 0.291892 | 0.110049 | 4.677770 |
| Lowest threshold with precision >= 30% | 0.83 | 0.301195 | 0.327301 | 0.313706 | 0.047320 | 6.916792 |
| Lowest threshold with precision >= 40% | 0.95 | 0.406494 | 0.179994 | 0.249508 | 0.019282 | 9.334926 |
| Highest threshold with recall >= 50% | 0.66 | 0.208068 | 0.504209 | 0.294575 | 0.105524 | 4.778166 |
| Highest threshold with recall >= 70% | 0.50 | 0.133999 | 0.709766 | 0.225436 | 0.230653 | 3.077207 |

## Population-Target Closest Thresholds
| Target population | Threshold | Actual population | Precision | Recall | Lift |
|---|---:|---:|---:|---:|---:|
| 1% | 0.99 | 0.008719 | 0.494692 | 0.099056 | 11.360338 |
| 5% | 0.82 | 0.049888 | 0.295092 | 0.338069 | 6.776626 |
| 10% | 0.67 | 0.101304 | 0.212436 | 0.494210 | 4.878486 |
| 20% | 0.51 | 0.208144 | 0.142593 | 0.681586 | 3.274583 |

## Policy Comparison
| Policy | Threshold | Population targeted | Precision | Recall | Lift | Positives captured |
|---|---:|---:|---:|---:|---:|---:|
| Best-F1 threshold | 0.79 | 0.058083 | 0.277695 | 0.370404 | 6.377130 | 34,675 |
| Precision >= 20% threshold | 0.65 | 0.110049 | 0.203696 | 0.514784 | 4.677770 | 48,191 |
| Precision >= 30% threshold | 0.83 | 0.047320 | 0.301195 | 0.327301 | 6.916792 | 30,640 |
| Top 1% | - | 0.010000 | 0.478742 | 0.109941 | 10.994062 | 10,292 |
| Top 5% | - | 0.050000 | 0.294837 | 0.338539 | 6.770770 | 31,692 |
| Top 10% | - | 0.100000 | 0.213922 | 0.491262 | 4.912611 | 45,989 |

## Decision Review
Threshold 0.5 is recall-heavy rather than precision-oriented. It captures many buyers, but it also targets a large share of the population, so it is not naturally aligned with capacity-limited marketing campaigns.

The maximum-F1 threshold is useful as a diagnostic, but it should not automatically become the business policy because F1 assumes precision and recall have equal value. Campaign cost, channel capacity, and user fatigue usually make population size and precision more important.

TopK policies are easier to operate because they let the business decide a fixed campaign size. For the current baseline, TopK ranking is preferable to a fixed probability threshold. Top 5% is the recommended default campaign segment; Top 1% is better for expensive or conservative outreach; Top 10% is suitable for broader campaigns.

## Final Conclusion
A fixed threshold is not preferable for the current baseline decision policy. TopK is preferable because the model's strongest validated behavior is ranking quality, and TopK maps directly to marketing capacity.

## Privacy
Artifacts are aggregate-only. No raw client IDs, raw query text, product names, row-level scores, or row-level prediction examples are persisted.
