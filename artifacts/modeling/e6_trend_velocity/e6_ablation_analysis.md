# E6 Ablation Analysis

Each row removes one E6 feature group from the full V2-2 + E6 experiment. This measures whether each velocity group contributes incremental ranking value.

| Removed E6 group | Removed features | ROC-AUC | PR-AUC | Precision@5% | Lift@5% | PR-AUC vs full E6 | Lift@5 vs full E6 |
|---|---|---:|---:|---:|---:|---:|---:|
| none_full_e6 | none | 0.832639 | 0.258004 | 0.298828 | 6.862422 | 0.00% | 0.00% |
| cart_velocity | cart_velocity_30d_vs_90d,cart_delta_30d_90d | 0.832971 | 0.257631 | 0.298242 | 6.848963 | -0.14% | -0.20% |
| buy_velocity | buy_velocity_30d_vs_90d,buy_delta_30d_90d | 0.833517 | 0.254834 | 0.296288 | 6.804098 | -1.23% | -0.85% |
| search_velocity | search_velocity_30d_vs_90d | 0.832811 | 0.257948 | 0.298651 | 6.858363 | -0.02% | -0.06% |
| activity_acceleration | activity_intensity_ratio | 0.832395 | 0.258274 | 0.299246 | 6.872036 | 0.10% | 0.14% |

## Interpretation

If removing a group improves or does not change metrics, that group is not clearly additive. If removing a group hurts PR-AUC or Lift@5%, that group may contain useful velocity signal.

## Privacy

This artifact contains aggregate model metrics only.
