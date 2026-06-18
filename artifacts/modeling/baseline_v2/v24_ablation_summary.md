# V2-4 Ablation Summary

## Summary

V2-4 consolidates E6.1 by retaining the velocity features that preserved ranking gains while removing the noisy `activity_intensity_ratio` feature.

## Comparison

| Comparison | PR-AUC change | Lift@5 change | Interpretation |
|---|---:|---:|---|
| Full E6 vs V2-2 | 1.03% | 0.90% | Full velocity signal improves ranking over V2-2. |
| V2-4 E6.1 vs V2-2 | 1.14% | 1.04% | Pruned velocity signal remains additive over V2-2. |
| V2-4 E6.1 vs full E6 | 0.11% | 0.14% | Removing `activity_intensity_ratio` checks whether the simpler subset preserves or improves TopK ranking. |

## Feature Decision

- Keep cart velocity features as minor positive contributors.
- Keep buy velocity features as the dominant E6 signal family.
- Keep search velocity in V2-4 because E6.1 selection retained the best TopK candidate.
- Exclude `activity_intensity_ratio` because E6.1 confirmed it as noisy.

## Production Merge Readiness

V2-4 is a candidate for production merge only if its PR-AUC or Lift@5 remains above V2-2 in the consolidated run.
