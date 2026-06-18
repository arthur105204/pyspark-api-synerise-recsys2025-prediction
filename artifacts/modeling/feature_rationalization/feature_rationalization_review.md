# Feature Rationalization Audit

## Scope
This audit combines E4 ablation evidence with feature variance, constant-feature detection, correlation structure, and business meaning. It keeps the original baseline unchanged for reproducibility and does not train a new model.

## Decision Matrix
| Family | Category | PR-AUC drop | Lift@5% drop | Avg corr with active_days_count | Constant features | Rationale |
|---|---|---:|---:|---:|---|---|
| add_to_cart_activity | REVIEW_REDUNDANCY | 0.10% | 0.18% | 0.533087 | - | Core commerce behavior has weak single-family ablation impact, likely because activity intensity overlaps with active_days_count and windowed counts. |
| product_buy_activity | REVIEW_REDUNDANCY | -0.60% | -0.40% | 0.405419 | - | Core commerce behavior has weak single-family ablation impact, likely because activity intensity overlaps with active_days_count and windowed counts. |
| remove_from_cart_activity | REVIEW_REDUNDANCY | -0.24% | -0.15% | 0.488517 | - | Business meaning is plausible as hesitation or negative intent, but current count-window representation may not capture directionality well. |
| search_activity | REVIEW_REDUNDANCY | 0.18% | 0.24% | 0.645473 | - | Search has plausible intent signal but count-only representation is noisy and overlaps with general activity. |
| recency_features | KEEP_SUPPORTING | 0.63% | 1.94% | 0.112238 | - | Moderate contribution and interpretable supporting signal. |
| ratio_features | REMOVE_CANDIDATE | -0.68% | -0.43% | 0.255892 | - | Derived ratios underperform in E4 and likely duplicate underlying counts. |
| product_metadata_features | KEEP_SUPPORTING | 2.02% | 0.60% | 0.284442 | - | Moderate contribution and interpretable supporting signal. |
| cohort_indicator | REMOVE_CONSTANT | 0.00% | 0.00% | - | is_eligible_purchase_propensity | Feature family is constant inside the filtered training dataset. |
| overall_activity | KEEP_CORE | 2.86% | 1.98% | - | - | Largest E4 drop and broad business meaning as engagement intensity. |

## Specific Findings
- active_days_count dominates E4 because it compresses general engagement intensity across event types into one dense, non-null feature.
- add_to_cart_activity and product_buy_activity should not be removed solely from single-family ablation; they are core commerce funnel signals and may be partially absorbed by active_days_count and overlapping window counts.
- remove_from_cart_activity can represent hesitation, friction, cart cleanup, or negative intent. Current count features may be insufficient because they do not distinguish sequence, timing relative to cart/add, or whether removal was followed by later purchase.
- ratio_features are derived from other activity counts and showed no unique gain in E4, so they are the clearest non-constant remove candidate.

## Required Answers
- Permanently remove: is_eligible_purchase_propensity
- Remain despite low ablation impact: add_to_cart_activity, product_buy_activity, remove_from_cart_activity, search_activity
- Require redesign rather than removal: remove_from_cart_activity, ratio_features, search_activity
- Recommended Baseline v2 family set: add_to_cart_activity, product_buy_activity, remove_from_cart_activity, search_activity, recency_features, product_metadata_features, overall_activity

## Privacy
Artifacts are aggregate-only. No raw client IDs, raw query text, product names, row-level predictions, row-level scores, or model binaries are persisted.
