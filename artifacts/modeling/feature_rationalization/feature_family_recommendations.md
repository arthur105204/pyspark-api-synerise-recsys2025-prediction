# Feature Family Recommendations for Future Baseline v2

## Scope

This review extends the variance, correlation, and E4 ablation audits with behavioral interpretation and feature engineering quality. It is a recommendation for a future Baseline v2 feature set only. The original E1 temporal baseline should remain unchanged for reproducibility and comparison.

Evidence used:

- E4 temporal feature ablation results.
- Feature variance audit on the E1 temporal training snapshot.
- Feature redundancy matrix and active-days correlation audit.
- Business meaning of ecommerce funnel behavior.

Important interpretation rule:

Correlation and low single-family ablation impact are not sufficient reasons to remove a feature family. A family may be retained if it has strong behavioral meaning, captures a distinct concept, or is a good candidate for redesign.

## Summary Recommendation

| Feature family | Recommendation | Short reason |
|---|---|---|
| overall_activity | KEEP | Strongest E4 contribution and clear engagement-intensity meaning. |
| recency_features | KEEP | Low redundancy with active days and meaningful freshness signal. |
| product_metadata_features | KEEP | Moderate PR-AUC contribution and interpretable category/price context. |
| add_to_cart_activity | KEEP_WITH_REDUCTION | Core positive-intent signal, but rolling count windows are highly redundant. |
| product_buy_activity | KEEP_WITH_REDUCTION | Repeat-purchase signal is meaningful, but current count windows overlap strongly. |
| search_activity | REDESIGN | Search count has intent meaning, but count-only search is noisy and overlaps with activity. |
| remove_from_cart_activity | REDESIGN | Potential negative/hesitation signal, but current count features are too blunt. |
| ratio_features | REMOVE | Current raw ratios underperform and duplicate count/intensity signals. |
| cohort_indicator | REMOVE | Constant after eligible-cohort filtering. |

## Family-Level Review

### overall_activity

Recommendation: KEEP

Statistical usefulness:

- `active_days_count` is not constant: 82 distinct values, stddev 1.962914.
- Removing it caused the largest E4 PR-AUC drop: 2.86% relative.
- Removing it also caused the largest Lift@5% drop: 1.98% relative.

Behavioral semantics:

- Represents general engagement intensity.
- Mostly neutral activity, but in purchase propensity it is a strong prior: users active on more days tend to have more opportunities to purchase.

Feature engineering quality:

- Simple, dense, and stable.
- It appears to proxy several activity families, especially search and cart behavior.
- Keep as a core baseline feature, but avoid letting it replace funnel-specific features in interpretation.

### recency_features

Recommendation: KEEP

Statistical usefulness:

- E4 removal caused a 0.63% PR-AUC drop and 1.94% Lift@5% drop.
- Average correlation with `active_days_count` is low at 0.112238, suggesting more independent signal than many count families.

Behavioral semantics:

- Strongly aligned with purchase propensity theory: recent cart, buy, remove, or search behavior is more likely to reflect current intent than old behavior.
- Recency can represent positive intent, cooling intent, or renewed interest depending on event type.

Feature engineering quality:

- Current implementation is a useful baseline.
- Future versions could improve by separating event-specific recency interactions, for example cart recency versus buy recency.

### product_metadata_features

Recommendation: KEEP

Statistical usefulness:

- E4 removal caused a 2.02% PR-AUC drop, the second-largest PR-AUC impact after `overall_activity`.
- Lift@5% drop is smaller at 0.60%, so the family helps overall ranking more than top-campaign precision.

Behavioral semantics:

- Product category and price context are meaningful because purchase intent differs by product type and price band.
- These features help move beyond pure activity volume.

Feature engineering quality:

- Current aggregates are coarse but useful.
- Keep for v2, while continuing to monitor SKU/category/price metadata stability.

### add_to_cart_activity

Recommendation: KEEP_WITH_REDUCTION

Statistical usefulness:

- E4 single-family removal had small impact: 0.10% PR-AUC drop and 0.18% Lift@5% drop.
- Average pairwise correlation inside the family is high at 0.836477.
- Average correlation with `active_days_count` is 0.533087.

Behavioral semantics:

- Add-to-cart is a core positive-intent commerce signal.
- It directly represents movement down the funnel and should not be removed only because current LR ablation shows small incremental value.

Feature engineering quality:

- Current implementation likely over-represents the same concept through total count plus 30d/60d/90d counts.
- For Baseline v2, keep this family but reduce redundancy. Prefer a compact set such as total count, one recent-window count, distinct SKU count, and/or a trend-style feature instead of all overlapping windows.

### product_buy_activity

Recommendation: KEEP_WITH_REDUCTION

Statistical usefulness:

- E4 removal slightly improved PR-AUC and Lift@5%, which suggests the current representation does not add clean incremental value to the LR baseline.
- Average pairwise correlation is high at 0.798540.
- Average correlation with `active_days_count` is moderate at 0.405419.

Behavioral semantics:

- Prior purchase behavior is highly meaningful for repeat purchase propensity.
- It represents a distinct concept from carting or searching: realized transaction history.

Feature engineering quality:

- Current count-window features may mix different purchase patterns without distinguishing recency, category continuity, or repeat-buy cadence.
- Keep with reduction for v2. Do not remove the concept; simplify and later redesign into repeat-purchase/category-aware features.

### remove_from_cart_activity

Recommendation: REDESIGN

Statistical usefulness:

- E4 removal slightly improved PR-AUC and Lift@5%, so current simple counts do not provide clear incremental signal.
- Average pairwise correlation is high at 0.852428.
- Average correlation with `active_days_count` is 0.488517.

Behavioral semantics:

- Remove-from-cart is not automatically negative. It may mean:
  - negative intent or abandonment,
  - hesitation or price sensitivity,
  - cart cleanup,
  - product substitution,
  - high engagement before later purchase.
- This makes it theoretically useful but ambiguous.

Feature engineering quality:

- Current implementation is likely too noisy because it counts removals without sequence context.
- Better future features would distinguish remove-after-add delay, remove-to-add ratio over recent windows, whether removal was followed by later add or buy, and category-level replacement behavior.
- Redesign rather than remove permanently.

### search_activity

Recommendation: REDESIGN

Statistical usefulness:

- E4 removal had small impact: 0.18% PR-AUC drop and 0.24% Lift@5% drop.
- Average pairwise correlation is high at 0.743330.
- Average correlation with `active_days_count` is the highest among non-overall families at 0.645473.
- `distinct_search_days` has very high correlation with `active_days_count` at 0.870504.

Behavioral semantics:

- Search can indicate discovery intent, but simple count does not distinguish vague browsing from targeted purchase intent.

Feature engineering quality:

- Current count-only search representation is noisy.
- Do not treat low ablation impact as proof search is useless. Redesign with better aggregates later, such as search recency, repeated search days, category-linked search-to-cart transitions, or cleaned query/category signals if privacy-safe.

### ratio_features

Recommendation: REMOVE

Statistical usefulness:

- E4 removal improved PR-AUC by 0.68% relative and improved Lift@5% by 0.43% relative.
- Ratios have extreme max values, especially `search_to_cart_ratio`, because denominators can be small.
- Average pairwise correlation is low, but low correlation alone does not imply useful signal.

Behavioral semantics:

- Ratios are interpretable in principle, but the current raw ratios are unstable.
- A high ratio may mean strong intent, noisy browsing, sparse denominator artifacts, or missing cart context.

Feature engineering quality:

- Current ratios are too brittle for Baseline v2.
- Remove current raw ratio features from Baseline v2.
- Later redesign only if ratio features are clipped, smoothed, bucketed, or computed over stable recent windows.

### cohort_indicator

Recommendation: REMOVE

Statistical usefulness:

- `is_eligible_purchase_propensity` is constant in the training dataset:
  - distinct count: 1
  - stddev: 0.0
  - min: 1
  - max: 1
- E4 removal changes PR-AUC and Lift@5% by effectively 0%.

Behavioral semantics:

- It only records the cohort rule.
- Since the training dataset is already filtered to eligible users, it no longer carries behavioral information.

Feature engineering quality:

- Remove from model input for Baseline v2.
- It can remain as metadata before label construction, but should not be included as a model feature.

## Rolling-Window Count Families

Recommendation: KEEP_WITH_REDUCTION for cart and buy; REDESIGN for remove/search where needed.

The current rolling-window families use total count plus 30d/60d/90d counts. This creates high redundancy because longer windows include shorter windows.

Observed high within-family correlations:

- add_to_cart_activity: 0.836477 average pairwise correlation.
- product_buy_activity: 0.798540 average pairwise correlation.
- remove_from_cart_activity: 0.852428 average pairwise correlation.
- search_activity: 0.743330 average pairwise correlation.

Future Baseline v2 should reduce overlapping windows rather than deleting the behavior concept. Good options:

- keep one recent window plus total count,
- replace overlapping windows with trend or delta features,
- use recency plus recent count instead of 30d/60d/90d together,
- preserve distinct SKU/category counts where they add meaning.

## Final Recommended Baseline v2 Feature Set

Keep:

- `active_days_count`
- recency features
- product metadata features

Keep with reduction:

- add-to-cart activity features
- product-buy activity features
- rolling-window count features, but simplified

Redesign:

- remove-from-cart activity
- search activity

Remove:

- `is_eligible_purchase_propensity`
- current raw ratio features:
  - `buy_to_cart_ratio`
  - `remove_to_cart_ratio`
  - `cart_minus_remove_count`
  - `search_to_cart_ratio`

## Final Position

For the current E1 baseline, keep everything unchanged for reproducibility.

For a future Baseline v2, remove only features with clear evidence:

- constant cohort indicator,
- unstable raw ratio features.

Do not remove cart, buy, remove, or search behavior solely because single-family ablation impact is small. These are behaviorally important signals, but their current implementation is redundant or noisy and should be reduced or redesigned.

## Privacy

This artifact contains aggregate metrics, feature names, and technical interpretation only. It contains no raw client IDs, raw query text, product names, row-level predictions, row-level scores, or row-level examples.
