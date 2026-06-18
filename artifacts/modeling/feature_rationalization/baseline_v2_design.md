# Baseline v2 Feature Design Review

## 1. Executive Summary

The feature rationalization work showed that the E1 temporal baseline is a useful and reproducible reference, but its feature set mixes strong behavioral concepts with redundant representations. The strongest single feature family in E4 was `overall_activity`, represented by `active_days_count`. This does not mean funnel features are unimportant. It means one dense engagement feature is currently absorbing signal from several overlapping activity-count families.

The main lesson is to separate feature concepts from feature implementations:

- Cart, buy, remove, and search behaviors are meaningful concepts.
- Current overlapping count windows can be redundant.
- A weak ablation result can mean the representation is noisy or duplicated, not that the behavior is useless.

Baseline v2 should therefore reduce redundant features, remove features with clear defects, and redesign ambiguous behavior families instead of deleting them purely for metric reasons. Behavior semantics matter because purchase propensity is not only a statistical ranking problem; it is a customer-funnel problem. A feature such as add-to-cart has strong business meaning even if `active_days_count` currently captures part of its signal.

Baseline v2 should preserve the original E1 baseline unchanged for reproducibility and compare all future changes against the E1 temporal metrics.

## 2. Baseline v2 Candidate Architecture

| Feature family | Current implementation | Problems identified | Proposed v2 implementation | Expected benefits | Risk assessment |
|---|---|---|---|---|---|
| overall_activity | `active_days_count` | Strong broad proxy for engagement; may absorb signal from activity families. | Keep `active_days_count` as a core dense feature. Use it as a control feature, not as a replacement for funnel features. | Stable engagement-intensity prior; strong baseline signal. | May dominate LR coefficients and hide weaker behavioral signals. |
| recency_features | Days since last add, remove, buy, search. | Useful but isolated from event intensity and sequence. | Keep event-specific recency features. Later test interactions such as recent cart plus cart count. | Captures freshness of intent with relatively low redundancy. | Null handling and median imputation can blur no-event semantics. |
| product_metadata_features | Category counts and price aggregates for cart/buy behavior. | Coarse aggregates; depends on metadata stability. | Keep current stable aggregates. Consider price bands and category concentration later. | Adds product context beyond pure activity volume. | Metadata instability or coarse category IDs can limit interpretability. |
| add_to_cart_activity | Total, distinct SKU, 30d/60d/90d counts. | High within-family redundancy; overlaps with active days. | Keep concept with reduction: total cart count, distinct cart SKU count, one recent cart window, and/or cart trend. | Preserves core positive-intent signal while reducing duplicated count windows. | Removing too many windows may lose time-scale information. |
| product_buy_activity | Total, distinct SKU, 30d/60d/90d counts. | High redundancy; weak incremental E4 result; current counts do not model cadence. | Keep concept with reduction: total prior purchases, purchase recency, one recent purchase window, and repeat/category continuity later. | Better repeat-purchase representation with fewer duplicate counts. | Prior purchases may reflect historical loyalty more than future intent for some users. |
| remove_from_cart_activity | Total, distinct SKU, 30d/60d/90d counts. | Ambiguous semantics; current counts cannot distinguish abandonment from active comparison. | Redesign as sequence-aware remove behavior: remove-after-add delay, remove followed by add, remove followed by purchase, unresolved removals. | Converts ambiguous negative/hesitation behavior into interpretable funnel signals. | Requires careful window joins and may be more expensive. |
| search_activity | Search count, distinct search days, 30d/60d/90d counts. | Count-only search is noisy; high overlap with active days. | Redesign toward intent quality: search recency, repeated search days, search frequency, search trend, search-to-cart transition. | Distinguishes targeted intent from broad browsing. | Query text should remain private; transition features need careful aggregation. |
| ratio_features | Raw buy/cart, remove/cart, cart-minus-remove, search/cart ratios. | Raw ratios are unstable with small denominators and underperformed in E4. | Remove current raw ratios from Baseline v2. Later reintroduce only smoothed/clipped/bucketed ratios if justified. | Reduces brittle numeric artifacts. | Some intensity-normalized behavior may be lost until redesigned. |
| cohort_indicator | `is_eligible_purchase_propensity` | Constant after training cohort filtering. | Remove from model input. Keep only as pre-label metadata if needed. | Eliminates zero-variance feature and correlation failure source. | No modeling risk because it carries no within-training variation. |

## 3. Rolling Window Simplification

Current rolling-window count families include:

- total count
- 30d count
- 60d count
- 90d count

This structure is easy to implement but highly redundant because longer windows include shorter windows. E4 and the redundancy audit showed high within-family correlations:

- add-to-cart activity: 0.836477 average pairwise correlation
- product-buy activity: 0.798540 average pairwise correlation
- remove-from-cart activity: 0.852428 average pairwise correlation
- search activity: 0.743330 average pairwise correlation

Recommendation:

| Feature type | v2 recommendation | Justification |
|---|---|---|
| Total count | Keep for cart and buy; review for remove/search. | Captures long-term engagement/history. |
| 30d count | Keep as the primary recent-window count. | Most aligned with a 30-day target window. |
| 60d count | Remove or replace with trend. | Often duplicates 30d and 90d. |
| 90d count | Keep only if used as a long-window baseline or trend denominator. | Useful for historical comparison, but redundant as a raw standalone count. |
| Recency | Keep. | Recency can replace some need for multiple rolling windows. |
| Trend features | Introduce carefully. | Trends can express acceleration without keeping all overlapping counts. |

Proposed simplification pattern:

- Keep `total_count`.
- Keep `count_30d`.
- Remove raw `count_60d` as a standalone feature.
- Keep `count_90d` only if used to compute a controlled trend feature.
- Add trend only after a separate experiment, for example `count_30d / count_90d` with smoothing or `count_30d - prior_30d`.

This avoids adding correlated copies while preserving short-term and long-term behavior.

## 4. Search Feature Redesign

Search behavior should not be judged only by raw counts. Search can represent purchase intent, but simple counts mix focused shopping with broad browsing.

| Proposed feature | Behavioral meaning | Implementation logic | Expected impact |
|---|---|---|---|
| search_recency | Fresh search activity may reflect current intent. | Days since last pre-cutoff search event. Already partially represented; keep and validate. | Better signal freshness than total search count. |
| repeated_search_days | Repeated search across days may indicate sustained interest. | Count distinct search days in recent window, e.g. 30d. | More robust than raw query count spikes. |
| search_frequency | Search intensity normalized by active days. | `search_query_count_30d / active_days_count_30d` if active-day window is available. Use smoothing. | Separates search-heavy users from generally active users. |
| search_trend | Increasing search activity may indicate rising intent. | Compare recent search count with earlier period, such as last 30d vs previous 30d. | Captures momentum instead of raw volume. |
| search_to_cart transition | Search followed by cart is stronger intent than search alone. | Aggregate clients with search before add-to-cart within a safe pre-cutoff window. Do not persist raw query text. | Converts noisy search into funnel progression signal. |

Implementation guardrails:

- Do not output raw query text.
- Keep all outputs aggregate or user-level features only under ignored processed data.
- Avoid query semantics until privacy-safe query/category mapping is available.

## 5. Remove-From-Cart Redesign

Remove-from-cart behavior is behaviorally rich but ambiguous. It may represent negative purchase intent, but it can also represent comparison shopping, cart cleanup, substitution, or high engagement.

| Proposed feature | Behavioral meaning | Implementation logic | Expected impact |
|---|---|---|---|
| add_remove_delay | Short delay may indicate reconsideration; long delay may indicate cleanup. | For same client and SKU, compute time between latest add and subsequent remove before cutoff; aggregate median/min delay. | Distinguishes immediate rejection from delayed cleanup. |
| unresolved_removed_items | Removed items not followed by another add or purchase may indicate abandonment. | Count remove events without later same-SKU add/buy before cutoff. | More directly captures negative intent. |
| remove_followed_by_add | Removal followed by add may indicate substitution or active shopping. | Count client/SKU or client/category sequences where remove is followed by add. | Separates negative removal from continued engagement. |
| remove_followed_by_purchase | Removal followed by purchase means removal was not necessarily negative. | Count remove events followed by later product_buy before cutoff. | Prevents treating all removals as bad intent. |
| cart_abandonment_indicator | Cart activity without purchase after remove may signal drop-off. | Aggregate add/remove/buy sequence state before cutoff. | More interpretable funnel status feature. |

Expected benefit:

These sequence-aware features should give remove behavior a clearer sign. Current count features are too blunt, which explains why E4 removal did not hurt performance.

Risk:

Sequence joins can be more expensive and must stay leakage-safe by using only pre-cutoff events.

## 6. Purchase Feature Redesign

Prior purchase behavior is meaningful, but raw rolling counts do not fully describe repeat-purchase dynamics.

| Proposed feature | Behavioral meaning | Implementation logic | Expected impact |
|---|---|---|---|
| purchase_cadence | Regular buyers may be more likely to buy again. | Compute average days between prior purchases for users with multiple purchases. | Better repeat-purchase signal than total count. |
| purchase_recency | Recent purchase can indicate loyalty or recent need fulfillment. | Keep days since last purchase. Consider event-specific handling for no prior purchase. | Strong interpretable repeat behavior signal. |
| repeat_category_purchases | Repeated category purchase suggests durable preference. | Count distinct categories bought and dominant/repeated category frequency. | Adds category-level repeat intent. |
| purchase_acceleration | Recent purchases increasing relative to history. | Compare 30d purchase count with previous period or longer baseline. | Captures rising purchase activity. |
| buyer_type flags | Distinguish never-bought, one-time buyer, repeat buyer. | Use prior `product_buy_count` buckets. | Improves interpretability for mentor/business review. |

Expected benefit:

Purchase features should shift from raw volume to repeat behavior, cadence, and category continuity.

Risk:

Some purchase history may indicate recent fulfillment rather than immediate future need, so validation must decide the direction empirically.

## 7. Prioritized Implementation Plan

### Phase 1: Quick Wins

Goal: reduce obvious defects and redundancy with minimal code change.

| Item | Change | Effort | Complexity | Expected benefit |
|---|---|---|---|---|
| Remove cohort indicator from model input | Exclude `is_eligible_purchase_propensity` from Baseline v2 features. | Low | Low | Removes constant feature; no expected performance loss. |
| Remove raw ratio features | Drop current raw ratio family from Baseline v2 candidate. | Low | Low | Removes unstable denominator artifacts. |
| Simplify rolling windows | Keep total and 30d; remove standalone 60d; review 90d. | Medium | Low-Medium | Reduces multicollinearity and improves interpretability. |
| Keep E1 baseline unchanged | Do not overwrite existing model/artifacts. | Low | Low | Preserves comparison baseline. |

### Phase 2: Medium Complexity

Goal: improve representation without changing model class.

| Item | Change | Effort | Complexity | Expected benefit |
|---|---|---|---|---|
| Search redesign v1 | Add search recency/frequency/trend features. | Medium | Medium | Turns noisy search counts into intent-quality signals. |
| Purchase redesign v1 | Add purchase cadence, buyer type, purchase acceleration. | Medium | Medium | Better repeat-purchase representation. |
| Metadata refinement | Add price bands or category concentration if metadata remains stable. | Medium | Medium | More interpretable product-context signal. |

### Phase 3: Advanced Behavioral Redesign

Goal: capture sequence and funnel transitions.

| Item | Change | Effort | Complexity | Expected benefit |
|---|---|---|---|---|
| Remove-from-cart sequence features | Add add→remove, remove→add, remove→buy, unresolved removal features. | High | High | Converts ambiguous remove counts into interpretable intent/abandonment signals. |
| Search-to-cart transitions | Build privacy-safe transition counts from search to cart. | High | High | Stronger funnel progression signal. |
| Category-aware funnel features | Track cart/buy/remove behavior by category aggregates. | High | High | Better captures product preference and intent continuity. |

## 8. Recommended Experiments

### Experiment V2-1: Remove Constant and Raw Ratio Features

Hypothesis:

Removing `is_eligible_purchase_propensity` and raw ratio features will maintain or improve temporal PR-AUC and TopK lift while simplifying the feature set.

Feature changes:

- Remove cohort indicator.
- Remove current raw ratio features.
- Keep all other features unchanged.

Metrics:

- ROC-AUC
- PR-AUC
- Precision@1%, @5%, @10%
- Lift@1%, @5%, @10%

Success criteria:

- PR-AUC drop less than 1% relative.
- Lift@5% drop less than 1% relative.
- Feature set is simpler and more defensible.

### Experiment V2-2: Rolling Window Reduction

Hypothesis:

Reducing overlapping 30d/60d/90d count windows will preserve performance while improving interpretability.

Feature changes:

- Keep total count and 30d count.
- Remove 60d count.
- Keep 90d only if needed for a trend experiment.

Metrics:

- Same temporal validation metrics as E1/E4.
- Compare coefficient stability if inspecting LR coefficients later.

Success criteria:

- PR-AUC and Lift@5% drop less than 1% relative.
- Reduced feature count and clearer interpretation.

### Experiment V2-3: Search Redesign

Hypothesis:

Search intent features based on recency, frequency, trend, or transition to cart will outperform raw search counts.

Feature changes:

- Replace or supplement raw search counts with redesigned search features.
- Keep privacy-safe aggregate-only feature generation.

Metrics:

- PR-AUC
- Top 5% precision/lift
- Search-family ablation after redesign

Success criteria:

- Search ablation impact increases compared with current E4.
- TopK metrics improve or remain stable with better interpretability.

### Experiment V2-4: Remove-From-Cart Sequence Redesign

Hypothesis:

Sequence-aware remove-from-cart features will capture abandonment or hesitation better than raw remove counts.

Feature changes:

- Add remove sequence features using only pre-cutoff events.

Metrics:

- PR-AUC
- TopK lift
- Remove-family ablation after redesign

Success criteria:

- Remove family becomes neutral-to-positive in ablation.
- Review confirms no target leakage.

### Experiment V2-5: Purchase Cadence Redesign

Hypothesis:

Purchase cadence and repeat-category signals will represent repeat purchase intent better than raw buy counts.

Feature changes:

- Add purchase cadence, buyer type, repeat-category, and purchase acceleration features.

Metrics:

- PR-AUC
- TopK lift
- Segment-level behavior review

Success criteria:

- Improves PR-AUC or TopK lift without reducing interpretability.

## 9. Final Recommendation

Final proposed Baseline v2 feature set:

Keep unchanged:

- `active_days_count`
- event-specific recency features
- product metadata aggregates

Keep with reduction:

- add-to-cart activity: total, distinct SKU, one recent window, optional trend
- product-buy activity: total, distinct SKU, one recent window, recency/cadence

Redesign:

- search activity into intent-quality and transition features
- remove-from-cart activity into sequence-aware funnel features

Remove:

- `is_eligible_purchase_propensity`
- current raw ratio features:
  - `buy_to_cart_ratio`
  - `remove_to_cart_ratio`
  - `cart_minus_remove_count`
  - `search_to_cart_ratio`

Do not remove the cart, buy, remove, or search concepts. Those are central to the business meaning of purchase propensity. The recommended action is to reduce duplicate windows and redesign noisy representations, while keeping the E1 temporal baseline as the comparison anchor.

## Privacy

This design document uses only aggregate metrics, feature names, and technical interpretation. It contains no raw client IDs, raw search query text, product names, row-level predictions, row-level scores, or row-level examples.
