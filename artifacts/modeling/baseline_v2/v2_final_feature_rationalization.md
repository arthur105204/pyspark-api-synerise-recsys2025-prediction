# Final Feature Rationalization for Production Baseline V2

## Scope

This artifact converts the completed E4/E5 audits and V2 experiments into a final production feature selection decision.

Production candidate baseline: **Baseline V2-2**.

Evidence used:

- E4 feature-family ablation results.
- Feature variance and missingness profiling.
- Feature redundancy and active-days correlation audits.
- V2-1 defect removal experiment.
- V2-2 rolling-window reduction experiment.
- V2-3a search bucket experiment.
- V2-3b/V2-3c transition experiments.

No code was changed, no models were retrained, and no new features were created for this final rationalization.

## Per-Feature Decisions

### add_to_cart_count:

* business_purpose: Total historical add-to-cart volume; positive commerce-funnel intent.
* predictive_evidence: Add-to-cart family removal caused a small PR-AUC drop and Lift@5 drop; behaviorally core despite overlap with activity.
* redundancy_evidence: Family has high internal correlation, but total count captures long-run funnel intensity not fully replaced by 30d count.
* risk_analysis: Pre-cutoff only, available at inference, no cohort leakage; low operational cost.
* recommendation: KEEP
* justification: Retain as a core, interpretable commerce-intent volume signal.

### distinct_add_to_cart_sku_count:

* business_purpose: Breadth of cart interest across products; positive purchase-intent diversity.
* predictive_evidence: Add-to-cart family has weak but nonzero ablation contribution; SKU breadth is more behaviorally distinct than raw count.
* redundancy_evidence: Correlates with add-to-cart volume and active days, but encodes product breadth rather than only frequency.
* risk_analysis: Pre-cutoff aggregate only; no raw product names; SKU count is available before scoring.
* recommendation: KEEP
* justification: Keeps a distinct behavioral concept: breadth of cart consideration.

### days_since_last_add_to_cart:

* business_purpose: Freshness of cart intent.
* predictive_evidence: Recency family removal caused meaningful Lift@5 degradation; freshness is important for targeting.
* redundancy_evidence: Recency family has low average correlation with active_days_count, so it adds independent timing signal.
* risk_analysis: Null for users without add-to-cart history; handled by imputation; computed before cutoff.
* recommendation: KEEP
* justification: Strong business meaning and relatively independent statistical signal.

### add_to_cart_count_30d:

* business_purpose: Recent add-to-cart intensity.
* predictive_evidence: Add-to-cart family adds modest signal; recent windows align with 30-day target behavior.
* redundancy_evidence: Correlated with 90d and total count, but 30d captures short-term intent.
* risk_analysis: Pre-cutoff only; stable count feature; low maintainability risk.
* recommendation: KEEP
* justification: Keep as the primary recent add-to-cart window after removing the redundant 60d window.

### add_to_cart_count_90d:

* business_purpose: Medium-term add-to-cart intensity.
* predictive_evidence: Add-to-cart family contribution is modest but behaviorally important.
* redundancy_evidence: Highly correlated with total add-to-cart count; V2-2 kept 90d for stability after removing 60d.
* risk_analysis: Pre-cutoff only; low leakage risk; moderate redundancy risk.
* recommendation: KEEP_BUT_MONITOR
* justification: Retain for production V2 because V2-2 improved after only removing 60d; monitor for V3 simplification.

### remove_from_cart_count:

* business_purpose: Total remove-from-cart volume; hesitation, cleanup, substitution, or negative intent.
* predictive_evidence: Remove-family ablation did not show strong positive contribution, but signal may be poorly represented by simple counts.
* redundancy_evidence: High within-family redundancy and overlap with active_days_count.
* risk_analysis: Pre-cutoff and available at inference; semantics are ambiguous but not leakage-prone.
* recommendation: KEEP_BUT_MONITOR
* justification: Keep because it represents a distinct funnel behavior; monitor because current representation may be noisy.

### distinct_remove_from_cart_sku_count:

* business_purpose: Breadth of removed products; possible consideration churn or product substitution.
* predictive_evidence: Remove-family ablation is weak, but SKU breadth may capture different behavior from raw remove count.
* redundancy_evidence: Correlates with remove volume and general activity; still carries breadth semantics.
* risk_analysis: Pre-cutoff aggregate only; no product names; low operational risk.
* recommendation: KEEP_BUT_MONITOR
* justification: Retain as a distinct behavior concept; redesign may be better than removal in V3.

### days_since_last_remove_from_cart:

* business_purpose: Freshness of cart removal or hesitation.
* predictive_evidence: Recency family is useful; event-specific recency may capture recent friction.
* redundancy_evidence: Recency features have low overall redundancy with active_days_count.
* risk_analysis: Sparse for users without remove events; imputation required; no leakage if computed before cutoff.
* recommendation: KEEP
* justification: Retain as a timing signal for recent hesitation or cart cleanup.

### remove_from_cart_count_30d:

* business_purpose: Recent remove-from-cart activity.
* predictive_evidence: Remove-family signal is weak globally, but recent remove behavior may matter for top targeting.
* redundancy_evidence: Correlated with 90d and total remove count; 30d is the most interpretable recent window.
* risk_analysis: Pre-cutoff only; stable count feature.
* recommendation: KEEP_BUT_MONITOR
* justification: Keep as the recent remove signal; monitor because simple remove counts are semantically ambiguous.

### remove_from_cart_count_90d:

* business_purpose: Medium-term remove-from-cart intensity.
* predictive_evidence: Weak direct family ablation evidence.
* redundancy_evidence: Highly correlated with total remove count; kept after V2-2 only because 90d was not isolated for removal.
* risk_analysis: Pre-cutoff only; no leakage; redundancy risk is the main concern.
* recommendation: KEEP_BUT_MONITOR
* justification: Keep for production V2 stability, but mark as a V3 simplification candidate.

### product_buy_count:

* business_purpose: Historical purchase volume; repeat-purchase behavior.
* predictive_evidence: Product-buy family ablation did not improve incremental metrics, likely because signal overlaps with activity and recency.
* redundancy_evidence: Correlated with product-buy windows and active_days_count, but realized purchase history is uniquely meaningful.
* risk_analysis: Uses pre-cutoff purchases only; safe if cutoff boundary is preserved; available at inference.
* recommendation: KEEP
* justification: Prior purchases are core propensity context and should not be removed solely due to redundant baseline representation.

### distinct_product_buy_sku_count:

* business_purpose: Breadth of prior purchased products.
* predictive_evidence: Product-buy family has weak incremental ablation impact, but purchase breadth is behaviorally meaningful.
* redundancy_evidence: Related to purchase count but represents variety, not just frequency.
* risk_analysis: Pre-cutoff aggregate; no product names; low operational risk.
* recommendation: KEEP
* justification: Retain as a compact repeat-purchase breadth signal.

### days_since_last_product_buy:

* business_purpose: Freshness of prior purchase behavior.
* predictive_evidence: Recency family is one of the clearer supporting families, especially for Lift@5.
* redundancy_evidence: Low average correlation with activity features compared with count windows.
* risk_analysis: Sparse for users without prior purchase; imputation handles nulls; no leakage if pre-cutoff.
* recommendation: KEEP
* justification: Retain as a stable and interpretable repeat-purchase timing signal.

### product_buy_count_30d:

* business_purpose: Recent purchase activity.
* predictive_evidence: Recent purchase behavior may inform repeat purchase or active buyer intent.
* redundancy_evidence: Correlated with total/90d buy counts, but 30d captures recent transaction activity.
* risk_analysis: Pre-cutoff only; low leakage risk if target-window purchases remain excluded.
* recommendation: KEEP
* justification: Keep as recent purchase-intensity signal after removing the redundant 60d window.

### product_buy_count_90d:

* business_purpose: Medium-term purchase activity.
* predictive_evidence: Product-buy family contribution is weak, but prior purchases remain behaviorally central.
* redundancy_evidence: Highly correlated with total product_buy_count.
* risk_analysis: Pre-cutoff only; low operational complexity; moderate redundancy risk.
* recommendation: KEEP_BUT_MONITOR
* justification: Retain for V2 stability; monitor for V3 window simplification.

### distinct_cart_category_count:

* business_purpose: Breadth of cart interest across product categories.
* predictive_evidence: Product metadata family removal caused one of the larger PR-AUC drops.
* redundancy_evidence: Correlates with cart breadth and active days, but category breadth is a distinct business concept.
* risk_analysis: Depends on stable product metadata; no product names; pre-cutoff cart events only.
* recommendation: KEEP
* justification: Strong metadata-family evidence and clear behavioral meaning.

### avg_cart_price:

* business_purpose: Typical price level of products added to cart.
* predictive_evidence: Product metadata family has meaningful PR-AUC contribution.
* redundancy_evidence: Related to max_cart_price but captures central tendency rather than upper bound.
* risk_analysis: Requires stable price metadata; null when no cart metadata exists; imputation required.
* recommendation: KEEP
* justification: Retain as interpretable price-affinity context.

### max_cart_price:

* business_purpose: Highest price point considered in cart.
* predictive_evidence: Product metadata family contributes materially to PR-AUC.
* redundancy_evidence: Related to avg_cart_price; still captures willingness to consider high-price items.
* risk_analysis: Metadata quality must be monitored; pre-cutoff only; no product names.
* recommendation: KEEP_BUT_MONITOR
* justification: Retain for V2; monitor with metadata stability because price extrema can be noisy.

### distinct_bought_category_count:

* business_purpose: Breadth of prior purchase categories.
* predictive_evidence: Product metadata family is useful; prior category breadth adds repeat-purchase context.
* redundancy_evidence: Related to distinct_product_buy_sku_count but category-level abstraction is more stable.
* risk_analysis: Depends on product metadata consistency; pre-cutoff only.
* recommendation: KEEP
* justification: Retain as stable category-level repeat-purchase breadth.

### avg_bought_price:

* business_purpose: Average historical purchase price level.
* predictive_evidence: Product metadata family removal caused meaningful PR-AUC drop.
* redundancy_evidence: Related to max_bought_price, but captures typical purchase price rather than extreme.
* risk_analysis: Sparse for users without prior purchases; imputation required; metadata stability should be monitored.
* recommendation: KEEP
* justification: Retain as interpretable buyer price-affinity signal.

### max_bought_price:

* business_purpose: Highest historical purchase price.
* predictive_evidence: Included in metadata family with useful aggregate contribution.
* redundancy_evidence: Partly overlaps with avg_bought_price and purchase count, but captures high-price purchase capacity.
* risk_analysis: Extremes can be noisy; metadata stability matters; no leakage when pre-cutoff.
* recommendation: KEEP_BUT_MONITOR
* justification: Retain for V2 but monitor as a possible simplification candidate.

### search_query_count:

* business_purpose: Total discovery/search activity before cutoff.
* predictive_evidence: Search family removal caused a small PR-AUC and Lift@5 drop; search segments show higher positive rates at high counts.
* redundancy_evidence: Strong overlap with active_days_count and other search windows.
* risk_analysis: Uses no raw query text; count-only representation is privacy-safe but behaviorally noisy.
* recommendation: KEEP_BUT_MONITOR
* justification: Keep because V2-3a bucket replacement failed and V2-2 remains stronger; monitor for future transition/session redesign.

### distinct_search_days:

* business_purpose: Breadth of days with search activity; discovery persistence.
* predictive_evidence: Part of search family with small but nonzero ablation contribution.
* redundancy_evidence: Very high correlation with active_days_count, so it may duplicate general activity.
* risk_analysis: Pre-cutoff aggregate; no raw query text; low leakage risk.
* recommendation: KEEP_BUT_MONITOR
* justification: Retain for V2 because removing/replacing search representation hurt metrics; monitor due high redundancy.

### days_since_last_search_query:

* business_purpose: Freshness of search/discovery behavior.
* predictive_evidence: Recency family contributes meaningfully; search recency is behaviorally interpretable.
* redundancy_evidence: Lower correlation with active_days_count than search count/day features.
* risk_analysis: Sparse for users without search; imputation required; no raw query text persisted.
* recommendation: KEEP
* justification: Retain as the cleanest current search timing signal.

### search_query_count_30d:

* business_purpose: Recent search intensity.
* predictive_evidence: Search count buckets showed strong segment-level positive rates, but V2-3a replacement hurt ranking; raw recent count remains useful in V2-2.
* redundancy_evidence: Correlated with total search count and active_days_count, but captures recent discovery.
* risk_analysis: Pre-cutoff only; no raw query text; heavy-tail behavior should be monitored.
* recommendation: KEEP_BUT_MONITOR
* justification: Keep for V2 stability; monitor heavy-tail/noise for V3 search redesign.

### search_query_count_90d:

* business_purpose: Medium-term search intensity.
* predictive_evidence: Search family has small but nonzero contribution; V2-2 kept 90d after removing 60d.
* redundancy_evidence: Very high correlation with total search count; likely redundant.
* risk_analysis: Pre-cutoff only; privacy-safe count; main risk is unnecessary complexity.
* recommendation: KEEP_BUT_MONITOR
* justification: Retain for production V2 because V2-2 is best stable baseline; monitor as a future simplification candidate.

### active_days_count:

* business_purpose: Overall engagement intensity across event types.
* predictive_evidence: Removing this family caused the largest PR-AUC and Lift@5 drop in E4.
* redundancy_evidence: Proxies many activity families, but that is exactly its useful broad engagement role.
* risk_analysis: Pre-cutoff only; not a cohort artifact; available at inference.
* recommendation: KEEP
* justification: Core production feature with strongest evidence and stable operational behavior.

## Final Production Feature Set (V2-2 Refined)

Final production model should use the V2-2 feature set:

- `add_to_cart_count`
- `distinct_add_to_cart_sku_count`
- `days_since_last_add_to_cart`
- `add_to_cart_count_30d`
- `add_to_cart_count_90d`
- `remove_from_cart_count`
- `distinct_remove_from_cart_sku_count`
- `days_since_last_remove_from_cart`
- `remove_from_cart_count_30d`
- `remove_from_cart_count_90d`
- `product_buy_count`
- `distinct_product_buy_sku_count`
- `days_since_last_product_buy`
- `product_buy_count_30d`
- `product_buy_count_90d`
- `distinct_cart_category_count`
- `avg_cart_price`
- `max_cart_price`
- `distinct_bought_category_count`
- `avg_bought_price`
- `max_bought_price`
- `search_query_count`
- `distinct_search_days`
- `days_since_last_search_query`
- `search_query_count_30d`
- `search_query_count_90d`
- `active_days_count`

## Features Removed With Reasons

- `is_eligible_purchase_propensity`: Remove because it is constant after eligible-cohort filtering and carries no model signal.
- `buy_to_cart_ratio`: Remove because raw ratios were unstable and V2-1 improved after removal.
- `remove_to_cart_ratio`: Remove because raw ratios duplicated count behavior and did not improve ranking.
- `cart_minus_remove_count`: Remove because it belongs to the underperforming raw ratio/difference family.
- `search_to_cart_ratio`: Remove because raw search/cart ratio is unstable with small denominators and V2-1 improved after removal.
- `add_to_cart_count_60d`: Remove because it is a redundant middle rolling window; V2-2 improved while removing 60d windows.
- `product_buy_count_60d`: Remove because it is redundant between 30d and 90d/total purchase counts.
- `remove_from_cart_count_60d`: Remove because it is a redundant middle rolling window.
- `search_query_count_60d`: Remove because it is redundant between 30d and 90d/total search counts.

## Features To Monitor For Future V3

- `add_to_cart_count_90d`: Monitor because it is highly correlated with total add-to-cart count.
- `remove_from_cart_count`, `distinct_remove_from_cart_sku_count`, `remove_from_cart_count_30d`, `remove_from_cart_count_90d`: Monitor because remove behavior is meaningful but current count representation is ambiguous.
- `product_buy_count_90d`: Monitor because it likely overlaps with total purchase count.
- `max_cart_price`, `max_bought_price`: Monitor because extrema can be sensitive to metadata quality.
- `search_query_count`, `distinct_search_days`, `search_query_count_30d`, `search_query_count_90d`: Monitor because search is behaviorally useful but count-only representation is noisy and overlaps with active_days_count.
- `recent_search_then_cart_flag`: Not included in production V2, but keep as the strongest candidate feature for V3 because it improved TopK slightly and has clean behavioral meaning.

## Summary Of Why This Is Finalized

This feature set is finalized because:

- V2-1 removed constant and unstable raw-ratio defects and improved PR-AUC/TopK.
- V2-2 removed redundant 60-day rolling windows and improved or maintained temporal ranking metrics while reducing feature count.
- V2-3a search buckets reduced ranking performance and should not replace raw search representation.
- V2-3b transition features showed real signal but did not beat V2-2 globally.
- V2-3c isolated the best transition flag and improved TopK slightly, but not enough to pass adoption criteria.
- E4/E5 evidence supports keeping behaviorally meaningful activity, recency, metadata, and search signals while removing only proven defects and redundant windows.

Final decision:

**Freeze Baseline V2-2 as the production feature set.**

No further feature iteration is required before production freezing. Future V3 work should be tracked separately as model improvement, not as a blocker for the current production baseline.

## Privacy Confirmation

This artifact contains only aggregate evidence, feature names, and production feature-selection decisions. It does not include raw client IDs, raw query text, product names, row-level examples, row-level predictions, row-level transitions, absolute local paths, secrets, or local environment details.
