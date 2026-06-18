# Search Activity Redesign Review for Baseline V2-3

## 1. Executive Summary

Baseline V2-2 is the current experimental baseline. It keeps the search behavior concept but removes one redundant rolling-window feature, `search_query_count_60d`, as part of the broader 60-day rolling-window reduction.

Search activity should remain classified as **REDESIGN**, not **REMOVE**.

The evidence is mixed:

- Search behavior has business meaning: it represents discovery, exploration, comparison, and possible purchase intent before cart activity.
- Aggregate EDA shows higher search-count segments have higher positive rates than no-search users.
- Current count-only search features have weak incremental model contribution in E4.
- Search features overlap strongly with broad activity intensity, especially `active_days_count`.
- Raw query text is privacy-sensitive and should not be persisted or used directly in mentor-facing artifacts.

The V2-3 goal should be to test whether search can become a cleaner intent signal by replacing blunt count-only representations with privacy-safe, aggregate search-intent features.

No code, feature engineering, model training, or scoring was performed for this review.

## 2. Current Search Features

Current search-related features observed in the feature catalog:

| Feature | Current role | Status after V2-2 | Review |
|---|---|---|---|
| `search_query_count` | Total search event count before cutoff | Kept | Broad search activity signal; high variance but noisy. |
| `distinct_search_days` | Number of days with search activity | Kept | Strongly overlaps with general active-day behavior. |
| `days_since_last_search_query` | Search recency | Kept as recency feature | More semantically useful than total count because it captures freshness. |
| `search_query_count_30d` | Recent 30-day search count | Kept | Useful recent activity proxy, but still count-only. |
| `search_query_count_60d` | 60-day search count | Removed in V2-2 | Redundant middle window between 30d and 90d. |
| `search_query_count_90d` | 90-day search count | Kept for separate evaluation | Highly correlated with total count; should be reviewed later. |
| `search_to_cart_ratio` | Raw search/cart ratio | Removed in V2-1 | Unstable ratio; removed as high-confidence defect. |

## 3. Statistical Evidence

### Variance and Sparsity

Search features are not constant and have meaningful variance:

| Feature | Non-null count | Distinct values | Stddev | Min | Max | Interpretation |
|---|---:|---:|---:|---:|---:|---|
| `search_query_count` | 1,703,581 | 592 | 14.971500 | 0 | 2,482 | High spread; total count can be dominated by heavy search users. |
| `distinct_search_days` | 1,703,581 | 79 | 1.633368 | 0 | 104 | Dense aggregate, but likely close to general activity. |
| `search_query_count_30d` | 1,703,581 | 274 | 5.460077 | 0 | 773 | Recent count has signal but can still be noisy. |
| `search_query_count_60d` | 1,703,581 | 403 | 9.430228 | 0 | 1,401 | Removed in V2-2 as redundant middle window. |
| `search_query_count_90d` | 1,703,581 | 524 | 13.029930 | 0 | 2,217 | Strongly overlaps with total count. |
| `days_since_last_search_query` | 481,564 | 110 | 31.765646 | 1 | 109 | Sparse because only users with search history have non-null recency. |

The high maximum values suggest that raw search counts may be heavy-tailed. This can make count-only features sensitive to very active users and less precise as an intent measure.

### Correlation Evidence

V2-2 rolling-window review:

| Feature | Corr with total count | Corr with 30d | Corr previous | Corr next | V2-2 recommendation |
|---|---:|---:|---:|---:|---|
| `search_query_count` | 1.000000 | 0.615079 |  |  | Keep |
| `search_query_count_30d` | 0.615079 | 1.000000 |  | 0.776565 | Keep |
| `search_query_count_60d` | 0.815051 | 0.776565 | 0.776565 | 0.876651 | Remove |
| `search_query_count_90d` | 0.955107 | 0.664375 | 0.876651 |  | Keep, evaluate separately |

Feature rationalization evidence:

- Search family average pairwise correlation: `0.743330`.
- Search family average correlation with `active_days_count`: `0.645473`.
- Maximum search-family correlation with `active_days_count`: `0.870504`.
- `distinct_search_days` vs `active_days_count`: `0.870504`.
- `search_query_count_30d` vs `active_days_count`: `0.480628`.
- `search_query_count_90d` vs `active_days_count`: `0.643668`.

Interpretation:

Search features are not useless, but much of the current representation is entangled with general engagement. `distinct_search_days` especially behaves like a proxy for active days rather than a distinct search-intent signal.

### E4 Ablation Evidence

E4 single-family ablation, removing all search activity features:

| Metric | E1 temporal baseline | Remove search activity | Relative interpretation |
|---|---:|---:|---|
| ROC-AUC | 0.835559 | 0.835500 | Nearly unchanged |
| PR-AUC | 0.253155 | 0.252698 | Small drop |
| Precision@1% | 0.478742 | 0.478789 | Nearly unchanged |
| Precision@5% | 0.294837 | 0.294120 | Small drop |
| Precision@10% | 0.213922 | 0.213480 | Small drop |
| Lift@5% | 6.770770 | 6.754319 | Small drop |

The feature decision matrix recorded search activity as `REVIEW_REDUNDANCY`, with:

- PR-AUC drop relative: `0.001806`.
- Lift@5% drop relative: `0.002430`.

Interpretation:

Current search features contribute weak incremental signal to the class-weighted Logistic Regression baseline. This does not prove search behavior is unimportant; it shows the current count-window representation is not extracting enough independent intent signal.

### Aggregate Target Relationship

Target validation search segments:

| Segment | Client count | Positive count | Positive rate | Lift vs baseline | Interpretation |
|---|---:|---:|---:|---:|---|
| No search | 1,519,157 | 43,054 | 0.028341 | 0.650830 | Below baseline |
| Low search 1-3 | 260,839 | 12,843 | 0.049237 | 1.130708 | Near/slightly above baseline |
| Medium search 4-10 | 200,571 | 13,799 | 0.068799 | 1.579923 | Above baseline |
| High search >10 | 169,229 | 23,918 | 0.141335 | 3.245686 | Strongly above baseline |

Interpretation:

Search count has directional relationship with the target at aggregate segment level. The issue is not absence of signal. The issue is that the current model features encode search mainly as volume, which overlaps with broader activity and does not distinguish search intent quality.

## 4. Why Current Search Counts Underperform

Search count features likely underperform for five reasons:

1. **Intent ambiguity**

   Search can mean serious product intent, casual browsing, comparison shopping, misspelled queries, repeated failed search, or product discovery. A simple count collapses all of these into one number.

2. **Overlap with general engagement**

   Search count and distinct search days correlate strongly with `active_days_count`. The model can already learn much of the broad activity signal without needing detailed search counts.

3. **Heavy-tailed behavior**

   Very high search counts may represent power users, noisy browsing, repeated failed searches, or bot-like/high-friction behavior. Raw counts do not separate useful intensity from noise.

4. **Weak funnel context**

   Search is most valuable when linked to downstream actions such as add-to-cart, remove-from-cart, or purchase. Current features mostly do not encode search-to-cart or search-to-buy sequence information.

5. **Privacy-safe query semantics are missing**

   Raw query text may carry intent, but it should not be persisted in artifacts or exposed. Since current MVP avoids raw text, the model only gets count-level search behavior.

## 5. Candidate Search-Intent Features

These are design candidates only. No new features were implemented.

### Quick Wins

| Candidate feature | Behavioral meaning | Implementation sketch | Expected value | Risk | Complexity |
|---|---|---|---|---|---|
| `has_search_before_cutoff` | Whether user has any search history | Binary from pre-cutoff search count > 0 | Separates no-search users from search-active users | May duplicate count zero/nonzero | Low |
| `search_count_bucket` | Nonlinear search intensity | Bucket total or 30d search count into none/low/medium/high | Handles heavy tails better than raw count | Bucket cutoffs need validation | Low |
| `search_recency_bucket` | Freshness of search intent | Bucket `days_since_last_search_query` into no-search, 0-7, 8-30, 31-60, >60 | More interpretable than raw recency | Requires null handling | Low |
| `recent_search_active_flag` | Recent active search intent | Binary from search in last 30 days | Simple targeting-friendly signal | May overlap with 30d count | Low |

Recommended first V2-3 quick-win experiment:

Replace or augment raw search counts with bucketed search intensity and search recency buckets, while keeping the V2-2 split/model fixed.

### Medium Complexity

| Candidate feature | Behavioral meaning | Implementation sketch | Expected value | Risk | Complexity |
|---|---|---|---|---|---|
| `search_days_per_active_day` | Search concentration among active days | `distinct_search_days / active_days_count`, with smoothing/clipping | Distinguishes search-heavy users from generally active users | Ratio must be smoothed to avoid instability | Medium |
| `search_frequency_per_search_day` | Repeated search intensity on search days | `search_query_count / distinct_search_days`, smoothed | Captures focused/repeated searching | Heavy-tail clipping needed | Medium |
| `recent_search_share` | Whether searches are recent or historical | `search_query_count_30d / search_query_count`, smoothed | Separates fresh search intent from old activity | Ratio stability | Medium |
| `search_to_cart_same_day_count` | Search leading to cart behavior on same day | Join daily search and add-to-cart aggregates per user/date | More funnel-aware than raw count | More expensive joins; sequence approximation | Medium |
| `search_then_cart_within_7d_flag` | Search followed by cart activity | User-level temporal join before cutoff | Captures conversion path | Must avoid target-window leakage | Medium |

Recommended medium experiment:

Test search-to-cart transition signals using only pre-cutoff events. This directly addresses why search counts are weak: they lack downstream behavior context.

### Advanced

| Candidate feature | Behavioral meaning | Implementation sketch | Expected value | Risk | Complexity |
|---|---|---|---|---|---|
| Privacy-safe query normalization features | Query specificity without exposing raw text | Derive aggregate query length buckets, token count buckets, repeated-query counts; do not persist raw query text | Could separate vague from specific search | Privacy review required | High |
| Query repetition pattern | Repeated interest in same/similar intent | Hash or canonicalize in-memory only; persist aggregate repeated-query counts only | Stronger intent signal | Hashing/query handling must be carefully governed | High |
| Search-to-category alignment | Whether search behavior aligns with cart/buy categories | Map search to category only if privacy-safe semantic mapping exists | High business meaning | Requires safe query/category strategy | High |
| Session-level search funnel | Search, cart, remove, buy sequences in sessions | Build sessionized pre-cutoff event paths | Strong behavioral representation | More engineering and validation effort | High |

Advanced features should not be implemented until simpler aggregate redesigns prove that search can add stable incremental lift.

## 6. Leakage Risk Review

Search redesign is feasible only if all features use events strictly before the cutoff date.

Leakage rules:

- Do not use target-window `product_buy` events.
- Do not use target-window `add_to_cart`, `remove_from_cart`, or `search_query` events.
- Do not compute search-to-purchase features that look into the label window.
- If creating search-to-cart transition features, both search and cart events must occur before cutoff.
- If using recency, compute days relative to cutoff, not relative to target-window purchase.
- Do not use model predictions, labels, target event counts, target-window dates, or post-cutoff metadata as inputs.

Candidate leakage risk by feature type:

| Feature group | Leakage risk | Notes |
|---|---|---|
| Search count buckets | Low | Safe if pre-cutoff only. |
| Search recency buckets | Low | Safe if relative to cutoff. |
| Search frequency ratios | Low to medium | Safe if pre-cutoff only; ratio instability is modeling risk, not leakage. |
| Search-to-cart transitions | Medium | Requires careful pre-cutoff temporal join. |
| Search-to-buy transitions | High | Can accidentally overlap with target label if not restricted before cutoff. |
| Query/category semantic features | Medium to high | Privacy and data-governance risk; leakage depends on source and timing. |

## 7. Privacy Review

Search is the highest privacy-risk event family because raw query text may contain sensitive or identifying content.

Privacy rules for V2-3:

- Do not persist raw query text.
- Do not write raw query examples.
- Do not write row-level examples.
- Do not write raw `client_id`.
- Do not persist product names.
- Persist only aggregate counts, buckets, rates, and feature-level summaries.
- If query-derived features are explored, raw text should be transformed in memory and only sanitized aggregate numeric features should be saved.
- Avoid committed artifacts containing query tokens, normalized queries, hashes that could be linked back to rare queries, or per-user query histories.

Privacy-safe candidates:

- Search count buckets.
- Search recency buckets.
- Distinct search days.
- Search days per active day with smoothing.
- Search-to-cart aggregate counts before cutoff.

Privacy-sensitive candidates:

- Raw query text.
- Query examples.
- Rare query categories.
- Per-user repeated query strings.
- Product-name matching from query text.

## 8. Implementation Complexity

| Candidate area | Engineering effort | Runtime impact | Main risk |
|---|---|---|---|
| Count/recency buckets | Low | Low | Cutoff choices may be arbitrary. |
| Smoothed search intensity ratios | Low to medium | Low | Ratio instability if not clipped/smoothed. |
| Search-to-cart daily transitions | Medium | Medium | Requires joining daily user aggregates. |
| Search-to-cart within N days | Medium | Medium to high | Temporal join can be expensive if not aggregated first. |
| Query length/token-count aggregates | Medium | Medium | Requires strict privacy controls. |
| Query semantic/category features | High | High | Privacy and complexity risk. |
| Session-level search funnel | High | High | Requires sessionization and more validation. |

## 9. V2-3 Experiment Definitions

V2-3 experiments should isolate one variable at a time and keep V2-2 fixed as the baseline.

### Experiment V2-3A: Search Count Bucketing

Hypothesis:

Raw search counts are too heavy-tailed; bucketed intensity improves ranking stability.

Change:

- Replace or add bucketed versions of `search_query_count` and `search_query_count_30d`.
- Keep labels, temporal split, model class, class weighting, and evaluation unchanged.

Primary metrics:

- PR-AUC.
- Precision@5%.
- Lift@5%.

Success criteria:

- PR-AUC improves by at least 0.5% relative vs V2-2, or
- Lift@5% improves by at least 0.5% relative without Precision@1% degradation above 1%.

Failure criteria:

- PR-AUC or Lift@5% drops by more than 0.5% relative.

Priority:

Quick Win.

### Experiment V2-3B: Search Recency Bucketing

Hypothesis:

Fresh search activity is more predictive than raw search volume.

Change:

- Add or replace raw `days_since_last_search_query` with interpretable recency buckets.
- Preserve V2-2 otherwise.

Primary metrics:

- PR-AUC.
- Precision@5%.
- Lift@5%.

Success criteria:

- Improves or maintains V2-2 metrics while improving interpretability.

Priority:

Quick Win.

### Experiment V2-3C: Search Activity Normalization

Hypothesis:

Search is useful when normalized against general activity, because current search count overlaps with `active_days_count`.

Change:

- Add smoothed `search_days_per_active_day`.
- Add smoothed/clipped `search_frequency_per_search_day`.
- Do not reintroduce unstable raw `search_to_cart_ratio`.

Primary metrics:

- PR-AUC.
- Precision@5%.
- Lift@5%.

Success criteria:

- At least 0.5% relative improvement in PR-AUC or Lift@5%, with no material Precision@1% loss.

Priority:

Medium Complexity.

### Experiment V2-3D: Pre-Cutoff Search-to-Cart Transition

Hypothesis:

Search becomes more predictive when connected to downstream cart behavior before cutoff.

Change:

- Add aggregate pre-cutoff transition features such as same-day search-to-cart count or search followed by cart within 7 days.
- Do not use post-cutoff or target-window events.

Primary metrics:

- PR-AUC.
- Precision@1%.
- Precision@5%.
- Lift@5%.

Success criteria:

- Meaningful TopK improvement, especially Precision@1% or Precision@5%, because transition features should identify stronger intent users.

Priority:

Medium Complexity.

### Experiment V2-3E: Privacy-Safe Query Shape Features

Hypothesis:

Query specificity can be approximated without storing raw query text.

Change:

- Add aggregate query-shape features such as query length bucket, token count bucket, and repeated-query count.
- Persist only numeric aggregates.

Primary metrics:

- PR-AUC.
- Lift@5%.
- Privacy review pass/fail.

Success criteria:

- Metrics improve and privacy review confirms no raw query content is persisted.

Priority:

Advanced.

## 10. Recommended V2-3 Order

1. **V2-3A: Search Count Bucketing**

   Lowest implementation risk; directly addresses heavy-tail count behavior.

2. **V2-3B: Search Recency Bucketing**

   Also low risk and improves interpretability of freshness.

3. **V2-3C: Search Activity Normalization**

   Tests whether search adds signal beyond general activity.

4. **V2-3D: Pre-Cutoff Search-to-Cart Transition**

   Higher business meaning, but more engineering complexity and leakage risk.

5. **V2-3E: Privacy-Safe Query Shape Features**

   Potentially useful, but should wait until the team explicitly accepts privacy and governance constraints.

## 11. Final Recommendation

For Baseline V2-3, do not remove search activity. Redesign it.

Recommended first implementation:

- Keep V2-2 as the comparison baseline.
- Start with search count buckets and search recency buckets.
- Do not use raw query text.
- Do not implement search-to-cart transitions until quick-win bucket features are evaluated.
- Do not implement query-shape or semantic query features until privacy constraints are reviewed.

Decision:

**Search activity remains REDESIGN. It should be represented as privacy-safe intent intensity, freshness, and eventually pre-cutoff funnel transition behavior, not as raw count-only volume.**

## 12. Sources Used

- `artifacts/features/feature_catalog.csv`
- `artifacts/modeling/feature_rationalization/feature_variance_audit.csv`
- `artifacts/modeling/feature_rationalization/feature_redundancy_matrix.csv`
- `artifacts/modeling/feature_rationalization/feature_decision_matrix.csv`
- `artifacts/modeling/feature_rationalization/feature_family_recommendations.md`
- `artifacts/modeling/e4_feature_ablation/feature_ablation_results.csv`
- `artifacts/modeling/baseline_v2/v22_window_selection_review.csv`
- `artifacts/modeling/baseline_v2/v22_evaluation.md`
- `artifacts/target_validation/search_signal_summary.csv`

## 13. Privacy Confirmation

This artifact contains only aggregate metrics, feature names, design recommendations, and repo-relative artifact references. It does not include raw `client_id`, raw search query text, product names, row-level predictions, row-level scores, row-level examples, absolute local paths, secrets, or local environment details.
