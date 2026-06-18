# Target and Problem Framing Review

## 1. Current target definition

Prediction unit:
One row per eligible `client_id`.

Current eligible cohort:
Clients with at least one pre-cutoff `add_to_cart` or `product_buy` event.

Feature window:
Historical behavior before cutoff date `2022-11-09`.

Label window:
30-day target window from `2022-11-09` through `2022-12-08`.

Positive label definition:
Eligible client has at least one `product_buy` event in the target window.

Negative label definition:
Eligible client has no `product_buy` event in the target window.

Current target name:
Previously described as 30-day purchase prediction or purchase propensity.

What it claims:
The current target predicts whether prior cart/purchase users will purchase in the next 30 days.

What it does not claim:
It is not a broad all-active-user purchase propensity model, because the eligibility cohort does not include all users with page visits or search-only activity. It is also not exactly cart conversion, because it includes users with purchase history even when they have no prior cart event.

## 2. Candidate problem definitions

### Option A: Current target, reframed

Name:
`30-day purchase prediction for prior cart/purchase users`

Cohort:
Clients with pre-cutoff `add_to_cart` or `product_buy`.

Business action:
Rank prior cart/purchase users for purchase follow-up.

Positive label source:
`product_buy` events in the 30-day target window.

Expected complexity:
Low incremental complexity because this is the current implemented path.

Data needed:
Processed `add_to_cart`, `product_buy`, existing features, and labels.

Current pipeline reuse:
High.

Expected class imbalance risk:
High. Positive rate is `0.043546`.

API serving fit:
Good after offline scoring.

Pros:
Clear enough for MVP, already leakage-safe, already implemented, broader than cart-only, and supported by feature-target EDA.

Cons:
The name “purchase propensity” is too broad if not qualified by the cohort.

Recommendation status:
Keep for MVP, but rename/reframe.

### Option B: Cart conversion prediction

Name:
`30-day cart conversion prediction`

Cohort:
Clients with pre-cutoff `add_to_cart`.

Business action:
Predict which cart-intent users will purchase.

Positive label source:
`product_buy` events in the 30-day target window.

Expected complexity:
Low to medium. It can reuse most logic but requires a new cohort/label rebuild and review.

Data needed:
Processed `add_to_cart`, `product_buy`, and cart-related features.

Current pipeline reuse:
High.

Expected class imbalance risk:
High. Positive rate is `0.040271`.

API serving fit:
Good after offline scoring.

Pros:
Very clear business framing and easy mentor explanation.

Cons:
Narrower than the current target. It excludes `277,087` current-cohort clients who have prior purchase history but no cart history, and target-window purchase evidence shows purchase does not require prior cart.

Recommendation status:
Future extension or comparison target, not a replacement now.

### Option C: Search/cart/buy active-user purchase propensity

Name:
`30-day purchase propensity for search/cart/buy active users`

Cohort:
Clients with pre-cutoff `search_query`, `add_to_cart`, or `product_buy`.

Business action:
Predict purchase among a broader behavior-active cohort without using `page_visit`.

Positive label source:
`product_buy` events in the 30-day target window.

Expected complexity:
Medium. Processed search is available, but the cohort, labels, features, and metrics would need to be rebuilt and reviewed.

Data needed:
Processed `search_query`, `add_to_cart`, `product_buy`, and existing feature logic.

Current pipeline reuse:
Medium-high.

Expected class imbalance risk:
High. Positive rate is `0.035199`.

API serving fit:
Good after offline scoring, but requires a new score table.

Pros:
Broader and closer to general purchase propensity than the current cohort. It adds `647,037` clients beyond the current cohort.

Cons:
Search-only users have low positive rate, `0.007466`, so broadening with search adds coverage but also imbalance/noise. Raw query text is intentionally excluded, limiting search intent interpretation.

Recommendation status:
Broaden cohort in next iteration, not inside the current MVP.

### Option D: All-observed active-user purchase propensity

Name:
`30-day purchase propensity for all observed active users`

Cohort:
Clients with pre-cutoff `page_visit`, `search_query`, `add_to_cart`, or `product_buy`.

Business action:
Broadest active-user purchase propensity.

Positive label source:
Would use `product_buy` events in the 30-day target window after building a page-visit-aware cohort.

Expected complexity:
High.

Data needed:
Processed `page_visit` plus existing event tables.

Current pipeline reuse:
Medium. The concept can reuse parts of the pipeline, but page-visit processing and features are not part of the current MVP.

Expected class imbalance risk:
Unknown until the page-visit cohort is built.

API serving fit:
Future fit after offline scoring.

Pros:
Most complete active-user coverage.

Cons:
`page_visit` has 199,451,980 rows and 20,927,946 distinct clients in raw EDA. It is high-volume and likely weaker intent than cart, purchase, or search. It would require a dedicated EDA and feature strategy before modeling.

Recommendation status:
Future extension requiring page-visit EDA.

### Option E: Repeat purchase prediction

Name:
`30-day repeat purchase prediction`

Cohort:
Clients with pre-cutoff `product_buy`.

Business action:
Predict whether previous buyers will buy again.

Positive label source:
`product_buy` events in the 30-day target window.

Expected complexity:
Low to medium.

Data needed:
Processed `product_buy` and product metadata.

Current pipeline reuse:
Medium-high.

Expected class imbalance risk:
Moderate. Positive rate is `0.105069`.

API serving fit:
Good after offline scoring.

Pros:
Clear retention-style business problem and better positive rate than the current target.

Cons:
Much narrower than the current MVP and no longer a general purchase-intent scoring problem.

Recommendation status:
Future extension.

### Option F: Product/category recommendation or next-purchase ranking

Name:
`next product/category ranking`

Cohort:
Users with enough historical behavior to support candidate generation and ranking.

Business action:
Rank products or categories likely to be bought next.

Positive label source:
Future product/category purchases, not just a binary purchase flag.

Expected complexity:
High.

Data needed:
Product events, product metadata, candidate generation, ranking labels, and ranking evaluation.

Current pipeline reuse:
Low to medium. Some aggregates and metadata are reusable, but the modeling task is different.

Expected class imbalance risk:
Different from binary classification; ranking sparsity and candidate explosion become the main risks.

API serving fit:
Different API contract would be needed.

Pros:
Closer to recommendation use cases.

Cons:
Too large a scope change for the current MVP.

Recommendation status:
Future extension, not recommended now.

## 3. Additional EDA needed

Existing artifacts were not sufficient to compare all candidate problem framings, so a new aggregate-only EDA job was added:

`jobs/01d_compare_candidate_problem_framings.py`

Generated artifacts:

- `artifacts/problem_framing/candidate_cohort_comparison.csv`
- `artifacts/problem_framing/candidate_target_balance.csv`
- `artifacts/problem_framing/candidate_overlap_matrix.csv`
- `artifacts/problem_framing/candidate_feature_availability.csv`
- `artifacts/problem_framing/candidate_processing_cost_estimate.csv`
- `artifacts/problem_framing/problem_framing_recommendation.md`

The job uses processed add-to-cart, product-buy, and search-query event tables. It does not process `page_visit`; all-active page-visit framing is documented from existing EDA counts as a high-cost future extension.

## 4. Candidate cohort comparison

| Candidate cohort | Cohort clients | Positive clients | Positive rate | Extra clients vs current | Requires page_visit |
| --- | ---: | ---: | ---: | ---: | --- |
| Current: add_to_cart OR product_buy | 2,149,796 | 93,614 | 0.043546 | 0 | No |
| Cart-only | 1,872,709 | 75,415 | 0.040271 | 0 | No |
| Search/cart/buy | 2,796,833 | 98,445 | 0.035199 | 647,037 | No |
| Buy-only | 739,219 | 77,669 | 0.105069 | 0 | No |
| Search-only diagnostic | 647,037 | 4,831 | 0.007466 | 647,037 | No |
| All-active with page_visit | 20,927,946 from raw EDA | Not computed | Not computed | Not computed | Yes |

The search/cart/buy cohort is the most plausible broadening path that does not require page-visit processing. It adds coverage but lowers positive rate, mainly because search-only clients have much weaker purchase rate.

## 5. Cart conversion clarification

Cart users before cutoff:
`1,872,709`.

Cart users who buy in target window:
`75,415`.

Cart conversion positive rate:
`0.040271`.

Difference from current target:
The current target has `2,149,796` clients and `93,614` positives. It includes `277,087` clients beyond the cart-only cohort. Therefore, current target is not identical to cart conversion.

Future buyers without prior cart:
Target-window purchase path validation found `172,245` target-window buyers without pre-cutoff add-to-cart. This supports not reducing the current MVP to a cart-only target.

## 6. Search cohort clarification

Search clients before cutoff:
`1,277,676`.

Search-only clients before cutoff:
`647,037`.

Search-only clients who buy in target window:
`4,831`.

Search-only positive rate:
`0.007466`.

Overlap:
The search/cart/buy cohort contains `2,796,833` clients, adding `647,037` clients beyond the current cohort.

Interpretation:
Search can broaden the target, but search-only users have low purchase rate. Including search-only clients would make the target more general but also more imbalanced and noisier. This is defensible as a next iteration after mentor review, not as an immediate MVP replacement.

## 7. Page visit decision

`page_visit` should remain deferred for the current MVP.

Evidence:

- Raw `page_visit` has `199,451,980` rows.
- Raw `page_visit` has `20,927,946` distinct clients.
- It is much larger than search, cart, and purchase event tables.
- It may add broad coverage, but page visits are weaker intent signals than cart, purchase, and search.
- It is not required to validate the current scoring path, which already has a defined cohort, labels, features, model, batch scoring, and API lookup.

Cost-benefit:
Including page visits would make the problem broader, but it would also require a dedicated preprocessing strategy, page-level feature design, noise analysis, and new cohort/label review. The current MVP can justify deferring it because the goal is to demonstrate a leakage-safe end-to-end scoring pipeline, not to maximize all possible active-user coverage.

Future EDA needed:
Before including `page_visit`, run aggregate EDA for page-visit frequency, recency, distinct URLs, overlap with search/cart/buy users, and target-window positive rates by page-visit activity bucket.

## 8. Product metadata consistency

Existing target validation artifacts show:

- raw metadata has `0` SKU rows with multiple category values
- raw metadata has `0` SKU rows with multiple price values
- processed metadata has `0` SKU rows with multiple category values
- processed metadata has `0` SKU rows with multiple price values

SKU does not need to be unique in event tables because events are transactional. The same SKU can appear many times across users and timestamps. SKU should be stable and unique in product metadata because that table is used as a dimension lookup. If future data has duplicate or inconsistent SKU metadata, the pipeline should deduplicate deterministically by SKU or aggregate metadata before joining.

## 9. Feature meaning review

Simple aggregate features are defensible for a baseline:

- Counts capture activity intensity.
- Recency captures how recently the user interacted or bought.
- Ratios capture conversion-style relationships, such as buy-to-cart.
- Search count has aggregate lift in some segments, but search-only users are weak, so it should not be overclaimed as direct purchase intent.
- Product category and price features are defensible because metadata is stable by SKU in current checks.

Limitations:
These features are predictive signals, not causal explanations. Count features can be noisy. Same-SKU cart-to-buy path is weak, with only `3.579%` of target purchase events having prior same-SKU add-to-cart. Future work may need sequence features, category-level transitions, and session-based features.

## 10. Decision matrix

| Option | Business clarity | Data support | Cohort coverage | Implementation cost | Mentor defensibility | Reuse current pipeline | Recommended action |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Option A: Current target, reframed | High | High | Medium | Low | High | High | Keep current target but rename/reframe |
| Option B: Cart conversion | Very high | High | Medium-low | Low-medium | High | High | Keep as future extension or comparison target |
| Option C: Search/cart/buy active users | Medium-high | Medium | Medium-high | Medium | Medium | Medium-high | Broaden cohort in next iteration |
| Option D: All observed active users | Medium | Incomplete | Very high | High | Medium-low now | Medium | Keep as future extension after page-visit EDA |
| Option E: Repeat purchase | High | High | Low | Low-medium | High | Medium-high | Keep as future extension |
| Option F: Next product/category ranking | High for recommendation | Not prepared for current MVP | Depends on candidate strategy | High | Low for current MVP | Low-medium | Not recommended for MVP |

## 11. Final recommendation

For the current MVP, I recommend keeping the current target but renaming/reframing it as `30-day purchase prediction for prior cart/purchase users`, because this is the most defensible interpretation of the implemented cohort, reuses the completed leakage-safe pipeline, is meaningfully different from cart conversion, and avoids introducing a new cohort/modeling cycle before mentor review.

The current target should not be described as broad all-active-user purchase propensity. A broader `search/cart/buy active-user purchase propensity` target is a good next iteration, but search-only users have low positive rate and would require rebuilding labels, features, metrics, and serving outputs. Cart conversion, repeat purchase, all-active page-visit propensity, and next-product/category ranking should remain future extensions rather than replacing the current MVP now.

## 12. Documentation update

`docs/project_report.md` should summarize this review briefly and point to this document for the detailed comparison. The report should state that the current target remains valid but should be renamed or framed more precisely for mentor review.
