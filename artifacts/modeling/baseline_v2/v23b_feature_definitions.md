# Baseline V2-3b Feature Definitions

## Scope

Baseline V2-3b adds only aggregate pre-cutoff search-to-cart transition context to Baseline V2-2. It preserves the temporal split, labels, Logistic Regression model class, median imputation, class weighting, and TopK evaluation.

## New Features

| Feature | Definition | Aggregation logic | Null handling | Leakage review | Privacy review | Complexity |
|---|---|---|---|---|---|---|
| `search_before_cart_count` | Count of add-to-cart events preceded by at least one search within 7 days. | Filter search/cart events before cutoff, link by `client_id`, require `search_ts <= cart_ts`, then aggregate to user level. | Missing transition count is filled with 0. | Uses pre-cutoff events only. | Does not use raw query text or row examples. | Medium temporal join. |
| `search_to_cart_rate` | Smoothed share of cart events with search context. | `(search_before_cart_count + 1) / (add_to_cart_count + 2)` using pre-cutoff cart count from transition source. | Users without transition rows are filled with 0. | Uses pre-cutoff events only. | Aggregate numeric feature only. | Low after transition count is computed. |
| `recent_search_then_cart_flag` | 1 when a search in the final 30 pre-cutoff days was followed by add-to-cart before cutoff. | Link recent pre-cutoff search to later pre-cutoff cart for the same user. | Missing flag is filled with 0. | Uses pre-cutoff events only. | Aggregate binary feature only. | Medium temporal join. |

## Explicit Non-Goals

- No full search family redesign.
- No raw query text features.
- No query embeddings.
- No session features.
- No trend features.
- No category transition features.
- No label, split, model class, threshold, or calibration changes.

## Privacy

The new features use only pre-cutoff timestamps and aggregate user-level counts/rates/flags. Artifacts do not include raw client IDs, raw query text, product names, row-level examples, or row-level predictions.
