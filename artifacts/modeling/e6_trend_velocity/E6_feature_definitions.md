# E6 Feature Definitions

## Scope

E6 adds only derived trend/velocity features on top of the frozen Baseline V2-2 feature set. Existing V2-2 features are not modified or removed.

| Feature | Definition | Rationale | Leakage/null handling |
|---|---|---|---|
| `cart_velocity_30d_vs_90d` | `(add_to_cart_count_30d + 1) / (add_to_cart_count_90d + 1)` | Captures whether cart activity is concentrated recently. | Uses pre-cutoff counts; +1 smoothing prevents division by zero. |
| `cart_delta_30d_90d` | `add_to_cart_count_30d - (add_to_cart_count_90d / 3)` | Compares recent cart volume against an average 30-day slice of 90-day history. | Uses pre-cutoff counts; nulls treated as 0. |
| `buy_velocity_30d_vs_90d` | `(product_buy_count_30d + 1) / (product_buy_count_90d + 1)` | Captures recent acceleration in purchase behavior. | Uses pre-cutoff counts; +1 smoothing prevents division by zero. |
| `buy_delta_30d_90d` | `product_buy_count_30d - (product_buy_count_90d / 3)` | Compares recent buying to medium-term buying pace. | Uses pre-cutoff counts; nulls treated as 0. |
| `search_velocity_30d_vs_90d` | `(search_query_count_30d + 1) / (search_query_count_90d + 1)` | Captures recent acceleration in discovery/search activity. | Uses pre-cutoff counts; no raw query text. |
| `activity_intensity_ratio` | `(active_days_count_30d_proxy + 1) / (active_days_count + 1)` | Captures recent activity concentration across cart, buy, remove, and search events. | `active_days_count_30d_proxy` is computed from pre-cutoff event dates only and is not used as a model feature. |

## Non-Goals

- No sequence or graph features.
- No search-to-cart transition features.
- No query text or embedding features.
- No model architecture changes.
- No production baseline retraining.

## Privacy

Artifacts are aggregate-only. No raw client IDs, raw query text, product names, row-level predictions, or row-level examples are persisted.
