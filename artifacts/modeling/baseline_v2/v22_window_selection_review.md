# Baseline V2-2 Rolling Window Selection Review

## Objective

Validate whether overlapping rolling-window count features can be simplified before adding any new behavioral features.

## Selection Policy

- Keep total count features as broad lifetime activity signals.
- Keep 30-day count features as recent activity signals.
- Remove standalone 60-day count features because they sit between 30-day and 90-day windows and showed high neighboring-window correlation in V2-1 review.
- Keep 90-day count features in V2-2 so this experiment isolates 60-day removal. 90-day removal should be evaluated separately after V2-2 metrics are available.

## Rolling Features Removed In V2-2

- `add_to_cart_count_60d`
- `product_buy_count_60d`
- `remove_from_cart_count_60d`
- `search_query_count_60d`

## Feature Review

| Feature | Family | Window | Variance | Corr total | Corr 30d | Corr previous | Corr next | Recommendation |
|---|---|---:|---:|---:|---:|---:|---:|---|
| add_to_cart_count | add_to_cart_activity | total_count | 44.537758 | 1.0 | 0.707872 |  |  | KEEP_IN_V22 |
| add_to_cart_count_30d | add_to_cart_activity | 30d | 12.366872 | 0.707872 | 1.0 |  | 0.834193 | KEEP_IN_V22 |
| add_to_cart_count_60d | add_to_cart_activity | 60d | 25.510918 | 0.881492 | 0.834193 | 0.834193 | 0.919316 | REMOVE_IN_V22 |
| add_to_cart_count_90d | add_to_cart_activity | 90d | 37.889554 | 0.971731 | 0.743782 | 0.919316 |  | KEEP_IN_V22_EVALUATE_SEPARATELY |
| product_buy_count | product_buy_activity | total_count | 5.430323 | 1.0 | 0.645344 |  |  | KEEP_IN_V22 |
| product_buy_count_30d | product_buy_activity | 30d | 1.363511 | 0.645344 | 1.0 |  | 0.781737 | KEEP_IN_V22 |
| product_buy_count_60d | product_buy_activity | 60d | 3.060837 | 0.854142 | 0.781737 | 0.781737 | 0.897814 | REMOVE_IN_V22 |
| product_buy_count_90d | product_buy_activity | 90d | 4.619583 | 0.960835 | 0.680176 | 0.897814 |  | KEEP_IN_V22_EVALUATE_SEPARATELY |
| remove_from_cart_count | remove_from_cart_activity | total_count | 16.832304 | 1.0 | 0.728958 |  |  | KEEP_IN_V22 |
| remove_from_cart_count_30d | remove_from_cart_activity | 30d | 4.446898 | 0.728958 | 1.0 |  | 0.848869 | KEEP_IN_V22 |
| remove_from_cart_count_60d | remove_from_cart_activity | 60d | 9.233415 | 0.889933 | 0.848869 | 0.848869 | 0.924218 | REMOVE_IN_V22 |
| remove_from_cart_count_90d | remove_from_cart_activity | 90d | 14.230132 | 0.975042 | 0.762035 | 0.924218 |  | KEEP_IN_V22_EVALUATE_SEPARATELY |
| search_query_count | search_activity | total_count | 224.145814 | 1.0 | 0.615079 |  |  | KEEP_IN_V22 |
| search_query_count_30d | search_activity | 30d | 29.812443 | 0.615079 | 1.0 |  | 0.776565 | KEEP_IN_V22 |
| search_query_count_60d | search_activity | 60d | 88.929194 | 0.815051 | 0.776565 | 0.776565 | 0.876651 | REMOVE_IN_V22 |
| search_query_count_90d | search_activity | 90d | 169.779082 | 0.955107 | 0.664375 | 0.876651 |  | KEEP_IN_V22_EVALUATE_SEPARATELY |

## Privacy

This review is aggregate-only. It contains feature-level variance and correlation statistics, not raw client IDs, raw query text, product names, row-level scores, or row-level examples.
