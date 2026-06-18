# E4 Feature Ablation Analysis

## Scope
This experiment removes one existing feature family at a time and evaluates temporal validation performance using the same Logistic Regression setup as E1. It does not add features, change labels, modify cohorts, tune hyperparameters, calibrate scores, benchmark new model classes, persist row-level predictions, or write model binaries.

## Baseline Reference
- ROC-AUC: 0.835559
- PR-AUC: 0.253155
- Lift@5%: 6.770770

## Feature Family Mapping
- add_to_cart_activity: add_to_cart_count, distinct_add_to_cart_sku_count, add_to_cart_count_30d, add_to_cart_count_60d, add_to_cart_count_90d
- product_buy_activity: product_buy_count, distinct_product_buy_sku_count, product_buy_count_30d, product_buy_count_60d, product_buy_count_90d
- remove_from_cart_activity: remove_from_cart_count, distinct_remove_from_cart_sku_count, remove_from_cart_count_30d, remove_from_cart_count_60d, remove_from_cart_count_90d
- search_activity: search_query_count, distinct_search_days, search_query_count_30d, search_query_count_60d, search_query_count_90d
- recency_features: days_since_last_add_to_cart, days_since_last_remove_from_cart, days_since_last_product_buy, days_since_last_search_query
- ratio_features: buy_to_cart_ratio, remove_to_cart_ratio, cart_minus_remove_count, search_to_cart_ratio
- product_metadata_features: distinct_cart_category_count, avg_cart_price, max_cart_price, distinct_bought_category_count, avg_bought_price, max_bought_price
- cohort_indicator: is_eligible_purchase_propensity
- overall_activity: active_days_count

## Ablation Results
| Removed family | Removed features | ROC-AUC | PR-AUC | Precision@5% | Lift@5% | PR-AUC drop | Lift@5% drop | Recommendation |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| overall_activity | 1 | 0.833895 | 0.245920 | 0.288985 | 6.636388 | 2.86% | 1.98% | KEEP_LOWER_PRIORITY |
| product_metadata_features | 6 | 0.827523 | 0.248045 | 0.293060 | 6.729964 | 2.02% | 0.60% | KEEP_LOWER_PRIORITY |
| recency_features | 4 | 0.834105 | 0.251568 | 0.289106 | 6.639166 | 0.63% | 1.94% | KEEP_LOWER_PRIORITY |
| search_activity | 5 | 0.835500 | 0.252698 | 0.294120 | 6.754319 | 0.18% | 0.24% | REMOVE_CANDIDATE |
| add_to_cart_activity | 5 | 0.835639 | 0.252896 | 0.294306 | 6.758592 | 0.10% | 0.18% | REMOVE_CANDIDATE |
| cohort_indicator | 1 | 0.835558 | 0.253155 | 0.294837 | 6.770770 | 0.00% | 0.00% | REMOVE_CANDIDATE |
| remove_from_cart_activity | 5 | 0.835879 | 0.253771 | 0.295265 | 6.780597 | -0.24% | -0.15% | REMOVE_CANDIDATE |
| product_buy_activity | 5 | 0.835662 | 0.254673 | 0.296028 | 6.798116 | -0.60% | -0.40% | REMOVE_CANDIDATE |
| ratio_features | 4 | 0.834274 | 0.254870 | 0.296102 | 6.799825 | -0.68% | -0.43% | REMOVE_CANDIDATE |

## Key Findings
- Most signal by PR-AUC drop: overall_activity
- Most important for TopK Lift@5%: overall_activity
- Least disruptive ablation: ratio_features
- Cohort indicator recommendation: Recommend excluding from future modeling

## Final Recommendations
- overall_activity: KEEP_LOWER_PRIORITY
- product_metadata_features: KEEP_LOWER_PRIORITY
- recency_features: KEEP_LOWER_PRIORITY
- search_activity: REMOVE_CANDIDATE
- add_to_cart_activity: REMOVE_CANDIDATE
- cohort_indicator: REMOVE_CANDIDATE
- remove_from_cart_activity: REMOVE_CANDIDATE
- product_buy_activity: REMOVE_CANDIDATE
- ratio_features: REMOVE_CANDIDATE

## Privacy
Artifacts are aggregate-only. No raw client IDs, raw query text, product names, row-level scores, row-level prediction examples, or model binaries are persisted.
