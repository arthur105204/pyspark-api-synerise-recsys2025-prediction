# Baseline V2-3c Feature Selection Review

## Objective

V2-3c is a pruning experiment, not a redesign experiment.

The goal is to isolate one transition signal from V2-3b:

`recent_search_then_cart_flag`

This tests whether a single clean behavioral flag improves Baseline V2-2 without the redundancy/noise introduced by the full V2-3b transition set.

## Evidence From V2-3b

V2-3b added three transition features:

| Feature | Coefficient | Abs coefficient rank | Positive-rate evidence | Decision |
|---|---:|---:|---|---|
| `recent_search_then_cart_flag` | 0.409962 | 2 | Flag = 1 positive rate: 0.152753; flag = 0 positive rate: 0.031564 | Keep for V2-3c |
| `search_to_cart_rate` | -0.107695 | 5 | High-rate bucket has signal, but non-monotonic buckets suggest mixed interpretation | Remove from V2-3c |
| `search_before_cart_count` | -0.007490 | 22 | Count buckets have signal only at high values; coefficient is near zero | Remove from V2-3c |

V2-3b model-level result:

- PR-AUC decreased slightly versus V2-2.
- Precision@1% and Precision@5% improved slightly.
- Precision@10% decreased.
- Recommendation was `Investigate further`.

## Selected Feature

`recent_search_then_cart_flag` is selected because:

- It has the strongest coefficient magnitude among V2-3b transition features.
- It ranked #2 by absolute coefficient among all model features in V2-3b.
- It has a clean behavioral meaning: recent search progressed into cart activity before cutoff.
- It has strong positive-rate separation.
- It is sparse but not constant.
- It avoids heavy-tailed count behavior.
- It avoids noisy smoothed-rate interpretation.

## Removed V2-3b Transition Features

`search_before_cart_count` is removed because:

- Its coefficient was near zero in V2-3b.
- It is heavy-tailed, with high values likely overlapping with general activity.
- It adds complexity without clear independent coefficient strength.

`search_to_cart_rate` is removed because:

- It had a notable coefficient but negative direction.
- Positive-rate buckets were not cleanly monotonic.
- Smoothed rate behavior may be difficult to explain and may overlap with cart intensity.

## V2-3c Feature Set

Start from Baseline V2-2 and add only:

- `recent_search_then_cart_flag`

Do not include:

- `search_before_cart_count`
- `search_to_cart_rate`
- any additional transition features
- any search family redesign
- trend/session/query/category features

## Expected Outcome

If V2-3c improves TopK metrics and maintains PR-AUC, then the recent search-to-cart flag is likely additive.

If V2-3c performs similarly to V2-2, the flag may be useful for interpretability but not strong enough to adopt.

If V2-3c underperforms, keep V2-2 and defer transition features until a more careful session or category-aware design.

## Privacy Confirmation

This artifact uses aggregate metrics and feature names only. It contains no raw client IDs, raw query text, product names, row-level examples, row-level transitions, row-level scores, absolute local paths, secrets, or local environment details.
