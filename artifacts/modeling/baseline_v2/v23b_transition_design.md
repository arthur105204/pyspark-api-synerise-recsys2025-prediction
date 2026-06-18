# Baseline V2-3b Transition Design Review

## Objective

Baseline V2-3b tests one hypothesis:

**Search becomes more useful when connected to downstream cart behavior.**

This design keeps Baseline V2-2 as the comparison baseline and adds only a small set of aggregate, pre-cutoff search-to-cart transition features. It does not redesign the full search family.

## Available Event Keys

Processed `search_query` events contain:

| Column | Use in V2-3b | Notes |
|---|---|---|
| `client_id` | Join key | User-level link between search and cart events. |
| `event_ts` | Ordering key | Used to confirm search happened before add-to-cart. |
| `event_date` | Optional date filter | Useful for pre-cutoff filtering and possible date-level aggregation. |
| `event_type` | Metadata | Not needed for feature calculation after table selection. |

Processed `add_to_cart` events contain:

| Column | Use in V2-3b | Notes |
|---|---|---|
| `client_id` | Join key | User-level link between search and cart events. |
| `event_ts` | Ordering key | Used as the downstream cart timestamp. |
| `event_date` | Pre-cutoff filter | Used to enforce feature-window boundaries. |
| `event_type` | Metadata | Not needed for feature calculation after table selection. |
| `sku` | Not used for search linking | Search events do not contain SKU, so same-product linking is not available. |

## Linking Feasibility

Search and add-to-cart events can be linked safely by:

- `client_id`
- temporal order: `search.event_ts <= add_to_cart.event_ts`
- pre-cutoff filtering for both event types

Search events cannot be linked safely by:

- SKU, because processed search events do not contain SKU.
- Category, because processed search events do not contain category.
- Raw query text, because raw query text must not be persisted or used in committed artifacts.

## Event Ordering Assumptions

V2-3b assumes:

- `event_ts` is parsed and comparable across event tables.
- A search can be considered prior context for a cart event if it occurs before the add-to-cart timestamp.
- Same-user transition context is sufficient for this experiment.
- Search-to-cart features are aggregate user-level summaries, not row-level sequences.

This is weaker than true session modeling. It is intentionally small and leakage-safe.

## Proposed Minimal Feature Set

| Feature | Definition | Intended meaning |
|---|---|---|
| `search_before_cart_count` | Number of pre-cutoff add-to-cart events preceded by at least one search from the same user within 7 days. | Cart behavior with search context. |
| `search_to_cart_rate` | Smoothed `search_before_cart_count / add_to_cart_count`. | Share of cart behavior that appears search-assisted. |
| `recent_search_then_cart_flag` | 1 when a search in the final 30 pre-cutoff days was followed by cart activity before cutoff. | Recent search progressing into cart intent. |

## Leakage Review

Leakage controls:

- Both search and add-to-cart events are filtered to `event_date < cutoff_date`.
- Search timestamps must be before or equal to cart timestamps.
- No target-window `product_buy` events are used.
- No label, prediction, target count, or target-window metadata is used.
- Transition features are computed separately for each temporal snapshot using that snapshot's cutoff date.

Leakage risk is low if the cutoff filter and event ordering conditions are preserved.

## Privacy Review

Privacy controls:

- Raw query text is not read from processed data and is not persisted.
- No raw `client_id` values are written to artifacts.
- No row-level transition examples are written.
- No product names are used or persisted.
- Artifacts contain only aggregate metrics, feature definitions, and model-level summaries.

## Computational Complexity

The transition count requires a temporal self-style join between search events and add-to-cart events by `client_id` and timestamp range. To control complexity:

- Both event tables are filtered before cutoff.
- Search rows use only `client_id` and `event_ts`.
- Add-to-cart rows use only `client_id` and `event_ts`.
- The join result is reduced immediately to aggregate user-level counts.

Expected complexity is medium. It is more expensive than pure feature-table transformations but smaller than sessionization or query semantic processing.

## Decision Boundary

Adopt V2-3b only if transition features improve or maintain V2-2 temporal ranking metrics while adding interpretable search-funnel signal.

If V2-3b underperforms V2-2, keep V2-2 and treat search-to-cart transition engineering as requiring a more careful session/category-aware design.

## Privacy Confirmation

This artifact is aggregate/design-only. It contains no raw client IDs, raw query text, product names, row-level predictions, row-level scores, row-level transition examples, absolute local paths, secrets, or local environment details.
