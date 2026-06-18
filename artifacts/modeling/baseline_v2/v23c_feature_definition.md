# Baseline V2-3c Feature Definition

## Feature

`recent_search_then_cart_flag`

## Definition

Binary indicator equal to 1 when a user has at least one pre-cutoff search event in the final 30 days before cutoff that is followed by at least one pre-cutoff add-to-cart event.

Otherwise, the flag is 0.

## Time-Window Logic

For each temporal snapshot:

- Filter `search_query` events to `event_date < cutoff_date`.
- Filter `add_to_cart` events to `event_date < cutoff_date`.
- Keep only searches where `search_ts >= cutoff_date - 30 days`.
- Join search and cart events by `client_id`.
- Require `search_ts <= cart_ts`.
- Aggregate to one binary feature per user.

Snapshot cutoffs:

- Train snapshot cutoff: `2022-10-10`.
- Validation snapshot cutoff: `2022-11-09`.

## Leakage Risk

Leakage risk is low if the following controls are preserved:

- No target-window events are used.
- No `product_buy` target-window rows are used.
- No label, target event count, prediction, or post-cutoff metadata is used.
- The feature is computed separately per snapshot with that snapshot's cutoff.

## Sparsity and Stability

V2-3b training-snapshot evidence:

- Non-null rate: 1.000000.
- Distinct values: 2.
- Mean: 0.081686.
- Zero rate: 0.918314.
- Flag = 1 row count: 139,159.
- Flag = 1 positive rate: 0.152753.
- Flag = 0 positive rate: 0.031564.

The feature is sparse but not rare enough to be unusable. Its behavior is stable enough for an isolated pruning experiment.

## Privacy

This feature uses only pre-cutoff timestamps and aggregate user-level existence logic. Artifacts do not include raw client IDs, raw query text, product names, row-level examples, row-level transitions, or row-level predictions.
