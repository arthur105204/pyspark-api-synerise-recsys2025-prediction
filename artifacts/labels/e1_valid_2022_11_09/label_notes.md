# Label Construction Notes

This artifact contains aggregate-only label construction notes.

Task: purchase_propensity.
Boundary rule: event_ts >= cutoff_date and event_ts < date_add(target_end, 1).
Eligible cohort count: 2149796.
Positive count: 93614.
Positive rate: 0.043546.
Multiple target-window purchases are aggregated to one binary label row per client.
Feature null handling is unchanged from Phase 3; model-stage imputation is deferred.
No model, prediction, batch scoring, or API output was created.
