# Business Target Selection Notes

This artifact contains aggregate-only EDA notes.

Recommendation: purchase_propensity is recommended as the provisional MVP target using a 30-day target window, pending preprocessing validation.

Leakage rule: Features must only use events before cutoff_date. Labels must only use purchase events from cutoff_date to target_end. No target-window behavior should be used in feature calculations.