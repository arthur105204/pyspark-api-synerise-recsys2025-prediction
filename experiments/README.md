# Experiments

This folder contains offline modeling and evaluation experiments that extend the
main purchase prediction pipeline.

The main reproducible pipeline stages remain in `jobs/`:

- raw inspection and EDA
- preprocessing
- feature engineering
- label construction
- baseline model training
- batch scoring
- serving exports

Experiment scripts live here because they compare validation strategies,
threshold policies, calibration, feature ablations, feature-set variants, and
final model selection. They should not be treated as API request-path code or
serving jobs.

## Run Pattern

Run from the repository root:

```bash
python experiments/<script_name>.py
```

Each experiment writes aggregate-only artifacts under `artifacts/`. Model
outputs, when produced, are written under ignored `data/models/` paths and
should not be committed unless explicitly approved.

## Scripts

| Script | Purpose |
|---|---|
| `05b_threshold_analysis.py` | E2 threshold and decision policy analysis for temporal validation scores. |
| `05c_calibration_analysis.py` | E3 calibration analysis for temporal validation scores. |
| `05d_feature_ablation.py` | E4 single-family feature ablation. |
| `05e_feature_redundancy_followup.py` | E4 follow-up combined ablation and redundancy audit. |
| `05f_feature_rationalization_audit.py` | Feature rationalization audit combining variance, redundancy, and ablation evidence. |
| `05g_train_baseline_v21.py` | Baseline V2-1 high-confidence feature defect removal. |
| `05h_train_baseline_v22.py` | Baseline V2-2 rolling-window reduction. |
| `05i_train_baseline_v23a.py` | V2-3a search bucket quick-win experiment. |
| `05j_train_baseline_v23b.py` | V2-3b search-to-cart transition experiment. |
| `05k_train_baseline_v23c.py` | V2-3c transition feature pruning experiment. |
| `05l_train_e6_velocity.py` | E6 trend/velocity feature experiment. |
| `05m_e6_pruning.py` | E6.1 velocity feature pruning experiment. |
| `05n_train_baseline_v24.py` | V2-4 consolidated candidate training. |
| `05o_e9_final_benchmark.py` | E9 final benchmark between V2-2 and V2-4. |

## Guardrails

- Do not commit raw data, processed data, model binaries, local databases, or
  row-level prediction outputs.
- Keep artifacts aggregate-only.
- Do not write raw `client_id` values, raw search query text, product names, or
  row-level examples.
- Keep production serving/API changes separate from offline experiments.
