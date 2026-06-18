# E3 Calibration Analysis

## Scope
This evaluation uses the existing temporal Logistic Regression model and temporal validation snapshot. It does not retrain the model, fit a calibration model, modify features, change labels, change thresholds, or persist row-level predictions.

## Validation Population
- Validation rows: 2,149,796
- Positives: 93,614
- Negatives: 2,056,182
- Positive rate: 0.043546
- Average predicted score: 0.336810

## Calibration Curve
| Score bucket | Sample count | Predicted probability avg | Actual positive rate | Absolute gap |
|---|---:|---:|---:|---:|
| 0.0-0.1 | 139,572 | 0.075664 | 0.005309 | 0.070354 |
| 0.1-0.2 | 666,310 | 0.146616 | 0.006162 | 0.140454 |
| 0.2-0.3 | 425,868 | 0.247705 | 0.011698 | 0.236006 |
| 0.3-0.4 | 181,777 | 0.344766 | 0.030609 | 0.314157 |
| 0.4-0.5 | 240,413 | 0.465420 | 0.048987 | 0.416433 |
| 0.5-0.6 | 208,522 | 0.533448 | 0.065163 | 0.468285 |
| 0.6-0.7 | 95,597 | 0.647818 | 0.098152 | 0.549666 |
| 0.7-0.8 | 72,942 | 0.745490 | 0.134490 | 0.611000 |
| 0.8-0.9 | 53,056 | 0.848228 | 0.197998 | 0.650229 |
| 0.9-1.0 | 65,739 | 0.961711 | 0.352272 | 0.609439 |

## Calibration Metrics
- Expected Calibration Error: 0.293265
- Maximum calibration gap: 0.650229
- Average calibration gap: 0.406602
- Buckets with gap < 0.05: 0 of 10
- Monotonic actual positive rate: True
- High-score overconfident: True
- Calibration quality: FAIL

## Questions Answered
1. Can the current LR score be interpreted as a probability? Calibration is poor and probability-based decision making should be avoided until calibration is applied.
2. Does score increase monotonically with actual purchase rate? True.
3. Are high-score users actually buying at similar rates? Review the 0.8-0.9 and 0.9-1.0 buckets above; large positive gaps mean the model is overconfident.
4. Is calibration good enough for business probability interpretation? Use the ECE and bucket gaps above as the decision gate.
5. Should future business decisions use raw probability thresholds or TopK? If calibration is not PASS, prefer ranking-only TopK policies from E2.

## Final Recommendation
Calibration is poor and probability-based decision making should be avoided until calibration is applied.

## E11 Recommendation
E11 optional calibration layer is recommended if E3 is PARTIAL PASS or FAIL and stakeholders need probability interpretation. It is not needed merely to keep using TopK ranking policies.

## Privacy
Artifacts are aggregate-only. No raw client IDs, raw query text, product names, row-level scores, or row-level prediction examples are persisted.
