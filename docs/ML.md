# ML interpretation and limits

The text pipeline uses TF-IDF followed by multinomial logistic regression. For term $t$ in document $d$, the smoothed inverse document frequency is

$$\operatorname{idf}(t)=\log\!\left(\frac{1+n}{1+\operatorname{df}(t)}\right)+1.$$

Scikit-learn combines this with term frequency and L2-normalizes each sparse document vector. The classifier minimizes regularized cross-entropy; its class probabilities are not calibrated confidence guarantees.

The current corpus has 50 curated examples and the held-out split has only 10. The recorded macro-F1 is 0.4467 for the deployed training configuration. A separate comparison uses a different vectorizer configuration and must not be represented as the same experiment. Improve the corpus with independently labelled reports before interpreting scores as generalization evidence; splitting generated paraphrases of the same templates across train/test would leak information.

Severity uses a random forest over rainfall, water level, wind and temperature. Its 0.9971 macro-F1 was measured on a synthetic holdout drawn from the same label-conditioned generator as training. That high score primarily measures recovery of the generator's structure. It does not validate physical hazard severity or transfer to real sensors. Missing all telemetry returns `unknown`; missing a single feature currently uses a documented simple imputation. Earthquake magnitude is retained as source metadata; the weather-only severity model must not be interpreted as an earthquake damage model.

The report-rate job trains Isolation Forest on up to 167 historical completed hourly bins and scores the latest completed hour. At least 20 training hours are required. Isolation Forest isolates sparse observations through short random-tree paths; for a sample size n, the normalization uses the expected unsuccessful binary-search path length `c(n)=2 H(n-1)-2(n-1)/n`, with anomaly score proportional to `2^(-E[h]/c(n))`. The library's decision function is thresholded using the configured 5% contamination. Trees cannot extrapolate indefinitely beyond their last split: very large future counts can share a boundary leaf. A separate `outside_training_range` rule flags counts exceeding the maximum observed training count. This guard is explicitly distinct from an ML prediction.

## Cost and systems constraints

- Sparse text inference costs approximately O(K * nnz(x)) for K classes. Vocabulary/IDF storage is O(V), with classifier weights O(KV). Keep vectors sparse.
- Forest inference costs O(Td) for T trees of maximum depth d; model storage scales with total tree nodes. Models load once per service process.
- Isolation Forest fitting is approximately O(T * psi * log(psi)) for T trees and subsample size psi; scoring is O(T log(psi)) per observation.
- Training and inference are CPU workloads here. GPU setup adds no demonstrated value to these small classical models.
- No model output is suitable for operational emergency decisions. The dashboard labels this prototype status.
