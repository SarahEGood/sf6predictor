# Street Fighter 6 Tournament Prediction: An Exploratory Case Study

## Project question

Can historical tournament results and participation records help predict which player will win a Street Fighter 6 set?

I started this project ahead of Evolution 2024, when SF6 had less than a year of competitive history. The work covered data collection, player identity matching, feature engineering, and exploratory modeling. I discontinued it after encountering data-quality and evaluation problems; it does not establish a reliable tournament forecasting system.

Repository: https://github.com/SarahEGood/sf6predictor

## Data pipeline

- **Collection:** Queried start.gg's GraphQL API for tournament and set records, including SF6 and later SFV. Supplemented coverage by scraping Liquipedia with BeautifulSoup and organizing records in pandas.
- **Identity matching:** Used fuzzy name matching to identify candidate player matches, with manual checks and an alias map to reconcile names across sources.
- **Historical features:** Calculated Elo and tournament participation counts by competition tier. The modeling notebook also constructs total participation counts and a days-between-events feature.
- **Model preparation:** Joined the two players' records into a set-level row, encoded the outcome as a binary target, and randomly swapped player sides to reduce ordering imbalance. Applied row-wise L1 normalization before the initial random train/test split.

The collection was not verified to contain every relevant match. The notebook filters event metadata to SF6; this alone does not establish whether SFV results influenced upstream Elo records.

## Models actually implemented

| Model | Saved implementation |
| --- | --- |
| Lasso regression | `Lasso(alpha=0.1)`, fitted to the binary outcome |
| Tweedie regression | `TweedieRegressor(power=1, alpha=0.5, link='log')`; predictions rounded for classification evaluation |
| Multilayer perceptron | `MLPClassifier` with one 15-unit hidden layer and the L-BFGS solver |

A `LinearRegression` object is instantiated but replaced by Lasso before the displayed fitting step. It is not a separately evaluated model.

### Reasons for exploring these models

I expected ordinary multiple linear regression to be too restrictive because some predictors might have nonlinear relationships with the outcome, and their effects might depend on one another. This motivated exploring both a regularized linear model and a nonlinear classifier; it did not establish in advance that a linear approach would fail.

- **Lasso:** I wanted to test whether penalizing coefficients and potentially reducing less useful ones to zero would improve predictions. This was relevant to a feature set containing potentially overlapping measures such as total participation and participation by tournament tier. Lasso still models a linear relationship in its input features; its role here was regularization and potential feature selection, rather than learning nonlinear interactions automatically.
- **Tweedie:** This was an exploratory choice, while Lasso and the MLP had clearer motivations. The saved configuration uses a Poisson distribution with a log link (`power=1`), which is not a natural match for a binary win/loss target and does not constrain predictions to the probability range of 0–1. I therefore treat it as an experiment rather than a specifically justified model for this outcome.
- **MLP:** I chose a multilayer perceptron classifier because I expected nonlinear relationships and interactions among the predictors. For example, participation history might affect how informative an Elo rating is. The model can learn such relationships without requiring every interaction to be specified manually; this example describes the motivation, not an interaction demonstrated by the results.

### Feature preprocessing in the saved notebook

The notebook applies row-wise L1 normalization, later adds 1 to the normalized values before chi-squared feature selection, and selects seven features with `SelectKBest`. These are preprocessing and feature-selection steps. The saved modeling code does not explicitly add polynomial terms, log-transform individual predictors, or construct interaction features to address nonlinearity. The Tweedie model's log link is part of the model specification, not a logged-input feature transformation.

The model-choice motivations above reflect my retrospective recollection. The implementation and preprocessing details are verified from the saved notebook; the exact original selection process and hyperparameter rationale are not fully documented.

## Saved prediction results

These are historical notebook outputs, not results from a fresh run or validated estimates for future tournaments.

| Experiment | Saved output | Interpretation |
| --- | --- | --- |
| Lasso | R² approximately −0.00000058; RMSE approximately 0.50000014 | Essentially mean-baseline performance on the nearly balanced binary target |
| Initial Tweedie | Approximately 67% accuracy; recall 0.97 for P1 wins and 0.38 for P2 wins | Uneven performance across player sides; many displayed predictions cluster near 0.5 |
| MLP after feature selection | Approximately 85% accuracy | Apparent performance under the saved evaluation procedure; leakage and preprocessing problems prevent a forecasting claim |

A later feature-selected Tweedie cell displays approximately 50% accuracy, but it reuses `y_preds` from the earlier split instead of computing predictions for the new test set. That report is not a valid evaluation of the refitted model. The MLP uses a different split and selected features, so the saved figures do not constitute a controlled model comparison.

## Methodological limitations

- **Evaluation did not simulate future prediction.** The notebook randomly splits rows, using 30% for training and 70% for testing. Training is not restricted to events preceding the test events.
- **Feature timing creates leakage risk.** Although the notebook builds shifted prior-event features in `elo3`, its initial set join uses unshifted `elo2` for the x-side player. Those Elo records are updated using the event's results. Swapping player sides later does not remove that information.
- **Feature selection sees the test labels.** `SelectKBest` is fitted on the full dataset before the later train/test split.
- **Event intervals are incorrectly grouped.** The days-between-events calculation groups by both player and event before taking a difference, rather than comparing consecutive events for each player. The 999-day fill value therefore cannot reliably identify newcomers or inactivity.
- **Player joins need validation.** Saved previews show missing opponent feature matches replaced with defaults. The matching and merge logic needs an audit before treating the resulting rows as trustworthy training examples. I believe I had abandoned the project while I was still working on this aspect of the project.
- **Coverage is broad but histories are thin.** Many players have very little recorded history. The majority of players in the dataset have one match corresponding to them. Participation also reflects travel resources, location, and availability, which complicates its interpretation as a skill proxy.
- **Outcome variability was a concern, not a demonstrated ceiling.** Upsets and unobserved influences motivated stopping the project, but these experiments do not isolate irreducible uncertainty from pipeline and evaluation errors.

## What this project demonstrates

The project documents an attempt to combine API extraction and web scraping, reconcile player identities across sources, and build historical features for prediction. Its main modeling conclusion is limited: the saved experiments do not establish reliable performance on unseen future tournaments.

A credible reevaluation would first repair joins and feature timing, calculate intervals correctly, split by event date, and fit feature selection using training data only. Comparing against simple baselines would then help determine whether the added features and models provide useful predictive value. These are proposed next steps, not completed improvements.