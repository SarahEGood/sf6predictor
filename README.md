# Street Fighter 6 Bracket Predictor

An exploratory Python project investigating whether historical tournament results and participation records can predict the winner of a Street Fighter 6 set.

The project combines start.gg API extraction, Liquipedia scraping, player identity matching, Elo calculations, and experiments with Lasso regression, Tweedie regression, and a multilayer perceptron classifier.

## Project status

Development was discontinued. The repository preserves exploratory work and requires repairs before the full pipeline can be run reliably. It should not be treated as a finished bracket simulator or a validated forecasting tool.

The saved evaluations use random train/test splits and contain feature-timing, preprocessing, and evaluation problems. Their scores do not establish accuracy on future tournaments.

[Read the project case study](case_study.md)

## How it works

| Component | Purpose |
| --- | --- |
| `extract_startgg_data.py` | Retrieves tournament, set, and player records from start.gg using its GraphQL API. The script targets both SF6 and Street Fighter V. |
| `collate_data.py` | Combines start.gg records and reconciles player identities using additional player information. |
| `scrape_liquidpedia.py` | Scrapes supported Liquipedia bracket and pool formats and contains functions for integrating scraped records. |
| `fuzzy_match_liquidpedia.ipynb` | Contains the cross-source player-name matching workflow. |
| `calc_elo.py` | Calculates event-level Elo records and tournament participation statistics from collected records. |
| `model_training.ipynb` | Prepares paired-player features and evaluates Lasso, Tweedie, and MLP experiments on a binary set-outcome target. |

Features include Elo, participation counts by tournament tier, total participation, and an attempted days-between-events measure. The interval calculation needs correction.

Although SFV data is collected, the modeling notebook filters event metadata to SF6. The extent to which SFV results influenced upstream Elo records still needs confirmation.

## Local setup

The original README specifies Python 3.11. The commands below prepare an environment for inspecting and repairing the project; they are not a verified end-to-end reproduction procedure.

### 1. Clone the repository

```bash
git clone https://github.com/SarahEGood/sf6predictor.git
cd sf6predictor
python -m venv sf6-predictor-env
```

Activate the environment using the command for your shell.

**macOS or Linux:**

```bash
source sf6-predictor-env/bin/activate
```

**Windows Command Prompt:**

```bat
sf6-predictor-env\Scripts\activate.bat
```

**Windows PowerShell:**

```powershell
.\sf6-predictor-env\Scripts\Activate.ps1
```

### 2. Install dependencies

```bash
python -m pip install -r requirements.txt
python -m pip install beautifulsoup4 notebook
```

The current requirements file pins NumPy, pandas, RapidFuzz, Requests, scikit-learn, and urllib3. It does not declare BeautifulSoup, which the scraper imports, or Jupyter Notebook, which is needed for the notebook interface shown here. Installing these additional packages does not resolve the pipeline issues described below.

### 3. Configure start.gg authentication

Obtain a token using the [start.gg authentication documentation](https://developer.start.gg/docs/authentication), then set the `startgg_token` environment variable in the shell where you will run the extraction code.

**macOS or Linux:**

```bash
export startgg_token='YOUR_TOKEN_HERE'
```

**Windows Command Prompt:**

```bat
set "startgg_token=YOUR_TOKEN_HERE"
```

**Windows PowerShell:**

```powershell
$env:startgg_token = 'YOUR_TOKEN_HERE'
```

## Liquipedia inputs

The scraper uses a manually prepared `scrape_brackets.csv` containing tournament URLs and metadata. It has separate handlers for brackets, groups with match details, and aggregate pool results; compatibility depends on the page format.

Include these columns:

| Column | Description |
| --- | --- |
| `event_id` | Locally assigned event identifier. Reuse it for pages belonging to the same event, and check downstream ID assumptions before assigning values. |
| `url` | Liquipedia page URL. |
| `event_name` | Tournament or event name. |
| `comptier` | Competition tier used by the project's event metadata. |
| `func_type` | `1` for the bracket handler, `2` for the groups handler, or `3` for the aggregate pools handler. |
| `date` | Event date in an unambiguous format such as `YYYY-MM-DD`. Repeated pages for an event should use consistent metadata. |
| `country` | Country; retain the column even if values are blank. |
| `city` | City; retain the column even if values are blank. |
| `state` | State or province; retain the column even if values are blank. |

Liquipedia processing also relies on intermediate files and player mappings. Creating this CSV alone does not make the integration ready to run. Aggregate pool results are not equivalent to observed head-to-head set records.

## Running and reproducing the project

The intended workflow is:

1. Collect start.gg records.
2. Collate event, set, and player data.
3. Prepare player mappings and integrate Liquipedia records where applicable.
4. Calculate Elo and participation features.
5. Open `model_training.ipynb` for model preparation and evaluation.

The current repository does not provide a reliable one-command or sequential-script reproduction path. Before attempting the complete workflow:

- Repair the extraction script's directory creation: `exist_ok=True` is passed to `os.path.dirname` instead of `os.makedirs`.
- Reconcile Windows-style paths and inconsistent intermediate-file locations. The collation and modeling code use files under `data`, while parts of the scraper expect files in the project root and an `sf6folder` directory.
- Prepare the required input CSVs and matching files. The Liquipedia integration is a separate preparation workflow, not a fully automated optional step.
- Review identifier handling. The modeling notebook contains a hard-coded event-ID split at 60, so the old instruction to use any Liquipedia event ID below 1000 is insufficient.
- Correct feature timing, player joins, event intervals, and evaluation logic before interpreting model scores. Feature selection must be fitted on training data, and forecasting evaluation should hold out later events.

With the notebook's required input files available, open it using:

```bash
jupyter notebook model_training.ipynb
```

Saved notebook outputs document earlier experiments; they are not evidence that the current pipeline reproduces those results unchanged. See the case study for the results and methodological limitations.

## License

This project is available under the [MIT License](LICENSE).

