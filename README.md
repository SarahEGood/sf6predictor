# Street Fighter 6 Bracket Predictor

This project aims to predict tournament results for Street Fighter 6 using historical match data from Street Fighter V and 6.

## How it works

* **Data Extraction:** Extract player, event, and match result data for both Street Fighter V and VI from start.gg within API limits using `extract_startgg_data.py`.
* **Data Scraping:** Scrape additional data from Liquipedia using a manually compiled CSV file containing tournament names and URLs with `scrape_liquidpedia.py`.
* **Data Collation:** Merge all extracted and scraped data into consolidated tables using `collate_data.py`.
* **ELO Calculation:** Compute ELO ratings and tournament participation statistics for every player for each event they participated in using `calc_elo.py`.
* **Model Training:** Train a machine learning model to predict match outcomes based on the data gathered and processed in previous steps using `model_training.ipynb`.

## Install
### Prerequisites
Before installation, ensure you have Python 3.11 installed on your system. You can download Python [here](https://www.python.org/downloads/).

### Steps
1. **Clone the Repositiory:** Clone this repositiory to your local machine using:

    git clone https://github.com/SarahEGood/sf6predictor

2. **Environment Setup:** Set up a virtual environment:
    python -m venv sf6-predictor-env
    source sf6-predictor-env/bin/activate # On Windows use `sf6-predictor-env\Scripts\activate`

3. **Install Dependencies:** Install the required Python libraries:
    pip install -r requirements.txt

4. **API Token:** Follow the instructions (here)[https://developer.start.gg/docs/authentication] to obtain a `start.gg` authentication token. Set this token as an environment variable:
    export startgg_token='YOUR_TOKEN_HERE'  # On Windows use `set startgg_token=YOUR_TOKEN_HERE`


## Usage
To run the full sequence from data extraction to predictions:
    python extract_startgg_data.py
    python scrape_liquidpedia.py
    python collate_data.py
    python calc_elo.py
    jupyter notebook model_training.ipynb

## License
The Street Figther 6 Bracket Predictor is available under the MIT license. See the LICENSE file for more info.