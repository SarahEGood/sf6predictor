# Street Fighter 6 Bracket Predictor

This project aims to predict tournament results for Street Fighter 6 using historical match data from Street Fighter V and 6.

## How it works

* **Data Extraction:** Extract player, event, and match result data for both Street Fighter V and VI from start.gg within API limits using `extract_startgg_data.py`.
* **Data Scraping:** Scrape additional data from Liquipedia using a manually compiled CSV file containing tournament names and URLs with `scrape_liquidpedia.py`.
* **Data Collation:** Extract more detailed player data from start.gg and merge all extracted and scraped data into consolidated tables using `collate_data.py`.
* **ELO Calculation:** Compute ELO ratings and tournament participation statistics for every player for each event they participated in using `calc_elo.py`.
* **Model Training:** Train a machine learning model to predict match outcomes based on the data gathered and processed in previous steps using `model_training.ipynb`.

## Install
### Prerequisites
Before installation, ensure you have Python 3.11 installed on your system. You can download Python [here](https://www.python.org/downloads/).

### Steps
1. **Clone the Repositiory:** Clone this repositiory to your local machine using:

```bash
git clone https://github.com/SarahEGood/sf6predictor
```

2. **Environment Setup:** Set up a virtual environment:
```bash
python -m venv sf6-predictor-env
source sf6-predictor-env/bin/activate # On Windows use `sf6-predictor-env\Scripts\activate`
```

3. **Install Dependencies:** Install the required Python libraries:
```bash
pip install -r requirements.txt
```
4. **API Token:** Follow the instructions (here)[https://developer.start.gg/docs/authentication] to obtain a `start.gg` authentication token. Set this token as an environment variable:
```bash
export startgg_token='YOUR_TOKEN_HERE'  # On Windows use `set startgg_token=YOUR_TOKEN_HERE`
```

5. **Setup Liquipedia Webscraping (Optional):** If you wish to also use Liquipedia tournaments, you will need to manually create a file called `scrape_brackets.csv`. Please note that the current scraping code supports individual tournaments only; group events are not supported. Ensure that the file includes the following columns:
* event_id: An integer less than 1000, unique for each event. Reuse this ID if an event has multiple pages for scraping.
* url: The URL to the page that needs to be scraped.
* event_name: The name of tournament.
* comptier: The competitive tier assigned by Liquipedia for the tournament.
* func_type: An integer that determines which function to use.
    * If only brackets are displayed, set `func_type` to 1.
    * If pools are displayed with expandable match data, set `func_type` to 2.
    * If aggregate pools data is displayes with no specific match data (i.e. you know who won the pool but not specifically who they matched against), use `func_type` 3.
* date: The date of tournament's last day.
* country: The country of tournament (can be blank)
* city: The city of tournament (can be blank)
* state: The state or province of tournament (can be blank)

This is an example setup for this csv:

|event_id|url                                                          |event_name        |comptier|func_type|date       |country|city  |state|
|--------|-------------------------------------------------------------|------------------|--------|---------|-----------|-------|------|-----|
|0       | https://liquipedia.net/fighters/Abang_Bald_Cup/5/Group_Stage| Abang Bald Cup #5|2       |1        | 2024-28-02|       |      |     |
|0       | https://liquipedia.net/fighters/Abang_Bald_Cup/5/Bracket    | Abang Bald Cup #5|2       |1        | 2024-28-02|       |      |     |
|1       | https://liquipedia.net/fighters/Keio_Cup/2024/Bracket       | Keio Cup         |2       |1        | 2024-28-01|JP     |Tokyo |     |
|2       | https://liquipedia.net/fighters/Gamers8/2023/SF6/Group_Stage| Gamers 8 2023    |1       |2        | 2023-13-09|SA     |Riyadh|     |
|2       | https://liquipedia.net/fighters/Gamers8/2023/SF6/Playoffs   | Gamers 8 2023    |1       |1        | 2023-13-09|SA     |Riyadh|     |

## Usage
To run the full sequence from data extraction to predictions:
```bash
python extract_startgg_data.py
python collate_data.py
python scrape_liquidpedia.py # Only run if brackets data is setup
python calc_elo.py
jupyter notebook model_training.ipynb
```

## License
The Street Figther 6 Bracket Predictor is available under the MIT license. See the LICENSE file for more info.