import pandas as pd
import os
from time import sleep, time
from datetime import datetime
from extract_startgg_data import startgg_vars, retryStrategy
import requests
from requests.adapters import HTTPAdapter
from rapidfuzz import process, fuzz

def safe_get(d, keys, default=None):
    """
    Safely navigate through nested dictionaries.

    Parameters:
        d (dict): The dictionary to search.
        keys (list): A list of keys to navigate through the dictionary.
        default (any): The default value to return if the keys are not found.

    Returns:
        The value from the dictionary located at the path specified by keys or
        the default value if the path is not found.
    """

    assert isinstance(keys, list), "keys must be provided as a list"
    current = d
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default
    return current

def fetchPlayerbyId(player_id):
    """
    Fetches player data from the start.gg API using GraphQL.

    Parameters:
        player_id (int): The unique identifier for the player.

    Returns:
        dict: The player data if successful, None otherwise.
    """
    api_endpoint, token = startgg_vars()

    # GraphQL query to fetch user data based on player ID
    query = """
        query UserData($playerId: ID!) {
        player(id: $playerId) {
            id
            gamerTag
            prefix
            user {
            id
            name
            location {
                country
                state
            }
            authorizations {
                id
                externalId
                externalUsername
                type
            }
            }
        }
        }
    """

    # Setup initial values and session variables
    headers = {'Authorization': 'Bearer ' + token}
    variables = {'playerId': int(player_id)}
    adapter = HTTPAdapter(max_retries=retryStrategy())
    session = requests.Session()

    # Allow useage of http and https
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    try:
        response = session.post(api_endpoint, json={'query': query, 'variables': variables}, headers=headers)
        response.raise_for_status()
        print("Request was successful!")
        data = response.json()
        return data['data']['player']
    except:
        return None

def processPlayerData(player_id, data):
    """
    Processes and extracts relevant fields from raw player data.

    Parameters:
        player_id (int): The player's unique ID.
        data (dict): The raw data dictionary returned from the API.

    Returns:
        dict: A dictionary containing the processed and formatted player data.
    """

    # Extract basic information and social media identifiers
    gamerTag = safe_get(data, ['gamerTag'])
    prefix = safe_get(data, ['prefix'])
    user_id = safe_get(data, ['user', 'id'])
    full_name = safe_get(data, ['user', 'name'])
    country = safe_get(data, ['user', 'location', 'country'])
    state = safe_get(data, ['user', 'location', 'state'])

    # Initialize social media information
    twitter_handle = None
    discord_id = None
    discord_username = None
    xbox_username = None
    mixer_username = None
    twitch_id = None
    twitch_name = None
    
    socials_raw = safe_get(data, ['user', 'authorizations'])
    if socials_raw:
        for auth in socials_raw:
            ext_id = safe_get(auth, ['externalId'])
            externalUsername = safe_get(auth, ['externalUsername'])
            service = safe_get(auth, ['type'])

            # Map social media data to respective fields based on service type
            if service.lower() == 'discord':
                discord_id = ext_id
                discord_username = externalUsername
            elif service.lower() == 'twitter':
                twitter_handle = externalUsername
            elif service.lower() == 'twitch':
                twitch_name = externalUsername
                twitch_id = ext_id
            elif service.lower() == 'xbox':
                xbox_username = externalUsername
            elif service.lower() == 'mixer':
                mixer_username = externalUsername

    # Compile all extracted data into a dictionary for further use in dataframe
    data_dict = {
            'player_name': gamerTag,
            'full_name': full_name,
            'prefix': prefix,
            'startgg_pid': int(player_id),
            'startgg_uid': user_id,
            'country': country,
            'state': state,
            'twitter_id': twitter_handle,
            'twitch_id': twitch_id,
            'twitch_name': twitch_name,
            'discord_id': discord_id,
            'discord_name': discord_username,
            'mixer_id': mixer_username,
            'xbox_id': xbox_username,
            'liquidpedia_name': ''
    }

    return data_dict

def integrateStartGGPlayers(og_data_path='players.csv', reset_uid_ind = False):
    """
    Integrates player data from a CSV file using data fetched from the start.gg API.

    Parameters:
        og_data_path (str): The original path to the CSV file containing player IDs.

    Returns:
        pd.DataFrame: A DataFrame containing the integrated player data.
    """
    start_time = time()

    # Check for an existing data file and create or update accordingly
    if os.path.isfile('data\\players.csv'):
        df = pd.read_csv('data\\players.csv')
        df['startgg_pid'] = df['startgg_pid']
        if reset_uid_ind:
            df['uid'] = range(0, len(df))
    else:
        df = pd.DataFrame({
            'uid': [],
            'player_name': [],
            'full_name': [],
            'prefix': [],
            'date_added': [],
            'country': [],
            'state': [],
            'startgg_pid': [],
            'startgg_uid': [],
            'liquidpedia_name': [],
            'twitter_id': [],
            'twitch_id': [],
            'twitch_name': [],
            'discord_id': [],
            'discord_name': [],
            'mixer_id': [],
            'xbox_id': []
        })

    startgg = pd.read_csv(og_data_path)

    # Process each player in the CSV file
    for pid in startgg['player_id'].unique():
        # Only add if a new player id is encountered
        if df.loc[df['startgg_pid'] == pid,:].empty:
            data = fetchPlayerbyId(pid)

            if data:
                new_data = processPlayerData(pid, data)
                datetime_now = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
                uid_ind = df.loc[:, 'uid'].max() + 1
                new_data.update({'uid': int(uid_ind), 'date_added': datetime_now})
                print(new_data)
                new_row = pd.DataFrame([new_data])
                df = pd.concat([df, new_row], axis=0, ignore_index=True)
                # Save periodically after processing every 20 players
                if uid_ind % 20 == 0:
                    df.to_csv('data\\new_players.csv', index=False)
                sleep(0.5) # Sleep to prevent rate limiting

    df.to_csv('data\\players.csv', index=False)

    hours = round((time() - start_time)/60, 2)
    print("Player data total runtime: {} hours".format(hours))

    return df

def return_current_datetime():
    return datetime.now().strftime("%m/%d/%Y, %H:%M:%S")

def extract_unique_values(df, id_col, value_col):
    # Function to apply to each group
    def get_unique_values(group):
        unique_vals = sorted(group[value_col].unique())
        first_val = unique_vals[0]  # Get the first unique value
        other_vals = unique_vals[1:]  # Get the rest of the unique values
        vals_list = {
            'new_id': [first_val]* len(other_vals),
            'old_id': other_vals
        }
        return pd.DataFrame(vals_list)
    
    # Group by id_col and apply the function
    result = df.groupby(id_col).apply(get_unique_values).reset_index()
    
    return result

def concat_id_matching(df, file_path='data\\ids.csv'):
    df = df[['new_id', 'old_id']]
    old_df = pd.read_csv(file_path)
    df = pd.concat([old_df, df], axis=0, ignore_index=True).drop_duplicates(ignore_index=True)

    return df

def resolve_duplicates_strict(df, id_cols, newer_col):
    id_matches = {
        'new_id': [],
        'old_id': []
    }

    for id_col in id_cols:
        df = df.sort_values(by=[id_col, newer_col])
        new_id_matches = extract_unique_values(df, id_col, newer_col)
        id_matches['new_id'].extend(new_id_matches['new_id'])
        id_matches['old_id'].extend(new_id_matches['old_id'])
    
    id_df = pd.DataFrame(id_matches)
    
    return id_df


def filter_by_list_content(df, column_name, value):
    # Filter rows where 'value' is in the list of 'column_name'
    mask = df[column_name].apply(lambda x: value in str(x))
    return df[mask]

# Function to perform fuzzy matching and maintain associated data
def fuzzy_match(row, master_list):
    # Perform the fuzzy matching
    master_list = list(master_list)
    master_list_match = [x.lower() for x in master_list]
    cleaned_names = [str(x).lower().replace(' ', '') for x in str(row['clean_name']).split('|')]
    best_match = []
    for clean_name in cleaned_names:
        match = process.extractOne(clean_name, master_list_match, scorer=fuzz.WRatio, score_cutoff=97)
        if match and best_match:
            if match[1] > best_match[1]:
                best_match = match
        elif match:
            best_match = match
    # Return match details along with event_id and entrant_name
    if best_match:
        matched_index = best_match[2]  # Get the index of the matched entry
        matched_data = master_list[matched_index]
        
        # Return match details along with event_id, entrant_name, and user_id from both sides
        return pd.Series([row['uid'], best_match[0], best_match[1], row['player_name'],
                          matched_data],
                         index=['uid', 'matched_name', 'score', 'player_name',
                                'liquidpedia_name'])
    else:
        # Return NaNs or some form of indication for no match found
        return pd.Series([None, None, None, row['player_name'], None],
                         index=['uid', 'matched_name', 'score', 'player_name',
                                'liquidpedia_name'])
    
def batch_fuzzy_match(df, player_list, batch_size=10000, test=True):
    results = []
    for start in range(0, df.shape[0], batch_size):
        end = min(start + batch_size, df.shape[0])
        df_batch = df.iloc[start:end]
        batch_results = df_batch.apply(fuzzy_match, axis=1, master_list=player_list)
        results.append(batch_results)

    return pd.concat(results).dropna(how='any')

def merge_other_players(df, player_list, test=True):
    unique_players = set(player_list)
    cleaned_players = unique_players

    match_me = df[['uid', 'player_name']]
    match_me['clean_name'] = df['player_name'].str.lower().str.replace(' ', '', regex=True)

    return batch_fuzzy_match(match_me, cleaned_players)

def update_matched_values(df, matches, label, id_label):
    for ind, row in matches.iterrows():
        df.loc[df[id_label] == row[id_label], label] = row[label]
    return df

def insert_new(df, player_list, label, id_label):
    player_list_unique = set(player_list)
    df['match_row'] = df[label].str.lower().str.replace(' ', '', regex=True).str.split("|")
    for player in player_list_unique:
        player_lower = player.lower().replace(' ', '')
        select = filter_by_list_content(df, 'match_row', player_lower)
        if select.empty:
            new_row = {
                id_label: df.loc[:, id_label].max() + 1,
                label: player
            }

            df = df._append(new_row, ignore_index=True)
    
    df = df.drop(columns=['match_row'])

    return df

def concat_sf_data(sf6_file, sf5_file, event=False):
    """
    Function to merge SF6 and SFV data for all_sets.csv or events.csv files.

    Args:
    sf6_file: First filepath to csv
    sf5_file: Second filepath to csv
    event: Set to True if merging events.csv data (default: False) 

    Returns:
    df1: Concatenated dataframe of both files' data
    """
    df1 = pd.read_csv(sf6_file)
    df2 = pd.read_csv(sf5_file)

    if event:
        df1['game'] = 'SF6'
        df2['game'] = 'SFV'

    df1 = pd.concat([df1, df2], ignore_index=True)

    if event:
        df1 = df1.sort_values(['start_at', 'event_id'], ascending=[True, True])

    return df1

if __name__ == '__main__':
    df = integrateStartGGPlayers(og_data_path='SFV\\all_sets.csv', reset_uid_ind=True)

    sets = concat_sf_data('SF6\\all_sets.csv', 'SFV\\all_sets.csv', events=False)
    events = concat_sf_data('SF6\\events.csv', 'SFV\\events.csv', events=True)

    sets.to_csv('data\\all_sets.csv', index=False)
    events.to_csv('data\\events.csv', index=False)

    match_cols = ['discord_id', 'twitch_id', 'twitter_id']

    df2, df_ids = resolve_duplicates_strict(df, match_cols, 'uid')
    df_ids = concat_id_matching(df_ids)
    df = df.sort_values(['uid'])

    df.to_csv('data\\players.csv', index=False)
    df_ids.drop_duplicates().to_csv('data\\ids.csv', index=False)