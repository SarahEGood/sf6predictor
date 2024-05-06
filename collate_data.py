import pandas as pd
import numpy as np
import os
from time import sleep, time
from datetime import datetime
from extract_startgg_data import startgg_vars, retryStrategy
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
        df['startgg_pid'] = df['startgg_pid'].astype(int)
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
        if df.loc[df['startgg_pid'].astype(int) == pid,:].empty:
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

if __name__ == '__main__':
    #df = integrateStartGGPlayers(og_data_path='SF6\\all_sets.csv', reset_uid_ind=True)
    df = pd.read_csv('data\\new_players.csv')

    match_cols = ['discord_id', 'twitch_id', 'twitter_id']

    df2, df_ids = resolve_duplicates_strict(df, match_cols, 'uid')
    df_ids = concat_id_matching(df_ids)
    df = df.sort_values(['uid'])

    df.to_csv('data\\players.csv', index=False)
    df_ids.drop_duplicates().to_csv('data\\ids.csv', index=False)