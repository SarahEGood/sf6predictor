import requests
import os
import pandas as pd
import numpy as np
from time import sleep, time
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

def safe_reassign(df, id, id_name, key, new_val):
    """
    Safely add new values from API requests if value exists.

    Parameters:
        df (dataframe): Dataframe to assign new value to.
        id (int): Id of relevant record.
        id_name (string): Name of id column.
        key (string): Name of column to add.
        new_val: Name of value to reassign.

    Returns:
        Dataframe if new value assigned if it exists. Else, return original
        Dataframe
    """
    if new_val:
        df.loc[df[id_name] == id, key] = new_val

    return df

def ensure_columns(df, new_data, fill_val = pd.NA):
    """
    Ensure that the DataFrame has all necessary columns from new data,
    filling missing columns with default values (e.g., NaN).
    
    Parameters:
        df (pd.DataFrame): The existing DataFrame.
        new_data (dict): Dictionary with new data that might contain new columns.

    Returns:
        pd.DataFrame: Updated DataFrame with all necessary columns.
    """
    # Extract columns from the new data that are not in the DataFrame
    new_cols = set(new_data.keys()) - set(df.columns)
    for col in new_cols:
        df[col] = fill_val  # Use pandas' NA for missing data by default which supports all data types
    return df 

def check_guest(value):
    """
    Checks if a value is 0 or null (for checking if a user id was pulled for a specific user)

    Args:
        value: Value to check if 0 or missing

    Returns:
        String "Yes" or "No" depending on if value is 0 or null (or not)
    """
    if pd.isna(value) or value == 0:
        return 'Yes'
    else:
        return 'No'

def retryStrategy():
    """
    Configures and returns a retry strategy for HTTP requests.

    This strategy is used to automatically retry requests that fail due to server-side error
    status codes such as 500 (Internal Server Error), 429 (Too Many Requests), and 503 (Service Unavailable).

    Returns:
        Retry: A configured urllib3 Retry object with specific rules for retrying HTTP requests.
    """
    retry_strategy = Retry(
    total=10,  # Maximum number of retry attempts
    status_forcelist=[500, 429, 503],  # Status codes to trigger a retry
    allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"],  # HTTP methods to retry
    backoff_factor=10  # Backoff factor to apply between attempts
    )
    return retry_strategy

def startgg_vars():
    """
    Retrieves API endpoint and access token for the start.gg API from environment variables.

    Returns:
        tuple: A tuple containing the API endpoint URL as a string and the API token as a string.
    """
    api_endpoint = "https://api.start.gg/gql/alpha"
    token = os.getenv('startgg_token')
    return api_endpoint, token

def eventsByVideogame(videogame_id = 43868, events_path='events.csv', integrateLiquid=True):
    """
    Fetches and processes a list of events by videogame from the start.gg GraphQL API.
    Retrieves events associated with a specific videogame (Street Fighter 6),organizes
    them into a pandas DataFrame, and writes the data to a CSV file.

    Returns:
        DataFrame: A DataFrame containing data about events, including event IDs, names, slugs,
        tournament names, IDs, start times, and competition tiers.
    """
    api_endpoint, token = startgg_vars()

    query = """
    query EventsByVideogame($perPage: Int!, $videogameId: ID!, $cursor: Int) {
        tournaments(query: {
            perPage: $perPage
            page: $cursor
            filter: {
                past: true
                videogameIds: [$videogameId]
            }
        }) {
            nodes {
                id
                name
                slug
                startAt
                city
                countryCode
                postalCode
                events {
                    id
                    name
                    slug
                    competitionTier
                    videogame {
                        id
                    }
                }
            }
        }
    }"""

    # Setup initial values and session variables
    cursor = 1
    headers = {'Authorization': 'Bearer ' + token}
    adapter = HTTPAdapter(max_retries=retryStrategy())
    session = requests.Session()

    # Allow useage of http and https
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # Intialize dataframe
    df = pd.DataFrame({
        'event_id': [],
        'event_name': [],
        'event_slug': [],
        'city': [],
        'country': [],
        'tournament_name': [],
        'tournament_id': [],
        'start_at': [],
        'competition_tier': []
    })

    # If the events file already exists, set newest_event_id to the earliest available event_id in current file
    # to prevent having to pull all events everytime.
    if os.path.isfile(events_path):
        old_df = pd.read_csv(events_path)
        newest_event_id = old_df[old_df['source'] == 'startgg']['event_id'].iloc[0]
        df = old_df
    else:
        newest_event_id = -1

    # Make continual queries until pagination runs out or the most recent event_id is pulled from a query
    while True:
        variables = {
            "perPage": 10,
            'cursor': cursor,
            "videogameId": videogame_id
        }

        # Attempt to parse tournament data from returned query
        try:
            response = session.post(api_endpoint, json={'query': query, 'variables': variables}, headers=headers)
            response.raise_for_status()
            print("Request was successful!")
            data = response.json()
            tournaments = data['data']['tournaments']['nodes']

            if not tournaments:  # If no tournaments, exit loop
                break

            result_list = [{
                'event_id': event['id'],
                'event_name': event['name'],
                'event_slug': event['slug'],
                'city': tournament['city'],
                'country': tournament['countryCode'],
                'postalCode': tournament['postalCode'],
                'tournament_name': tournament['name'],
                'tournament_id': tournament['id'],
                'start_at': tournament['startAt'],
                'competition_tier': event['competitionTier'],
                'source': 'startgg',
                'data_type': 'Brackets'
            } for tournament in tournaments for event in tournament['events'] if event['videogame']['id'] == videogame_id]

            # Concatenate resulting data to dataframe
            df_temp = pd.DataFrame(result_list)
            df = pd.concat([df, df_temp]).drop_duplicates()

            if len(tournaments) < 10 or newest_event_id in df['event_id'].unique():  # If fewer tournaments than perPage, assume it's the last page
                break
        # If the request fails, return error message
        except Exception as e:
            print(f"Request failed: {e}")
            break

        cursor += 1  # Increment the page number

    df['start_at'] = pd.to_datetime(df['start_at'], unit='s', utc=True, errors='ignore')

    if integrateLiquid == True:
        df = integrateLiquidpedia(df)

    df.to_csv(events_path, index=False)
    print(df.head())
    return df

def updateEvent(df, videogame_id = 43868):
    """
    Fetches and processes a list of events by videogame from the start.gg GraphQL API.
    If changes have occured to an event, add to table.

    Returns:
        DataFrame: A DataFrame containing data about events, including event IDs, names, slugs,
        tournament names, IDs, start times, and competition tiers.
    """
    api_endpoint, token = startgg_vars()

    query = """
    query EventsByVideogame($perPage: Int!, $videogameId: ID!, $cursor: Int) {
        tournaments(query: {
            perPage: $perPage
            page: $cursor
            filter: {
                past: true
                videogameIds: [$videogameId]
            }
        }) {
            nodes {
                id
                name
                slug
                startAt
                city
                countryCode
                postalCode
                events {
                    id
                    name
                    slug
                    competitionTier
                    videogame {
                        id
                    }
                }
            }
        }
    }"""

    # Setup initial values and session variables
    cursor = 1
    headers = {'Authorization': 'Bearer ' + token}
    adapter = HTTPAdapter(max_retries=retryStrategy())
    session = requests.Session()

    # Allow useage of http and https
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    df['event_id'] = df['event_id'].astype(int)

    # Make continual queries until pagination runs out
    while True:
        variables = {
            "perPage": 10,
            'cursor': cursor,
            "videogameId": videogame_id
        }

        response = session.post(api_endpoint, json={'query': query, 'variables': variables}, headers=headers)
        response.raise_for_status()
        print("Request was successful!")
        data = response.json()
        nodes = safe_get(data, ['data', 'tournaments', 'nodes'])
        print("TEST NODES")
        print(nodes)

        if nodes:
            for node in nodes:
                event_id = safe_get(node, ['events', 'id'])
                city = safe_get(node, ['city'])
                countryCode = safe_get(node, ['countryCode'])
                postalCode = safe_get(node, ['postalCode'])
                events = safe_get(node, ['events'])
                if events:
                    for event in events:
                        vid = safe_get(event, ['videogame', 'id'])
                        if vid == videogame_id:
                            event_id = safe_get(event, ['id'])
                            df = safe_reassign(df, event_id, 'event_id', 'city', city)
                            df = safe_reassign(df, event_id, 'event_id', 'country', countryCode)
                            df = safe_reassign(df, event_id, 'event_id', 'postalCode', postalCode)

            cursor += 1
        else:
            break

        if len(nodes) < 10:
            break

        sleep(0.7)

    return df

def getPhaseIds(event_id):
    """
    Fetches all phase id's from a given event for further processing

    Args:
    event_id (int): event_id via start.gg to query

    Returns:
    phase_ids (list): list of phase ids of an event
    """

    api_endpoint, token = startgg_vars()

    query = """
    query EventSets($eventId: ID!) {
            event(id: $eventId) {
                id
                name
                phases
                            {
                        id
                bracketType
                }
            }
        }
    """

    # Setup initial values and session variables
    headers = {'Authorization': 'Bearer ' + token}
    variables = {'eventId': int(event_id)}
    adapter = HTTPAdapter(max_retries=retryStrategy())
    session = requests.Session()

    # Allow useage of http and https
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    phase_ids = []

    try:
        response = session.post(api_endpoint, json={'query': query, 'variables': variables}, headers=headers)
        response.raise_for_status()
        print("Request was successful!")
        data = response.json()
        print(data)
        phases = data['data']['event']['phases']
        if phases:
            for phase in phases:
                phase_ids.append(phase['id'])
    except:
        pass

    print(phase_ids)
    return phase_ids

def getSetsByEvent(event_id):
    """
    Fetches sets data for a specific event from the start.gg API and organizes it into a DataFrame.

    Args:
        event_id (int): The unique identifier for the event to fetch sets from.

    Returns:
        DataFrame: A DataFrame containing set data including set IDs, entrant IDs, entrant names,
                   standings, user IDs, and associated event IDs.
    """

    # Initialize token and api endpoint for start.gg
    api_endpoint, token = startgg_vars()

    # Query string to send
    query = """
    query PhaseSets($phaseId: ID!, $cursor: Int!, $perPage: Int!) {
        phase(id: $phaseId) {
            id
            name
            sets(
                page: $cursor
                perPage: $perPage
                sortType: STANDARD
            ) {
                nodes {
                    id
                    slots {
                        id
                        entrant {
                            id
                            name
                            participants {
                                user {
                                    id
                                }
                                player {
                                    id
                                    gamerTag
                                    prefix
                                }
                            }
                        }
                        standing {
                            placement
                        }
                    }
                }
            }
        }
    }"""

    # Initialize variables for sending query for event
    headers = {'Authorization': 'Bearer ' + token}
    set_id, entrant_id, entrant_name, standing, user_id = [], [], [], [], []
    pids, gamertags, prefixes = [], [], []

    adapter = HTTPAdapter(max_retries=retryStrategy())
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    phase_ids = getPhaseIds(event_id)

    if phase_ids:
        for phase_id in phase_ids:
            cursor = 1
            has_next_page = True

            # Loop through all available pages for given event
            while has_next_page:

                # Set items per query and variables
                perPage = 10

                variables = {
                    "phaseId": int(phase_id),
                    "cursor": cursor,
                    "perPage": perPage
                }

                # Attempt to fetch response
                try:
                    response = session.post(api_endpoint, json={'query': query, 'variables': variables}, headers=headers)
                    response.raise_for_status()
                except:
                    break
                else:
                    print("Request was successful!")

                response_dict = response.json()
                print(response_dict)

                # Attempt to parse data from response
                try:
                    sets_data = response_dict['data']['phase']['sets']
                    if sets_data:
                        nodes = sets_data['nodes']
                        for set in nodes:
                            for slot in set['slots']:
                                print(slot)
                                try:
                                    info_list = [set['id']]
                                    entrant = slot['entrant']
                                    try:
                                        uid = entrant['participants'][0]['user']['id']
                                        if not uid:
                                            uid = 0
                                    except:
                                        uid = 0
                                    try:
                                        pid = entrant['participants'][0]['player']['id']
                                        gamerTag = entrant['participants'][0]['player']['gamerTag']
                                        p_prefix = entrant['participants'][0]['player']['prefix']
                                        if not pid:
                                            pid = 0
                                            gamerTag = ''
                                            p_prefix = ''
                                    except:
                                        pid = 0
                                        gamerTag = ''
                                        p_prefix = ''
                                    
                                    info_list.extend([entrant['id'], entrant['name'], slot['standing']['placement'], uid, pid, gamerTag, p_prefix])

                                    set_id.append(info_list[0])
                                    entrant_id.append(info_list[1])
                                    entrant_name.append(info_list[2])
                                    standing.append(info_list[3])
                                    user_id.append(info_list[4] if info_list[4] else None)
                                    pids.append(info_list[5])
                                    gamertags.append(info_list[6])
                                    prefixes.append(info_list[7])

                                except TypeError:
                                    continue
                        # Assume there are no pages left if most recent result returns entries less than perPage value (default 10)
                        if len(nodes) < perPage:
                            has_next_page = False
                    else:
                        has_next_page = False
                
                # Assume there are no pages left if result returns no matcing info in request
                except KeyError:
                    has_next_page = False

                cursor += 1

                # Mak next api call wait to adhere to rate limits
                sleep(0.5)

    # Build dataframe from resulting data and return
    df = pd.DataFrame({
        'set_id': set_id,
        'entrant_id': entrant_id,
        'entrant_name': entrant_name,
        'standing': standing,
        'user_id': user_id,
        'player_id': pids,
        'gamerTag': gamertags,
        'player_prefix': prefixes,
        'event_id': [event_id] * len(set_id),
        'source': ['startgg'] * len(set_id)
    })

    df = df.drop_duplicates()
    df['set_id'] = pd.to_numeric(df['set_id'], errors='ignore')
    print(df.head(5))
    return df

def getAllSets(event_list, sets_path = 'all_sets.csv'):
    """
    Fetches and combines sets data for multiple events into a single DataFrame.

    Args:
        event_list (list): A list of event IDs for which to fetch set data.

    Returns:
        DataFrame: A combined DataFrame containing all sets data from the listed events.
    """

    # Initialize dataframe for all sets
    main = pd.DataFrame({
        'set_id': [],
        'entrant_id': [],
        'entrant_name': [],
        'standing': [],
        'user_id': [],
        'event_id': []
    })

    # Set incrementer value for returning dataframe
    # Note: Function will periodically save new copy of dataframe for every 20 event_ids
    i = 0

    if os.path.isfile(sets_path):
        main = pd.read_csv(sets_path)
        newest_event_id = main[main['source'] == 'startgg']['event_id'].iloc[-1]
    else:
        newest_event_id = -1

    # Iterate through event_id's
    for id in event_list:
        df = getSetsByEvent(id)
        main = pd.concat([main, df])

        main = main.astype({'set_id': 'object',
                            'entrant_id': 'int32',
                            'standing': 'int32',
                            'event_id': 'int32',
                            'user_id': 'int32'},
                            errors='ignore')
        
        # Save over all_sets.csv every 20 event_ids (in case of network issues)
        if i % 20 == 0:
            main.to_csv(sets_path, index=False)
        if newest_event_id in main['event_id'].unique():
            break

        i += 1

    # Set user_id to 0 if None
    main.loc[main['user_id'].isnull(),'user_id'] = -1

    # Export to csv and return as dataframe to variable
    main.to_csv(sets_path, index=False)

    return main

def getPlayersFromSets(sets_df):
    """
    Extracts player information from a DataFrame containing sets data.

    Args:
        sets_df (DataFrame): A DataFrame containing sets data with user IDs and entrant names.

    Returns:
        DataFrame: A DataFrame with columns for user IDs and entrant names, without duplicates.
    """

    df = sets_df[['user_id', 'player_id', 'entrant_name', 'player_prefix', 'gamerTag', 'event_id']]
    df = df.drop_duplicates(subset=['user_id', 'player_id'])

    return df

def getEventSort(events_path = 'events.csv'):
    """
    Loads an events DataFrame from a CSV file, sorts it by start time, and returns the sorted DataFrame.

    Returns:
        DataFrame: A DataFrame containing sorted event data by their starting time in ascending order.
    """

    # Load tournament and set data
    event_list = pd.read_csv(events_path)

    # Get list of events sorted (ascending) by date
    event_sort_cols = ['event_id', 'start_at']
    event_list = event_list[event_sort_cols].sort_values('start_at', ascending=True)
    
    return event_list

def sortBySetId(sets_path='all_sets.csv'):
    """
    Loads a sets DataFrame from a CSV file, sorts it by set ID, and returns the sorted DataFrame.

    Returns:
        DataFrame: A DataFrame containing set data sorted by set IDs.
    """

    # Load set data
    sets = pd.read_csv(sets_path)

    # Sort by set_id
    sets = sets.sort_values('set_id')

    return sets

def integrateLiquidpedia(df):
    """
    Concatenates Liquidpedia data ("scrape_brackets.csv") to main events data.

    Args:
        df (DataFrame): Events DataFrame containing events data.

    Returns:
        DataFrame: Contains all event data. 
    """

    if 'data_type' not in df.columns:
        df['data_type'] = 'Brackets'

    df['data_type'][df['source'] == 'startgg'] = 'Brackets'
    df['state'] = pd.NaT
    df = df.reset_index(drop=True)

    df2 = pd.read_csv('scrape_brackets.csv')[['event_id','event_name','comptier','date','func_type', 'country', 'city', 'state']]

    df2 = df2.rename(columns={'date': 'start_at',
                             'event_name': 'event_slug',
                             'comptier':'competition_tier'})
    
    df2['source'] = 'Liquidpedia'
    df['start_at'] = pd.to_datetime(df['start_at'], errors='coerce')
    df2['start_at'] = pd.to_datetime(df2['start_at'], errors='coerce')
    df2['data_type'] = 'Brackets'
    df2.loc[df2['func_type'] != 3, 'data_type'] = 'Brackets'
    df2.loc[df2['func_type'] == 3, 'data_type'] = 'Pools'

    df2.drop(columns=['func_type'])

    df2 = pd.concat([df, df2], axis=0, ignore_index=True).sort_values(['start_at', 'competition_tier', 'country'], ascending=[False, True, True],
                                                                      na_position='last')
    df2.drop_duplicates(['event_id'], keep='first')

    return df2

# SF6: 43868
# SFV: 10055

def fetchAllSetsWrapper(videogame_id, events_path='events.csv', sets_path='all_sets.csv', players_path='players.csv',
                        integrateLiquid=False):
    """
    Function to pull all players, events, and set data available from start.gg for a given video game ID.

    Args:
    videogame_id: Id of given videogame from start.gg database
    events_path: Destination path for events csv file (default: 'events.csv')
    sets_path: Destination path for sets csv file (default: 'all_sets.csv')
    players_path: Destination path for players csv file (default: 'players.csv')
    integrateLiquid: If True, will integrate Liquidpedia brackets data into events data (default: False)
        Note: This needs to be refactored to allow selection of different data. 
    """

    # For logging runtime
    start_time = time()

    # Get events by videogame id
    df = eventsByVideogame(videogame_id, events_path, integrateLiquid=integrateLiquid)
    events = df[df['source'] == 'startgg']['event_id']

    # Get all sets by list of event_ids
    main = getAllSets(events, sets_path = sets_path)
    main.to_csv(sets_path, index=False)
    main = main[pd.to_numeric(main['set_id'], errors='coerce').notnull()]

    # Get player data from set table
    players = getPlayersFromSets(main)
    players.to_csv(players_path, index=False)

    # Update events and integrate Liquipedia data if true
    export_me = updateEvent(df, videogame_id = videogame_id)

    if integrateLiquid == True:
        export_me = integrateLiquidpedia(df)
    export_me.to_csv('data\\events.csv', index=False)

    # Export runtime
    hours = round((time() - start_time)/60, 2)
    print("All set total runtime: {} hours".format(hours))

if __name__ == '__main__':
    # Init dirs if not exist
    os.makedirs(os.path.dirname('SF6\\events.csv', exist_ok=True))
    os.makedirs(os.path.dirname('SFV\\events.csv', exist_ok=True))
    os.makedirs(os.path.dirname('data\\events.csv', exist_ok=True))

    # Fetch relevant sets
    fetchAllSetsWrapper(43868, 'SF6\\events.csv', 'SF6\\all_sets.csv', 'SF6\\players.csv')
    fetchAllSetsWrapper(10055, 'SFV\\events.csv', 'SFV\\all_sets.csv', 'SFV\\players.csv')