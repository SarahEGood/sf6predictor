import requests
import os
import pandas as pd
import numpy as np
from time import sleep
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def check_guest(value):
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

def eventsByVideogame():
    """
    Fetches and processes a list of events by videogame from the start.gg GraphQL API.
    Retrieves events associated with a specific videogame, organizes them into a pandas DataFrame,
    and writes the data to a CSV file.

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

    cursor = 1
    videogame_id = 43868  # Street Fighter 6 game id in start.gg database
    headers = {'Authorization': 'Bearer ' + token}
    adapter = HTTPAdapter(max_retries=retryStrategy())
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # Init dataframe
    df = pd.DataFrame({
        'event_id': [],
        'event_name': [],
        'event_slug': [],
        'tournament_name': [],
        'tournament_id': [],
        'start_at': [],
        'competition_tier': []
    })

    # If the events file already exists, set newest_event_id to the earliest available event_id in current file
    # to prevent having to pull all events everytime.
    if os.path.isfile('events.csv'):
        old_df = pd.read_csv('events.csv')
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
                'tournament_name': tournament['name'],
                'tournament_id': tournament['id'],
                'start_at': tournament['startAt'],
                'competition_tier': event['competitionTier'],
                'source': 'startgg',
                'data_type': 'Brackets'
            } for tournament in tournaments for event in tournament['events'] if event['videogame']['id'] == videogame_id]

            df_temp = pd.DataFrame(result_list)
            df = pd.concat([df, df_temp]).drop_duplicates()

            if len(tournaments) < 10 or newest_event_id in df['event_id'].unique():  # If fewer tournaments than perPage, assume it's the last page
                break
        except Exception as e:
            print(f"Request failed: {e}")
            break

        cursor += 1  # Increment the page number

    df['start_at'] = pd.to_datetime(df['start_at'], unit='s', utc=True, errors='ignore')

    df = integrateLiquidpedia(df)

    df.to_csv('events.csv', index=False)
    print(df.head())
    return df

def getSetsByEvent(event_id):
    """
    Fetches sets data for a specific event from the start.gg API and organizes it into a DataFrame.

    Args:
        event_id (int): The unique identifier for the event to fetch sets from.

    Returns:
        DataFrame: A DataFrame containing set data including set IDs, entrant IDs, entrant names,
                   standings, user IDs, and associated event IDs.
    """
    api_endpoint, token = startgg_vars()

    query = """
    query EventSets($eventId: ID!, $cursor: Int!, $perPage: Int!) {
        event(id: $eventId) {
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

    headers = {'Authorization': 'Bearer ' + token}
    cursor = 1
    has_next_page = True
    set_id, entrant_id, entrant_name, standing, user_id = [], [], [], [], []

    adapter = HTTPAdapter(max_retries=retryStrategy())
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    while has_next_page:
        perPage = 10

        variables = {
            "eventId": int(event_id),
            "cursor": cursor,
            "perPage": perPage
        }

        try:
            response = session.post(api_endpoint, json={'query': query, 'variables': variables}, headers=headers)
            response.raise_for_status()
        except:
            break
        else:
            print("Request was successful!")

        response_dict = response.json()
        print(response_dict)

        try:
            sets_data = response_dict['data']['event']['sets']
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
                        info_list.extend([entrant['id'], entrant['name'], slot['standing']['placement'], uid])

                        set_id.append(info_list[0])
                        entrant_id.append(info_list[1])
                        entrant_name.append(info_list[2])
                        standing.append(info_list[3])
                        user_id.append(info_list[4] if info_list[4] else None)

                    except TypeError:
                        continue
            if len(nodes) < perPage:
                has_next_page = False
            
        except KeyError:
            has_next_page = False

        cursor += 1
        sleep(0.5)

    df = pd.DataFrame({
        'set_id': set_id,
        'entrant_id': entrant_id,
        'entrant_name': entrant_name,
        'standing': standing,
        'user_id': user_id,
        'event_id': [event_id] * len(set_id),
        'source': ['startgg'] * len(set_id)
    })

    df = df.drop_duplicates()
    df['set_id'] = pd.to_numeric(df['set_id'], errors='ignore')
    print(df.head(5))
    return df

def getAllSets(event_list):
    """
    Fetches and combines sets data for multiple events into a single DataFrame.

    Args:
        event_list (list): A list of event IDs for which to fetch set data.

    Returns:
        DataFrame: A combined DataFrame containing all sets data from the listed events.
    """

    main = pd.DataFrame({
        'set_id': [],
        'entrant_id': [],
        'entrant_name': [],
        'standing': [],
        'user_id': [],
        'event_id': []
    })
    i = 0

    if os.path.isfile('all_sets.csv'):
        main = pd.read_csv('all_sets.csv')
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
        if i % 20 == 0:
            main.to_csv('all_sets.csv')
        if newest_event_id in main['event_id'].unique():
            break

        i += 1

    # Set user_id to 0 if None
    main.loc[main['user_id'].isnull(),'user_id'] = -1

    main.to_csv('all_sets.csv')

    return main

def getPlayersFromSets(sets_df):
    """
    Extracts player information from a DataFrame containing sets data.

    Args:
        sets_df (DataFrame): A DataFrame containing sets data with user IDs and entrant names.

    Returns:
        DataFrame: A DataFrame with columns for user IDs and entrant names, without duplicates.
    """

    df = sets_df[['user_id', 'event_id', 'entrant_name','is_guest']].drop_duplicates()

    return df

def getEventSort():
    """
    Loads an events DataFrame from a CSV file, sorts it by start time, and returns the sorted DataFrame.

    Returns:
        DataFrame: A DataFrame containing sorted event data by their starting time in ascending order.
    """

    # Load tournament and set data
    event_list = pd.read_csv('events.csv')

    # Get list of events sorted (ascending) by date
    event_sort_cols = ['event_id', 'start_at']
    event_list = event_list[event_sort_cols].sort_values('start_at', ascending=True)
    
    return event_list

def sortBySetId():
    """
    Loads a sets DataFrame from a CSV file, sorts it by set ID, and returns the sorted DataFrame.

    Returns:
        DataFrame: A DataFrame containing set data sorted by set IDs.
    """

    # Load set data
    sets = pd.read_csv('all_sets.csv')

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

    df['data_type'][df['source'] == 'startgg'] = 'Brackets'
    df = df.reset_index(drop=True)

    df2 = pd.read_csv('scrape_brackets.csv')[['event_id','event_name','comptier','date','func_type']]

    df2 = df2.rename(columns={'date': 'start_at',
                             'event_name': 'event_slug',
                             'comptier':'competition_tier'})
    
    df2['source'] = 'Liquidpedia'
    df2['start_at'] = pd.to_datetime(df['start_at'])
    df2['data_type'] = 'Brackets'
    df2['data_type'][df2['func_type'] != 3] = 'Brackets'
    df2['data_type'][df2['func_type'] == 3] = 'Pools'

    df2.drop(columns=['func_type'])

    df = pd.concat([df, df2], axis=0).reset_index(drop=True)
    df.drop_duplicates()

    return df

df = eventsByVideogame()
print(df)
df = pd.read_csv('events.csv')
events = df['event_id']
main = getAllSets(events)
main.to_csv('all_sets.csv', index=False)
main = main[pd.to_numeric(main['set_id'], errors='coerce').notnull()]
players = getPlayersFromSets(main)
players.to_csv('new_players.csv', index=False)