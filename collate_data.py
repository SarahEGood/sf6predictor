import pandas as pd
import numpy as np
import os
from time import sleep
import time
from datetime import datetime
from extract_startgg_data import startgg_vars, retryStrategy
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def safe_get(d, keys, default=None):
    assert isinstance(keys, list), "keys must be provided as a list"
    current = d
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default
    return current

def fetchPlayerbyId(player_id):
    api_endpoint, token = startgg_vars()

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
    gamerTag = safe_get(data, ['gamerTag'])
    prefix = safe_get(data, ['prefix'])
    user_id = safe_get(data, ['user', 'id'])
    full_name = safe_get(data, ['user', 'name'])
    country = safe_get(data, ['user', 'location', 'country'])
    state = safe_get(data, ['user', 'location', 'state'])

    twitter_handle = None
    discord_id = None
    discord_username = None
    xbox_username = None
    mixer_username = None
    twitch_id = None
    twitch_name = None
    
    socials_raw = safe_get(['user', 'authorizations'])
    if socials_raw:
        for auth in socials_raw:
            ext_id = safe_get(auth, ['externalId'])
            externalUsername = safe_get(auth, ['externalUsername'])
            service = safe_get(auth, ['type'])

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

    data_dict = {
            'player_name': gamerTag,
            'full_name': full_name,
            'prefix': prefix,
            'startgg_pid': int(player_id),
            'startgg_uid': int(user_id),
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

def integrateStartGGPlayers(og_data_path='players.csv'):
    start_time = time()

    if os.path.isfile('data\\players.csv'):
        df = pd.read_csv('data\\players.csv')
        uid_ind = df.loc[:, 'uid'].max() + 1
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
        uid_ind = 0

    startgg = pd.read_csv(og_data_path)

    for pid in startgg['player_id'].unique():
        if df.loc[df['startgg_pid'] == pid,:].empty:

            data = fetchPlayerbyId(pid)

            if data:
                new_data = processPlayerData(pid, data)
                datetime_now = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")

                new_data.update({
                    'uid': uid_ind,
                    'date_added': datetime_now
                    })

                uid_ind += 1
                new_row = pd.DataFrame(new_data)

                df = pd.concat([df, new_row], axis=0, ignore_index=True)

                if uid_ind % 20:
                    df.to_csv('data\\players.csv', index=False)

                sleep(0.5)

    df.to_csv('data\\players.csv', index=False)

    hours = round((time() - start_time)/60, 2)
    print("All set total runtime: {} hours".format(hours))

    return df

if __name__ == '__main__':
    df = integrateStartGGPlayers('SF6\\players.csv')