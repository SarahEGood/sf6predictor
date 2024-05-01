import pandas as pd
import numpy as np
import os
from time import sleep
from datetime import datetime

def integrateStartGGPlayers(og_data_path='players.csv'):
    if os.path.isfile('data\\players.csv'):
        df = pd.read_csv('data\\players.csv')
        uid_ind = df.loc[:, 'uid'].max() + 1
    else:
        df = pd.DataFrame({
            'uid': [],
            'player_name': [],
            'date_added': [],
            'country': [],
            'start_gg_id': [],
            'liquidpedia_name': []
        })
        uid_ind = 0

    startgg = pd.read_csv(og_data_path)

    for ind, row in startgg.loc[startgg['is_guest'] != 'Yes', :].iterrows():
        if df.loc[df['start_gg_id'] == row['user_id']].empty:
            new_uid = uid_ind
            player_name = row['entrant_name']
            start_gg_id = row['user_id']
            datetime_now = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
            print(df.columns)
            row_list = [new_uid, player_name, datetime_now, '', start_gg_id, '']
            print(row_list)

            new_row = pd.DataFrame([row_list], columns = df.columns)

            uid_ind += 1

            df = pd.concat([df, new_row], axis=0, ignore_index=True)

    df.to_csv('data\\players.csv', index=False)

    return df

df = integrateStartGGPlayers()