import requests
from bs4 import BeautifulSoup
import csv
import pandas as pd
import os
import re
import numpy as np

def writeBracketsToCsv(csv_file, tournament_data, event_id):
    with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Player 1', 'Result 1', 'Player 2', 'Result 2', 'Event Id'])
        
        for game in tournament_data:
            print(game)
            # Assuming each game in tournament_data contains multiple matches if available
            for match in game['matches']:
                writer.writerow([
                    match['player1'], match['result1'],
                    match['player2'], match['result2'],
                    event_id
                ])

    print('Data has been written to CSV.')

def writePoolsToCsv(csv_file, tournament_data, event_id):
    # Open the file in write mode
    with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Group', 'Player','Wins','Losses','Event_Id'])
        for player in tournament_data:
            writer.writerow(player)

    print('Data has been written to CSV.')

def scrapeBrackets(url, event_name, event_id):
    response = requests.get(url)
    html_content = response.text

    soup = BeautifulSoup(html_content, 'html.parser')
    tournament_data = []
    games = soup.find_all('div', class_='bracket-game')

    for game in games:
        matches = []
        players = game.find_all('div', class_=['bracket-player-top', 'bracket-player-bottom'])
        
        if len(players) != 2:
            continue  # Skip games without exactly two player entries
        
        # Collect all scores for both players
        all_scores = [
            [div.text.strip() for div in player.find_all('div', class_='bracket-score')]
            for player in players
        ]
        
        # Assuming both players have the same number of scores
        player_names = [player.find('span', style='vertical-align:-1px;').text.strip() for player in players]
        
        num_matches = len(all_scores[0])  # Number of matches based on the number of scores for player 1
        for i in range(num_matches):
            match_data = {
                'player1': player_names[0],
                'result1': all_scores[0][i],
                'player2': player_names[1],
                'result2': all_scores[1][i]
            }
            matches.append(match_data)

        tournament_data.append({'matches': matches})

    event_name = event_name.strip()
    file_name = 'sf6folder/{}_bracket.csv'.format(re.sub('[^A-Za-z0-9]+', '', event_name))
    i = 1
    while not os.path.isfile(file_name):
        file_name = 'sf6folder/{}{}_bracket.csv'.format(re.sub('[^A-Za-z0-9]+', '', event_name), i)
        i += 1
    writeBracketsToCsv(file_name, tournament_data, event_id)

def scrapeGroups(url, event_name, event_id):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    tournament_data = []
    match_rows = soup.find_all('tr', class_='match-row')
    for row in match_rows:
        cells = row.find_all('td')
        if len(cells) >= 4:  # Assumption: there are at least 4 cells (Player 1, Score 1, Score 2, Player 2)
            player1_name = cells[0].find('span', style='white-space: pre').get_text(strip=True)
            player2_name = cells[-1].find('span', style='white-space: pre').get_text(strip=True)
            score1 = cells[2].get_text(strip=True)
            score2 = cells[-2].get_text(strip=True)

            match_data = {
                'player1': player1_name,
                'result1': score1,
                'player2': player2_name,
                'result2': score2
            }

            matches = [match_data]

            tournament_data.append({'matches': matches})
    
    # Specify the CSV file to write to
    event_name = event_name.strip()
    file_name = 'sf6folder/{}_bracket.csv'.format(re.sub('[^A-Za-z0-9]+', '', event_name))
    i = 1
    while not os.path.isfile(file_name):
        file_name = 'sf6folder/{}{}_bracket.csv'.format(re.sub('[^A-Za-z0-9]+', '', event_name), i)
        i += 1

    writeBracketsToCsv(file_name, tournament_data, event_id)

def scrapePools(url, event_name, event_id):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    data = []
    current_group = ''

    tables = soup.find_all('table')  # Assuming each group is a separate table
    for table in tables:
        rows = table.find_all('tr')
        for index, row in enumerate(rows):
            if index == 0:
                # This assumes the first row might have group information or can be skipped if no group info
                group_header = row.find('th')  # Adjust tag if necessary
                if group_header:
                    current_group = group_header.text.strip()
                continue
            cells = row.find_all('td')
            #if len(cells) == 2:  # Now assuming exactly two cells, player info and score
            player_info = cells[0].text.strip()
            try:
                win_loss = cells[1].text.strip().split('-')
                if len(win_loss) == 2:
                    wins, losses = win_loss
                    data.append([current_group, player_info, wins, losses, event_id])
            except:
                continue

    # Specify the CSV file to write to
    # Specify the CSV file to write to
    event_name = event_name.strip()
    file_name = 'sf6folder/{}_pools.csv'.format(re.sub('[^A-Za-z0-9]+', '', event_name))
    i = 1
    while not os.path.isfile(file_name):
        file_name = 'sf6folder/{}{}_pools.csv'.format(re.sub('[^A-Za-z0-9]+', '', event_name), i)
        i += 1

    writePoolsToCsv(file_name, data, event_id)

def process_row(row):
    print(type(row))
    print(row)
    function_mapping = {
        1: scrapeBrackets,
        2: scrapeGroups,
        3: scrapePools,
    }
    func_type = row['func_type']
    print('func_type: {}'.format(func_type))
    url = row.get('url')
    event_name = row.get('event_name')
    event_id = row.get('event_id')
    func_to_call = function_mapping[func_type]  # Get the function based on func_type
    print(func_to_call)
    if func_to_call:
        func_to_call(url, event_name, event_id)  # Call the function

def scrapeAll(input_path):
    # Read csv of data to scrape
    df = pd.read_csv(input_path)
    
    for index, row in df.iterrows():
        process_row(row)

    directory = 'sf6folder'
    bracket_file = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('bracket.csv')]
    pool_file = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('pools.csv')]

    brackets = (pd.read_csv(file) for file in bracket_file)
    combined_brackets = pd.concat(brackets, ignore_index=True)
    combined_brackets.to_csv('brackets.csv', index=False)

    pools = (pd.read_csv(file) for file in pool_file)
    combined_pools = pd.concat(pools, ignore_index=True)
    combined_pools.to_csv('pools.csv', index=False)

def generateUID(df):
    return df.loc[:, 'user_id'].max() + 1

def generatePlayerRow(df, uid, is_guest='Yes'):
    # Get event_id and entrant_name
    event_id, entrant_name = df.loc[0, ['event_id', 'entrant_name_input']]

    # Generate Row
    row = pd.DataFrame({
        'user_id': [uid],
        'event_id': [event_id],
        'entrant_name': [entrant_name],
        'is_guest': [is_guest]
    })

    return row

def addPlayersFromLiquidpedia(df_path='all_matches.csv', players_path='players.csv', test=False):
    matching_table = pd.read_csv(df_path)

    # Get list of unique entant_name_input names
    players = pd.read_csv(players_path)

    # Only run function on entries which need matching
    new_players = matching_table[(matching_table['user_id_matched'] == 0) | (matching_table['user_id_matched'].isnull())]
    new_players = new_players['entrant_name_input'].unique()

    # iterate look on all entrant_names
    for p in new_players:
        player_lookup = matching_table.loc[matching_table['entrant_name_input'] == p, :].reset_index()
        score = player_lookup.loc[0,'score']
        uid = player_lookup.loc[0, 'user_id_matched']

        if score > 93:
            # If the user_id doesn't exist for this player, generate a new user_id for the players table
            if uid == 0:
                new_id = generateUID(players)

                # Assign new user_id to linking table
                matching_table.loc[matching_table['entrant_name_input'] == p, 'user_id_matched'] = new_id
            else:
                new_id = uid

            # Insert row for players table
            new_row = generatePlayerRow(player_lookup, new_id, is_guest='Yes')
            if test == True:
                print(new_row)
            players = pd.concat([players, new_row], axis=0).reset_index(drop=True)
        
        # If no sufficient match exists, do same as for if uid == 0
        else:
            new_id = generateUID(players)
            matching_table.loc[matching_table['entrant_name_input'] == p, 'user_id_matched'] = uid

            # Insert row for players table
            new_row = generatePlayerRow(player_lookup, new_id, is_guest='Yes')

            if test == True:
                print(new_row)

            players = pd.concat([players, new_row], axis=0).reset_index(drop=True)

    if test == False:
        players.loc[:, 'user_id'] = players.loc[:, 'user_id'].astype(int)
        matching_table.loc[:, 'user_id_matched'] = matching_table.loc[:, 'user_id_matched'].fillna(0).astype(int)

        players = players.drop_duplicates(['event_id', 'entrant_name', 'is_guest'])

        matching_table.to_csv(df_path, index=False)
        players.to_csv(players_path, index=False)

def checkIDSeries(ids):
    """
    Checks if a series has a value. If so, return first value. Else, return nan

    Args:
    ids (series): Series of user_id values from the all_sets table

    Returns:
    If a series is NOT empty, returns first value. Else, returns a numpy NaN value.
    """
    if len(ids) > 0:
        id = ids.iloc[0]
    else:
        id = np.nan

    return id

def getUserId(name_string, matched_players):
    """
    Gets user_id for players not matched in default dataframe

    Args:
    name_string (str): Entrant_name from liquidpedia brackets
    matched_players: df for matched players

    Returns:
    user_id of matched player
    """

    name = matched_players.loc[matched_players['entrant_name_input'] == name_string, 'user_id_matched'].min()

    # ToDo: Add function to assign a new user_id to matched_player table if returns a nan value
    print(name)
    return name


def integrateSets(brackets_data='brackets.csv', data='all_sets.csv', players_path='players.csv', matched_players='all_matches.csv', test=False):
    """
    Integrates bracket data to set data (by default, all_sets.csv)

    Args:
    df: Brackets data
    data: Filepath to set data
    players_path: filepath to players data
    test: Toggle to true if wanting to test function without changing data

    Returns: 
    """

    # Read set data with all sets
    sets = pd.read_csv(data)
    sets = sets[['set_id','entrant_id','entrant_name','standing','user_id','event_id','source']]

    # Get Liquidpedia brackets
    df = pd.read_csv(brackets_data)

    # Get Player data (for looking up player's user_id vals)
    players = pd.read_csv(players_path)

    id_matches = pd.read_csv(matched_players)

    events = df.loc[:,'Event Id'].unique()

    # For each event id, iterate over the rows that correspond to each event's matches
    for event in events:
        setid = 1
        subset = df.loc[df['Event Id'] == event, :].reset_index()

        results = []

        for ind, row in subset.iterrows():

            # Pull the score for the set (only if there's no DQ and there are actual scores for both)
            scores = [row['Result 1'], row['Result 2']]
            try:
                scores = [int(x) for x in scores]
                if len(scores) != 2 or not all(isinstance(x, int) for x in scores):
                    pass
                else:
                    # Allocate scores as standings (so winning the set gives you a 1, losing a 2)
                    if scores[0] > scores[1]:
                        standing = [1, 2]
                    elif scores[1] > scores[0]:
                        standing = [2, 1]

                    player1 = row.loc['Player 1']
                    player2 = row.loc['Player 2']

                    uid1 = players.loc[players['entrant_name'] == player1, 'user_id']
                    uid2 = players.loc[players['entrant_name'] == player2, 'user_id']

                    uid1 = checkIDSeries(uid1)
                    uid2 = checkIDSeries(uid2)

                    if uid1 == np.nan:
                        uid1 = getUserId(player1, id_matches)
                    if uid2 == np.nan:
                        uid2 = getUserId(player2, id_matches)

                    # Build rows and add to results space
                    row1 = [setid, 0, player1, standing[0], uid1, event, 'Liquidpedia']
                    row2 = [setid, 0, player2, standing[1], uid2, event, 'Liquidpedia']

                    results.extend([row1, row2])
            except:
                pass
                
            setid += 1

    results = pd.DataFrame(results, columns = sets.columns) 

    # Put set data into dataframe
    sets = pd.concat([sets, results])
    print(sets)

    if test == False:
        sets.to_csv(data, index=False)
    else:
        print(sets)
        sets.to_csv('test.csv', index=False)


#scrapeAll("scrape_brackets.csv")
addPlayersFromLiquidpedia(df_path='all_matches.csv', players_path='players.csv', test=False)
integrateSets(test=True)