import pandas as pd

def eloFormula(rating1, rating2, result1, result2, k=30):
    """
    Calculate the new Elo ratings for two players based on their match results.

    Args:
    rating1 (float): Current Elo rating of the first player.
    rating2 (float): Current Elo rating of the second player.
    result1 (int): Match standing for the first player (1 for win, 2 for loss).
    result2 (int): Match standing for the second player (1 for win, 2 for loss).
    k (int, optional): The maximum possible adjustment per game, defaults to 30.

    Returns:
    tuple: A tuple containing the new Elo ratings for the first and second players.
    """

    est1 = 1 / (1 + 10**((rating2-rating1)/400))
    est2 = 1 / (1 + 10**((rating1-rating2)/400))

    result1 = 1 if result1 == 1 else 0
    result2 = 1 if result2 == 1 else 0

    if result1 != result2:
        new1 = rating1 + k*(result1 - est1)
        new2 = rating2 + k*(result2 - est2)
    else:
        new1 = rating1 + k*(0.5 - est1)
        new2 = rating2 + k*(0.5 - est2)

    return new1, new2

def eloPoolsFormula(ratings, wins, losses, k=30):
    """
    Adjusts the Elo ratings for a pool of players based on their collective wins and losses.

    Args:
    ratings (list of float): Current Elo ratings of all players in the pool.
    wins (list of int): Number of wins for each player in the pool.
    losses (list of int): Number of losses for each player in the pool.
    k (int, optional): The maximum possible adjustment per game, defaults to 30.

    Returns:
    list: New Elo ratings for all players in the pool.
    """

    total_wins = sum(wins)
    total_losses = sum(losses)
    
    # Calculate sum of ratings weighted by wins and losses
    total_rating_wins = sum(ratings[i] * wins[i] for i in range(len(ratings)))
    total_rating_losses = sum(ratings[i] * losses[i] for i in range(len(ratings)))
    
    # Calculate average Elo of those who were won against and those who lost
    if total_wins > 0:
        ratings_winners = total_rating_wins / total_wins
    else:
        ratings_winners = 0  # To handle division by zero if no wins recorded
    
    if total_losses > 0:
        ratings_losers = total_rating_losses / total_losses
    else:
        ratings_losers = 0  # To handle division by zero if no losses recorded
    
    # Calculate estimated win/loss Elo values for each player
    new_ratings = []
    for i in range(len(ratings)):
        expected_win = 1 / (1 + 10 ** ((ratings[i] - ratings_winners) / 400))
        expected_loss = 1 / (1 + 10 ** ((ratings[i] - ratings_losers) / 400))
        
        # Update ratings based on the results and the expected outcomes
        rating_change_win = k * (wins[i] - expected_win)
        rating_change_loss = k * (losses[i] - expected_loss)
        new_rating = ratings[i] + rating_change_win + rating_change_loss
        new_ratings.append(new_rating)

    return new_ratings

# Function to get user_id from dataframes
def getSetElo(row, player_lookup, elo_lookup):
    """
    Retrieve or initialize Elo rating for a player based on event and player lookup tables.

    Args:
    row (pandas.Series): Data containing user_id, standing, entrant_name, and event_id.
    player_lookup (pandas.DataFrame): DataFrame mapping player names to user IDs.-
    elo_lookup (pandas.DataFrame): DataFrame maintaining Elo ratings by user_id and event_id.

    Returns:
    dict or None: Returns a dictionary with user and event details including Elo or None if user cannot be found.
    """
    user_id = row['user_id'][0]
    standing = row['standing'][0]
    user_name = row['entrant_name'][0]
    event_id = row['event_id'][0]

    new_elo_flag = 0

    # Search for user id if player has no id
    if user_id == 0:
        user_id = player_lookup[(player_lookup['entrant_name'] == user_name) & (player_lookup['event_id'] == event_id)]['user_id']
        if not user_id.empty:
            user_id = user_id.iloc[0]
        else:
            return None

    # Get Elo for event
    current_elo = elo_lookup[(elo_lookup['user_id'] == user_id) & (elo_lookup['event_id'] == event_id)]['elo']
    # If no elo for current event and player, create from previous event or set to 200
    if current_elo.empty:
        # Get most recent elo value
        current_elo = elo_lookup[elo_lookup['user_id'] == user_id]['elo'].iloc[-1:]
        if current_elo.empty:
            new_elo_flag = 1
            elo = 200
        else:
            elo = current_elo.iloc[0]
    else:
        elo = current_elo.iloc[0]

    return {'user_id': user_id,
            'standing': standing,
            'user_name': user_name,
            'event_id': event_id,
            'elo': elo,
            'new_elo_flag': new_elo_flag}

def reviseElo(user_id_1, user_id_2, elo1, elo2, event_id, elo_lookup):
    """
    Updates the Elo ratings for two players in the lookup table after a match.

    Args:
    user_id_1 (int): User ID of the first player.
    user_id_2 (int): User ID of the second player.
    elo1 (float): New Elo rating of the first player.
    elo2 (float): New Elo rating of the second player.
    event_id (int): ID of the event where the match occurred.
    elo_lookup (pandas.DataFrame): DataFrame containing the Elo ratings.

    Returns:
    pandas.DataFrame: Updated DataFrame with new Elo ratings.
    """
    user_list = [{'user_id': user_id_1,
                  'elo': elo1},
                  {'user_id': user_id_2,
                   'elo': elo2}]
    
    tiers = ['tier1', 'tier2', 'tier3', 'tier5']

    # First, check if elo exists in table
    for user in user_list:
        elo_check = elo_lookup.loc[(elo_lookup['user_id'] == user['user_id']) & (elo_lookup['event_id'] == event_id), 'elo']
        if elo_check.empty:
            get_tiers = elo_lookup.loc[(elo_lookup['user_id'] == user['user_id']), tiers]
            if not get_tiers.empty:
                get_tiers = get_tiers.iloc[-1]
                new_entry = pd.DataFrame({'user_id': [user['user_id']],
                                        'event_id': [event_id],
                                        'elo': [user['elo']],
                                        'tier1': get_tiers['tier1'],
                                        'tier2': get_tiers['tier2'],
                                        'tier3': get_tiers['tier3'],
                                        'tier5': get_tiers['tier5']})
            else:
                new_entry = pd.DataFrame({'user_id': [user['user_id']],
                                        'event_id': [event_id],
                                        'elo': [user['elo']],
                                        'tier1': 0,
                                        'tier2': 0,
                                        'tier3': 0,
                                        'tier5': 0})
            elo_lookup = pd.concat([elo_lookup, new_entry])
        else:
            elo_lookup.loc[(elo_lookup['user_id'] == user['user_id']) & (elo_lookup['event_id'] == event_id), 'elo'] = user['elo']

    return elo_lookup

def calcEloForSet(df, elo_lookup, player_lookup):
    """
    Processes a set (match between two players) to update Elo ratings.

    Args:
    df (pandas.DataFrame): DataFrame representing a single set.
    elo_lookup (pandas.DataFrame): DataFrame containing Elo ratings.
    player_lookup (pandas.DataFrame): DataFrame mapping player names to user IDs.

    Returns:
    pandas.DataFrame: Updated Elo lookup table after processing the set.
    """
    if len(df) == 2:
        # Retrieve Elo from user id and lookup tables
        p1 = getSetElo(df.reset_index().iloc[[0]].reset_index(), player_lookup, elo_lookup)
        p2 = getSetElo(df.reset_index().iloc[[1]].reset_index(), player_lookup, elo_lookup)

        if p1 != None and p2 != None:
    
            # Generate Elo Ratings
            elo1, elo2 = eloFormula(p1['elo'], p2['elo'], p1['standing'], p2['standing'])

            # Insert Elos back into lookup table
            elo_lookup = reviseElo(p1['user_id'], p2['user_id'], elo1, elo2, p1['event_id'], elo_lookup)

            return elo_lookup
        
        else:
            return elo_lookup

# Example usage:
# Assuming you have a pandas DataFrame `matches` with appropriate columns and `elo_lookup`, `player_lookup` DataFrames ready.
# results_elo = calcEloForSet(matches, elo_lookup, player_lookup)

def calcEloForEvent(df, event_id, elo_lookup, player_lookup, event_comptiers):
    # Filter to event_id
    df_events = df.loc[df['event_id'] == event_id]
    sets = df_events.set_id.unique()
    comp_tier = event_comptiers.loc[event_comptiers['event_id'] == event_id, 'competition_tier']
    print(comp_tier)
    
    if comp_tier.empty:
        comp_tier = 5
    else:
        comp_tier = comp_tier.iloc[0]
    comp_tier = int(comp_tier)

    tiers = {
        1: 'tier1',
        2: 'tier2',
        3: 'tier3',
        5: 'tier5',
        53: 'tier5'
    }

    for set in sets:
        df_set = df_events.loc[df_events['set_id'] == set]
        new_elo = calcEloForSet(df_set, elo_lookup, player_lookup)

        if isinstance(new_elo, pd.DataFrame):
            elo_lookup = new_elo

    updated_tiers = elo_lookup.loc[elo_lookup['event_id'] == event_id, tiers[comp_tier]].add(1)
    elo_lookup.loc[elo_lookup['event_id'] == event_id, tiers[comp_tier]] = updated_tiers

    return elo_lookup

def getEventList(path):
    """
    Reads a CSV file containing event data and returns a DataFrame with event IDs ordered by their start dates.

    Args:
    path (str): Path to the CSV file containing the event data.

    Returns:
    pandas.DataFrame: DataFrame containing columns for 'start_at' (event start date) and 'event_id', sorted by 'start_at'.
    """
    events = pd.read_csv(path)[['start_at', 'event_id']].sort_values('start_at')
    return events

def calcEloWrapper(set_path='all_sets.csv', player_path='players.csv', event_path='events.csv'):
    """
    Processes Elo rating updates for all events specified in a given imported dataframe. It reads the event, player, and set data,
    updates Elo ratings for each event, and saves the final Elo ratings to CSV files.

    Args:
    set_path (str, optional): Path to the CSV file containing match sets. Defaults to 'all_sets.csv'.
    player_path (str, optional): Path to the CSV file containing player information. Defaults to 'players.csv'.
    event_path (str, optional): Path to the CSV file containing event information. Defaults to 'events.csv'.
    """
    events = getEventList(event_path)
    event_comptiers = pd.read_csv(event_path)
    player_lookup = pd.read_csv(player_path)
    df = pd.read_csv(set_path)
    elo_lookup = pd.DataFrame({'user_id': [], 'event_id': [], 'elo': [], 'tier1': [], 'tier2': [],
                               'tier3': [], 'tier5': []})

    for event_id in events.event_id:
        elo_lookup = calcEloForEvent(df, event_id, elo_lookup, player_lookup, event_comptiers)

    elo_lookup.to_csv('elo_records.csv', index=False)
    current_elo = elo_lookup.sort_values(by="event_id").drop_duplicates(subset=["user_id"], keep="last")
    p_uniques = player_lookup[['user_id', 'entrant_name']].drop_duplicates(subset=["user_id"], keep="last")
    current_elo = current_elo.merge(p_uniques, how='left', on='user_id')
    current_elo.to_csv('current_elo.csv', index=False)

def calcEventParticipation(elo_records='elo_records.csv', event_path='events.csv'):
    """
    Calculates and prints the count of participants in events grouped by their competition tier, based on the Elo ratings record.

    Args:
    elo_records (str, optional): Path to the CSV file containing Elo records. Defaults to 'elo_records.csv'.
    event_path (str, optional): Path to the CSV file containing event information. Defaults to 'events.csv'.
    """
    events = pd.read_csv(event_path).sort_values('start_at', ascending=True)
    elo_df = pd.read_csv(elo_records)

    tiers = ['tier1', 'tier2', 'tier3', 'tier5']

    elo_df['tier1'] = 0
    elo_df['tier2'] = 0
    elo_df['tier3'] = 0
    elo_df['tier5'] = 0

    tier_mapping = {
        1: 'tier1',
        2: 'tier2',
        3: 'tier3',
        5: 'tier5'
    }

    # ToDO: need to calculate sum of each amount of tiers user participated in to date for each elo record

    for ind, row in elo_df.iterrows():
        event_id = row['event_id']
        comp_tier = events.loc[events['competition_tier'] == event_id, 'competition_tier'][0]
        user_tier_vals = elo_df.loc[elo_df['user_id'] == row['user_id'], tiers][0]
        row
        

calcEloWrapper()
calcEventParticipation()