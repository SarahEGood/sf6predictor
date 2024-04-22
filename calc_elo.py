import pandas as pd

def eloFormula(rating1, rating2, result1, result2, k=30):
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
    total_wins = sum(wins)
    total_losses = sum(losses)
    
    # Calculate sum of ratings weighted by wins and losses
    total_rating_wins = sum(r[i] * wins[i] for i in range(len(ratings)))
    total_rating_losses = sum(r[i] * losses[i] for i in range(len(ratings)))
    
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
    #print(row)
    user_id = row['user_id'][0]
    standing = row['standing'][0]
    user_name = row['entrant_name'][0]
    event_id = row['event_id'][0]

    new_elo_flag = 0

    # Search for user id if player has no id
    if user_id == 0:
        user_id = player_lookup[(player_lookup['entrant_name'] == user_name) & (player_lookup['event_id'] == event_id)]['user_id']
        if not user_id.empty:
            print(user_id)
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

'''Updates ELO table'''
def reviseElo(user_id_1, user_id_2, elo1, elo2, event_id, elo_lookup):
    user_list = [{'user_id': user_id_1,
                  'elo': elo1},
                  {'user_id': user_id_2,
                   'elo': elo2}]

    # First, check if elo exists in table
    for user in user_list:
        elo_check = elo_lookup.loc[(elo_lookup['user_id'] == user['user_id']) & (elo_lookup['event_id'] == event_id), 'elo']
        if elo_check.empty:
            new_entry = pd.DataFrame({'user_id': [user['user_id']], 'event_id': [event_id], 'elo': [user['elo']]})
            elo_lookup = pd.concat([elo_lookup, new_entry])
        else:
            elo_lookup.loc[(elo_lookup['user_id'] == user['user_id']) & (elo_lookup['event_id'] == event_id), 'elo'] = user['elo']

    return elo_lookup

'''Takes a dataframe subsetted by set'''
def calcEloForSet(df, elo_lookup, player_lookup):
    # Check if dataframe slice is two length before looping
    if len(df) == 2:
        # Retrieve Elo from user id and lookup tables
        print(df)
        p1 = getSetElo(df.reset_index().iloc[[0]].reset_index(), player_lookup, elo_lookup)
        p2 = getSetElo(df.reset_index().iloc[[1]].reset_index(), player_lookup, elo_lookup)

        if p1 != None and p2 != None:
    
            # Generate Elo Ratings
            elo1, elo2 = eloFormula(p1['elo'], p2['elo'], p1['standing'], p2['standing'])
            print(elo1, elo2)

            # Insert Elos back into lookup table
            elo_lookup = reviseElo(p1['user_id'], p2['user_id'], elo1, elo2, p1['event_id'], elo_lookup)

            return elo_lookup
        
        else:
            return elo_lookup

def calcEloForEvent(df, event_id, elo_lookup, player_lookup):
    # Filter to event_id
    df_events = df.loc[df['event_id'] == event_id]
    sets = df_events.set_id.unique()

    for set in sets:
        df_set = df_events.loc[df_events['set_id'] == set]
        new_elo = calcEloForSet(df_set, elo_lookup, player_lookup)

        if isinstance(new_elo, pd.DataFrame):
            elo_lookup = new_elo

    return elo_lookup

'''Func to get list of ordered event_ids by date'''
def getEventList(path):
    events = pd.read_csv(path)[['start_at', 'event_id']].sort_values('start_at')
    return events

'''Wrapper to iterate through all event_ids'''
def calcEloWrapper(set_path='all_sets.csv', player_path='players.csv', event_path='events.csv'):
    events = getEventList(event_path)
    player_lookup = pd.read_csv(player_path)
    df = pd.read_csv(set_path)
    elo_lookup = pd.DataFrame({'user_id': [], 'event_id': [], 'elo': []})

    for event_id in events.event_id:
        elo_lookup = calcEloForEvent(df, event_id, elo_lookup, player_lookup)

    elo_lookup.to_csv('elo_records.csv')
    current_elo = elo_lookup.sort_values(by="event_id").drop_duplicates(subset=["user_id"], keep="last")
    p_uniques = player_lookup[['user_id', 'entrant_name']].drop_duplicates(subset=["user_id"], keep="last")
    current_elo = current_elo.merge(p_uniques, how='left', on='user_id')
    current_elo.to_csv('current_elo.csv')

'''Calculate Tournament Participation Rates (locals and higher tiers)'''
def calcEventParticipation(elo_records='elo_records.csv', event_path='events.csv'):
    # Read in data
    events = pd.read_csv(event_path)
    elo_df = pd.read_csv(elo_records)

    df = events.groupby('competitionTier').count()
    print(df)

calcEloWrapper()
#calcEventParticipation()