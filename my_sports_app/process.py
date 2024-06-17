import json
import pandas as pd
from datetime import datetime


GAME_TRANSLATOR = {'tournament_id' : 'TournamentID',
                   'homeTeam_id' : 'homeTeamID',
                   'homeScore_current' : 'homeScore',
                   'awayScore_current' : 'awayScore',
                   'awayTeam_id' : 'awayTeamID'}
GAME_EXTRACTOR = {'tournament' : ['id'], 
                  'homeTeam' : ['id'], 
                  'awayTeam' : ['id'],
                  'homeScore' : ['current'],
                  'awayScore' : ['current'],
                  'startTimestamp' : None
                  }

TORUNAMENT_EXTRACTOR = ['name']

TEAM_EXTRACTOR = ['name', 
                  'gender', 
                  {'sport' : 'id'},
                  {'country' : 'name'}]



def concat(df_a, df_b, key):
    """
    Concatenates two pandas DataFrames if the value of the specified key in df_b is not present in df_a.
    
    Args:
        df_a (pandas.DataFrame): The first DataFrame.
        df_b (pandas.DataFrame): The second DataFrame.
        key (str): The key to check for existence in df_a.
        
    Returns:
        pandas.DataFrame: The concatenated DataFrame if the value of the specified key in df_b is not present in df_a.
        Otherwise, returns df_a.
    """
    if df_b[key].item() not in df_a[key].values:
        return pd.concat([df_a, df_b])
    return df_a
def extract_game(event):
    """
    Extracts game information from an event dictionary and returns it as a Pandas DataFrame.

    Parameters:
    event (dict): A dictionary containing information about an event.

    Returns:
    pandas.DataFrame: A DataFrame containing the extracted game information.

    """
    game_dict = {'GameID' : event['id']}
    for k,v in GAME_EXTRACTOR.items():
        if v is None:
            val = event[k]
            if k == 'startTimestamp':
                # Convert Unix timestamp to datetime object
                datetime_obj = datetime.fromtimestamp(val)
                # Format datetime object to SQL DATETIME format
                val = datetime_obj.strftime('%Y-%m-%d %H:%M:%S')
            game_dict[k] = val
        else:
            for key in v:
                df_key = GAME_TRANSLATOR[k+'_'+key]
                game_dict[df_key] = event[k][key]
    return pd.DataFrame(game_dict, index = ['game_id'])
def extract_tournament(event):
    """
    Extracts tournament information from an event and returns it as a pandas DataFrame.

    Parameters:
    event (dict): The event from which to extract tournament information.

    Returns:
    pandas.DataFrame: A DataFrame containing the extracted tournament information.

    """
    tournament_dict = {'tournament_id' : event['id']}
    for field in TORUNAMENT_EXTRACTOR:
        if isinstance(field, dict):
            for key, val in field.items():
                tournament_dict[key + '_' + val] = event[key][val]
        else:
            tournament_dict[field] = event[field]
    tournament_dict['tourName'] = tournament_dict.pop('name')
    return pd.DataFrame(tournament_dict, index = ['tournament_id'])
def extract_team(event, gender = None):
    """
    Extracts team information from an event.

    Parameters:
    event (dict): A dictionary containing event information.
    gender (str, optional): The gender of the team. Defaults to None.

    Returns:
    pandas.DataFrame: A DataFrame containing the extracted team information.

    """
    team_dict = {'team_id' : event['id']}
    for field in TEAM_EXTRACTOR:
        if isinstance(field, dict):
            for key, val in field.items():
                try:
                    team_dict[key + '_' + val] = event[key][val]
                except:
                    team_dict[key + '_' + val] = None
        else:
            try:
                if field == 'name':
                    team_dict['teamName'] = event[field]
                else:
                    team_dict[field] = event[field]
            except KeyError:
                team_dict[field] = None
    return pd.DataFrame(team_dict, index = ['team_id'])

def proccess_req(data):
    """
    Processes the given data and returns games, tournaments, and teams.

    Args:
        data (dict): The data to be processed.

    Returns:
        tuple: A tuple containing three pandas DataFrames - games_df, tournaments_df, and teams_df.

    """
    games_df = pd.DataFrame(columns=['GameID', 'TournamentID', 'homeTeamID', 'homeScore', 'awayScore', 'startTimestamp'])
    tournaments_df = pd.DataFrame(columns=['tournament_id', 'name'])
    teams_df = pd.DataFrame(columns=['team_id', 'teamName', 'gender', 'sport_id', 'country_name'])

    for event in data['events']:
        game = extract_game(event)
        games_df = concat(games_df, game, 'GameID')
        tournament = extract_tournament(event['tournament'])
        tournaments_df = concat(tournaments_df, tournament, 'tournament_id')
        homeTeam = extract_team(event['homeTeam'])
        awayTeam = extract_team(event['awayTeam'])
        teams_df = concat(teams_df, homeTeam, 'team_id')
        teams_df = concat(teams_df, awayTeam, 'team_id')

    return games_df, tournaments_df, teams_df

# SQL INSERTS: 
def insert_tour(cursor, tour_df):
    """
    Inserts tournament data into the Tournaments table.

    Args:
        cursor: The database cursor object.
        tour_df: The DataFrame containing the tournament data.

    Returns:
        None
    """
    for index, row in tour_df.iterrows():
        try:
            cursor.execute("IF NOT EXISTS (SELECT 1 FROM Tournaments WHERE TournamentID = ?) \
                           INSERT INTO Tournaments \
                           (TournamentID, Name) values(?,?)", 
                           row.tournament_id, row.tournament_id, row.tourName)
        except Exception as e:
            print(e)
            print(f'{row.tournament_id=}')
            
def insert_games(cursor, games_df):
    """
    Inserts or updates game records in the Games table using data from a DataFrame.

    Args:
        cursor: The database cursor object.
        games_df: A DataFrame containing game data.

    Returns:
        None
    """
    for index, row in games_df.iterrows():
        query = ''' MERGE INTO Games AS target \
        USING (SELECT ? AS GameID, ? AS TournamentID, ? AS homeTeamID, ? 
                       AS awayTeamID, ? AS homeScore, ? AS awayScore, ? AS startTimestamp) AS source
        ON target.GameID = source.GameID 
        WHEN MATCHED THEN
            UPDATE SET homeScore = source.homeScore,
                       awayScore = source.awayScore
        WHEN NOT MATCHED THEN
            INSERT (GameID, TournamentID, homeTeamID, awayTeamID, homeScore, awayScore, startTimestamp)
            VALUES (source.GameID, source.TournamentID, source.homeTeamID, 
                       source.awayTeamID, source.homeScore, source.awayScore, source.startTimestamp);
        '''
        data = [row['GameID'], row['TournamentID'], # VALUE
        row['homeTeamID'], row['awayTeamID'], # VALUE
        row['homeScore'], row['awayScore'], row['startTimestamp']]
        cursor.execute(query, data)   # VALUE

        
def insert_teams(cursor, teams_df):
    """
    Inserts teams data into the Teams table.

    Parameters:
    cursor (cursor): The database cursor object.
    teams_df (DataFrame): The DataFrame containing teams data.

    Returns:
    None
    """
    for index, row in teams_df.iterrows():
        cursor.execute("IF NOT EXISTS (SELECT 1 FROM Teams WHERE TeamID = ?) \
                       INSERT INTO Teams (TeamID,SportID,Name, \
                       Country,Gender) values(?,?,?,?,?)", 
                       row.team_id, row.team_id, row.sport_id, 
                       row.teamName, row.country_name, row.gender)



