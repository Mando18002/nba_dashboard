from google.cloud import storage, bigquery
import pandas as pd
import json
import io
import os
import time
from datetime import datetime, timedelta
from joblib import Parallel, delayed

def extract_meta_data(data, source_file, game_date):
    """ This functions purpose is to extract the metadata information from the nba_api resonse data.
        It captures the information such as verion numbers, request urls, timesteps, and game_dates 
        for it to be more easily queryable when cleaning the data

    Args:
    data (dict): The parsed JSON response from the NBA API containing all game data
       source_file (str): The file path/name where this data originated from (for tracking)
       game_date (str): The date of the game in YYYY-MM-DD format (extracted from file path)

    Returns:
        List: A list containing one dictionary with metadata fields, or empty list if no metadata found
    """
    # Creating empty list to collect metadata records
    records = []
    try:
        # Uses the try function to attempt to get the meta section from the api response
        # I then use the .get() to retrieve the values in order to get the value without getting an error if the key does not exist
        meta = data.get('meta')
        if meta:
            # This creates a standardized dictionaru for the metadata
            # This also flattens the nested structure into a standard format compatible with BigQuery
            record = {
                'block_type': 'meta',
                'version': meta.get('version'),
                'request': meta.get('request'),
                'time': meta.get('time'),
                'source_file': source_file,
                'game_date': game_date
            }
            # This function adds the metadata to the collection 
            # To do that we use the .append() function to add to the dictionary at the end of our records 
            records.append(record)
    except Exception as e:
        # Should an attempt fail, this function will log the error making it easier for us to find out the cause of the isse
        print("Error extracting meta: " + str(e))
    # Returns the list of the metadata records
    return records



def extract_game_header(data, source_file, game_date):
    """ This functions purpose is to extract baisc game identification information from the api_response
        
    Args:
        data (dict): The parsed JSON response from NBA API
        source_file (str): The file path where this data came from
        game_date (str): The game date in YYYY-MM-DD format
        
    Returns:
        list: List containing one dictionary with game header info, or empty list if error
    """
    # Creates an empty list to store our extracted records
    records = []
    try:
        # Navigates to the main boxscore section of the API response
        boxscore = data.get('boxScoreTraditional')
        # Checks to see if the data actually exists or not
        if boxscore:
            # If it does exist it creates a standardized dictionary with the core game identification data
            # This pulls in the basic game info that id's this specific game
            record = {
                'block_type': 'game_header',
                'game_id': boxscore.get('gameId'),
                'away_team_id': boxscore.get('awayTeamId'),
                'home_team_id': boxscore.get('homeTeamId'),
                'source_file': source_file,
                'game_date': game_date
            }
            # Adds this to the game header record to our collection
            records.append(record)
    except Exception as e:
        # If anything goes wrong, print the error so we can debug the issue
        print("Error extracting game header: " + str(e))
    # Returns a list 
    return records

def extract_team_info(data, source_file, game_date):
    """ Extracts team identification and naming infomarion for both teams in the games (home and away teams)

    Args:
        data (dict): The parsed JSON response from NBA API
        source_file (str): The file path where this data came from
        game_date (str): The game date in YYYY-MM-DD format

    Returns:
        list: List containing two dictionaries (home and away team info), or empty list if error
    """
    # Creates an empty list to sore team information 
    records = []
    try:
        # Navigates tot the main boxscore section of the API response
        boxscore = data.get('boxScoreTraditional')
        # Extracts the game ID once so we can use it for both teams playing in the same game
        game_id = boxscore.get('gameId')
        # Process HOME TEAM information first
        home_team = boxscore.get('homeTeam')
        # Creates a record with all hom team indentification information
        if home_team:
            record = {
                'block_type': 'team_info',
                'game_id': game_id,
                'team_id': home_team.get('teamId'),
                'team_city': home_team.get('teamCity'),
                'team_name': home_team.get('teamName'),
                'team_tricode': home_team.get('teamTricode'),
                'team_slug': home_team.get('teamSlug'),
                'is_home_team': True,
                'source_file': source_file,
                'game_date': game_date
            }
            # Adds home team record to our collection
            records.append(record)
        
        # Now we process the away team information
        away_team = boxscore.get('awayTeam')
        # Creates a record with all the away team's identification information
        # This is almost identical to the home team, except is_home_team = false
        if away_team:
            record = {
                'block_type': 'team_info',
                'game_id': game_id,
                'team_id': away_team.get('teamId'),
                'team_city': away_team.get('teamCity'),
                'team_name': away_team.get('teamName'),
                'team_tricode': away_team.get('teamTricode'),
                'team_slug': away_team.get('teamSlug'),
                'is_home_team': False,
                'source_file': source_file,
                'game_date': game_date
            }
            # Adds the away team record to our collection
            records.append(record)
    except Exception as e:
        # If anything goes wrong, this prints out the error so we'll be able to indentify the issue
        print("Error extracting team info: " + str(e))
    return records

def extract_team_statistics(data, source_file, game_date):
    """ Extracts complete team performance statistics for both teams in the game

    Args:
        data (dict): The parsed JSON response from NBA API
        source_file (str): The file path where this data came from
        game_date (str): The game date in YYYY-MM-DD format

    Returns:
        list: List containing two dictionaries (home and away team stats), or empty list if error
    """
    # Creats empty list to store team statistics records 
    records = []
    try:
        # Navigates to the main boxscore section of the api response
        boxscore = data.get('boxScoreTraditional')
        # Extract the game ID once so we can use it for both teams 
        game_id = boxscore.get('gameId')
        
        # Processes HOME TEAM statistics first
        home_team = boxscore.get('homeTeam')
        # Navigates to the statistics section iwhtin the home team data
        home_stats = home_team.get('statistics')
        if home_stats:
            # Creates a comprehensive record with all statistics 
            record = {
                'block_type': 'team_statistics',
                'game_id': game_id,
                'team_id': home_team.get('teamId'),
                'is_home_team': True,
                'minutes': home_stats.get('minutes'),
                'field_goals_made': home_stats.get('fieldGoalsMade'),
                'field_goals_attempted': home_stats.get('fieldGoalsAttempted'),
                'field_goals_percentage': home_stats.get('fieldGoalsPercentage'),
                'three_pointers_made': home_stats.get('threePointersMade'),
                'three_pointers_attempted': home_stats.get('threePointersAttempted'),
                'three_pointers_percentage': home_stats.get('threePointersPercentage'),
                'free_throws_made': home_stats.get('freeThrowsMade'),
                'free_throws_attempted': home_stats.get('freeThrowsAttempted'),
                'free_throws_percentage': home_stats.get('freeThrowsPercentage'),
                'rebounds_offensive': home_stats.get('reboundsOffensive'),
                'rebounds_defensive': home_stats.get('reboundsDefensive'),
                'rebounds_total': home_stats.get('reboundsTotal'),
                'assists': home_stats.get('assists'),
                'steals': home_stats.get('steals'),
                'blocks': home_stats.get('blocks'),
                'turnovers': home_stats.get('turnovers'),
                'fouls_personal': home_stats.get('foulsPersonal'),
                'points': home_stats.get('points'),
                'plus_minus_points': home_stats.get('plusMinusPoints'),
                'source_file': source_file,
                'game_date': game_date
            }
            # Adds the home team statistics record to our collection
            records.append(record)
        
        # Processes away team statistics after home team
        away_team = boxscore.get('awayTeam')
        # Navigates to the statistics section within the away team data
        away_stats = away_team.get('statistics')
        if away_stats:
            # Creates a record with all statistics
            # Basically the same thing but for the away team this time 
            record = {
                'block_type': 'team_statistics',
                'game_id': game_id,
                'team_id': away_team.get('teamId'),
                'is_home_team': False,
                'minutes': away_stats.get('minutes'),
                'field_goals_made': away_stats.get('fieldGoalsMade'),
                'field_goals_attempted': away_stats.get('fieldGoalsAttempted'),
                'field_goals_percentage': away_stats.get('fieldGoalsPercentage'),
                'three_pointers_made': away_stats.get('threePointersMade'),
                'three_pointers_attempted': away_stats.get('threePointersAttempted'),
                'three_pointers_percentage': away_stats.get('threePointersPercentage'),
                'free_throws_made': away_stats.get('freeThrowsMade'),
                'free_throws_attempted': away_stats.get('freeThrowsAttempted'),
                'free_throws_percentage': away_stats.get('freeThrowsPercentage'),
                'rebounds_offensive': away_stats.get('reboundsOffensive'),
                'rebounds_defensive': away_stats.get('reboundsDefensive'),
                'rebounds_total': away_stats.get('reboundsTotal'),
                'assists': away_stats.get('assists'),
                'steals': away_stats.get('steals'),
                'blocks': away_stats.get('blocks'),
                'turnovers': away_stats.get('turnovers'),
                'fouls_personal': away_stats.get('foulsPersonal'),
                'points': away_stats.get('points'),
                'plus_minus_points': away_stats.get('plusMinusPoints'),
                'source_file': source_file,
                'game_date': game_date
            }
            # Adds the  away team statistics to the collection
            records.append(record)
    except Exception as e:
        # Should anything go wron
        print("Error extracting team statistics: " + str(e))
    return records



def extract_team_starters(data, source_file, game_date):
    """ Extracts combined statistics for the starting lineup players of both teams.
    
    Args:
        data (dict): The parsed JSON response from NBA API
        source_file (str): The file path where this data came from
        game_date (str): The game date in YYYY-MM-DD format
    
    Returns:
        list: List containing two dictionaries (home and away starters stats), or empty list if error
    """
    # Creates a list to store team starters statistics records 
    records = []
    try:
        # Navigate to the main boxscore section of the API response
        boxscore = data.get('boxScoreTraditional')
        # Extracts the game ID once so we can use it for both teams
        game_id = boxscore.get('gameId')
        # Process HOME TEAM starters statistics first
        home_team = boxscore.get('homeTeam')
        # Navigate to the startes section which contains combined stats for the starting 5 players
        home_starters = home_team.get('starters')
        if home_starters:
            # Creates a record with combined statistics for all starting players
            record = {
                'block_type': 'team_starters',
                'game_id': game_id,
                'team_id': home_team.get('teamId'),
                'is_home_team': True,
                'minutes': home_starters.get('minutes'),
                'field_goals_made': home_starters.get('fieldGoalsMade'),
                'field_goals_attempted': home_starters.get('fieldGoalsAttempted'),
                'field_goals_percentage': home_starters.get('fieldGoalsPercentage'),
                'three_pointers_made': home_starters.get('threePointersMade'),
                'three_pointers_attempted': home_starters.get('threePointersAttempted'),
                'three_pointers_percentage': home_starters.get('threePointersPercentage'),
                'free_throws_made': home_starters.get('freeThrowsMade'),
                'free_throws_attempted': home_starters.get('freeThrowsAttempted'),
                'free_throws_percentage': home_starters.get('freeThrowsPercentage'),
                'rebounds_offensive': home_starters.get('reboundsOffensive'),
                'rebounds_defensive': home_starters.get('reboundsDefensive'),
                'rebounds_total': home_starters.get('reboundsTotal'),
                'assists': home_starters.get('assists'),
                'steals': home_starters.get('steals'),
                'blocks': home_starters.get('blocks'),
                'turnovers': home_starters.get('turnovers'),
                'fouls_personal': home_starters.get('foulsPersonal'),
                'points': home_starters.get('points'),
                'source_file': source_file,
                'game_date': game_date
            }
            # Adds the gome team starters statistics to the collection
            records.append(record)
        
        # Processes away team starters statistics after the home team starters
        away_team = boxscore.get('awayTeam')
        away_starters = away_team.get('starters')
        if away_starters:
            # Creates a record with combined statistics for all starting players
            # Basically the same as the logic block above, but just for away team starters
            record = {
                'block_type': 'team_starters',
                'game_id': game_id,
                'team_id': away_team.get('teamId'),
                'is_home_team': False,
                'minutes': away_starters.get('minutes'),
                'field_goals_made': away_starters.get('fieldGoalsMade'),
                'field_goals_attempted': away_starters.get('fieldGoalsAttempted'),
                'field_goals_percentage': away_starters.get('fieldGoalsPercentage'),
                'three_pointers_made': away_starters.get('threePointersMade'),
                'three_pointers_attempted': away_starters.get('threePointersAttempted'),
                'three_pointers_percentage': away_starters.get('threePointersPercentage'),
                'free_throws_made': away_starters.get('freeThrowsMade'),
                'free_throws_attempted': away_starters.get('freeThrowsAttempted'),
                'free_throws_percentage': away_starters.get('freeThrowsPercentage'),
                'rebounds_offensive': away_starters.get('reboundsOffensive'),
                'rebounds_defensive': away_starters.get('reboundsDefensive'),
                'rebounds_total': away_starters.get('reboundsTotal'),
                'assists': away_starters.get('assists'),
                'steals': away_starters.get('steals'),
                'blocks': away_starters.get('blocks'),
                'turnovers': away_starters.get('turnovers'),
                'fouls_personal': away_starters.get('foulsPersonal'),
                'points': away_starters.get('points'),
                'source_file': source_file,
                'game_date': game_date
            }
            # Adds the records to the collection
            records.append(record)
    except Exception as e:
        # Should an error occur a error message will show up for bebugging
        print("Error extracting team starters: " + str(e))
    return records



def extract_player_statistics(data, source_file, game_date):
    """ Extracts individual player performance statistics for all players from both teams.
        I used a for loop because we need to process multiple players (12-15 per team) and create a 
        separate record for each players performance
    
    Args:
        data (dict): The parsed JSON response from NBA API
        source_file (str): The file path where this data came from
        game_date (str): The game date in YYYY-MM-DD format
    
    Returns:
        list: List containing dictionaries for each player (typically 24-30 players total), or empty list if error
    """
    # Creates an empty list to store individual player statistics 
    records = []
    try:
        # Navigates to the main boxscore section of the API response
        boxscore = data.get('boxScoreTraditional')
        # Extracts the game id once so we can use it for all players
        game_id = boxscore.get('gameId')
        # Processes home team players first
        home_team = boxscore.get('homeTeam')
        home_team_id = home_team.get('teamId')
        # Uses a for loop to loop through each player on the home team roster 
        # By using enumerate() it gives us botht he index postion (i) and the player data
        for i, player in enumerate(home_team.get('players')):
            stats = player.get('statistics')
            record = {
                'block_type': 'player_statistics',
                'game_id': game_id,
                'team_id': home_team_id,
                'is_home_team': True,
                'player_index': i,
                'person_id': player.get('personId'),
                'first_name': player.get('firstName'),
                'family_name': player.get('familyName'),
                'name_i': player.get('nameI'),
                'player_slug': player.get('playerSlug'),
                'position': player.get('position'),
                'comment': player.get('comment'),
                'jersey_num': player.get('jerseyNum'),
                'minutes': stats.get('minutes'),
                'field_goals_made': stats.get('fieldGoalsMade'),
                'field_goals_attempted': stats.get('fieldGoalsAttempted'),
                'field_goals_percentage': stats.get('fieldGoalsPercentage'),
                'three_pointers_made': stats.get('threePointersMade'),
                'three_pointers_attempted': stats.get('threePointersAttempted'),
                'three_pointers_percentage': stats.get('threePointersPercentage'),
                'free_throws_made': stats.get('freeThrowsMade'),
                'free_throws_attempted': stats.get('freeThrowsAttempted'),
                'free_throws_percentage': stats.get('freeThrowsPercentage'),
                'rebounds_offensive': stats.get('reboundsOffensive'),
                'rebounds_defensive': stats.get('reboundsDefensive'),
                'rebounds_total': stats.get('reboundsTotal'),
                'assists': stats.get('assists'),
                'steals': stats.get('steals'),
                'blocks': stats.get('blocks'),
                'turnovers': stats.get('turnovers'),
                'fouls_personal': stats.get('foulsPersonal'),
                'points': stats.get('points'),
                'plus_minus_points': stats.get('plusMinusPoints'),
                'source_file': source_file,
                'game_date': game_date
            }
            # Adds the players statistics to the collection
            records.append(record)
        
        # Then processes away team after
        away_team = boxscore.get('awayTeam')
        # Gets the team id to associate with each away player
        away_team_id = away_team.get('teamId')
        # Loops through each player on the away team roster
        # Uses enumerate() to give both the index and the value 
        for i, player in enumerate(away_team.get('players')):
            stats = player.get('statistics')
            record = {
                'block_type': 'player_statistics',
                'game_id': game_id,
                'team_id': away_team_id,
                'is_home_team': False,
                'player_index': i,
                'person_id': player.get('personId'),
                'first_name': player.get('firstName'),
                'family_name': player.get('familyName'),
                'name_i': player.get('nameI'),
                'player_slug': player.get('playerSlug'),
                'position': player.get('position'),
                'comment': player.get('comment'),
                'jersey_num': player.get('jerseyNum'),
                'minutes': stats.get('minutes'),
                'field_goals_made': stats.get('fieldGoalsMade'),
                'field_goals_attempted': stats.get('fieldGoalsAttempted'),
                'field_goals_percentage': stats.get('fieldGoalsPercentage'),
                'three_pointers_made': stats.get('threePointersMade'),
                'three_pointers_attempted': stats.get('threePointersAttempted'),
                'three_pointers_percentage': stats.get('threePointersPercentage'),
                'free_throws_made': stats.get('freeThrowsMade'),
                'free_throws_attempted': stats.get('freeThrowsAttempted'),
                'free_throws_percentage': stats.get('freeThrowsPercentage'),
                'rebounds_offensive': stats.get('reboundsOffensive'),
                'rebounds_defensive': stats.get('reboundsDefensive'),
                'rebounds_total': stats.get('reboundsTotal'),
                'assists': stats.get('assists'),
                'steals': stats.get('steals'),
                'blocks': stats.get('blocks'),
                'turnovers': stats.get('turnovers'),
                'fouls_personal': stats.get('foulsPersonal'),
                'points': stats.get('points'),
                'plus_minus_points': stats.get('plusMinusPoints'),
                'source_file': source_file,
                'game_date': game_date
            }
            # Adds the away team statistics to the collection
            records.append(record)
    except Exception as e:
    # Should there be an error, prints out the error for bebugging
        print("Error extracting player statistics: " + str(e))
    return records



def process_single_file(storage_client, bucket_name, blob_name):
    """ This function processes a single NBA game file from Google Cloud Storage.
        It downloads the parquet file, extracts the JSON data, parses the game date,
        and runs all extraction functions to create database records.
        
        This function is kept OUTSIDE the class because joblib parallel processing
        works better with standalone functions (easier to serialize/pickle).
    
    Args:
        storage_client: Google Cloud Storage client for accessing buckets
        bucket_name (str): Name of the GCS bucket containing the game files
        blob_name (str): Full path to the specific game file to process
        
    Returns:
        list: List containing all extracted records from this game file
    """
    # Downloads the parquet from GCP into memory
    blob = storage_client.bucket(bucket_name).blob(blob_name) # Gets reference to the specific file
    buffer = io.BytesIO() # Creating a memory buffer to hold the downloaded file
    blob.download_to_file(buffer) # Download the file from GCS to memory buffer
    buffer.seek(0) # Reset buffer position into beginning so we can read from it 
    
    # Extracts the raw JSON data from the parquet file 
    parquet_dataframe = pd.read_parquet(buffer) # Reads the parquet file into the pandas df
    raw_json_string = parquet_dataframe.iloc[0]['raw_response'] # We saved the entire response as a single string in our other scripts 
    data = json.loads(raw_json_string) # From that single string we convert it back into a json dict
    
    # Parses through the game date from the file path structure 
    # The file name is just the date, so for example this would represent a game file for 2025-02-09 (year=2025/month=02/day=09/0022400752.parquet)
    # The reason why is because we need to extract the year, month, and day to create a proper date string
    parts = blob_name.split('/')
    year = None
    month = None
    day = None
    
    # This loops through each part of the file path to finc the date components 
    for part in parts:
        if part.startswith('year='):
            year = part.split('=')[1]
        elif part.startswith('month='):
            month = part.split('=')[1]
        elif part.startswith('day='):
            day = part.split('=')[1]
    
    # Creates a properly formated game date string 
    if year and month and day:
        game_date = year + "-" + month.zfill(2) + "-" + day.zfill(2)
    else:
        game_date = None
    
    # Calls ALL these functions to exact the data
    all_records = []
    all_records.extend(extract_meta_data(data, blob_name, game_date))
    all_records.extend(extract_game_header(data, blob_name, game_date))
    all_records.extend(extract_team_info(data, blob_name, game_date))
    all_records.extend(extract_team_statistics(data, blob_name, game_date))
    all_records.extend(extract_team_starters(data, blob_name, game_date))
    all_records.extend(extract_player_statistics(data, blob_name, game_date))
    # Returns ALL the stats 
    return all_records


class NBASilverProcessor:
    def __init__(self, request):
        """Initialize the NBA Silver Layer processor with all necessary configurations.
            Sets up clients for Google Cloud Storage and BigQuery, gets environment variables,
            and calculates yesterday's date for processing the most recent complete games.
        
        Args:
            request: HTTP request object from Google Cloud Functions (required by GCP)
        """
        # Get configuration from environment variables set in Google Cloud Functions
        self.storage_client = storage.Client()
        self.bigquery_client = bigquery.Client()
        self.bucket_name = os.environ.get("BOXSCORE_BUCKET_NAME")
        self.dataset_name = os.environ.get("DATASET_NAME")
        self.table_name = os.environ.get("TABLE_NAME")
        
        # Calculate yesterday's date for processing complete games
        # We process yesterday's games because NBA games can end after midnight
        # By waiting until the next day, we ensure we get complete final data
        current_time = datetime.now()
        yesterdays_time = current_time - timedelta(days=1)
        self.date_parameter = yesterdays_time.strftime("%Y-%m-%d")
        self.date_path = "year=" + yesterdays_time.strftime("%Y") + "/month=" + yesterdays_time.strftime("%m") + "/day=" + yesterdays_time.strftime("%d")

    def process_yesterdays_games(self):
        """ Process all NBA game files from yesterday using BOTH parallel and batch processing for speed.
            Uses 4 workers to process multiple game files simultaneously, automatically batching games
            into groups of 4 as workers become available. Each individual game still processes sequentially
            through all extraction functions, but multiple games run at the same time.
            
        Returns:
            list: All extracted records from all games, ready for BigQuery insertion
        """
        # Finds all the game files from yesterday in Google Cloud Storage
        # List _blobs() finds all files that start with out date path (for example: "year=2025/month=02/day=09/")
        blobs = list(self.storage_client.bucket(self.bucket_name).list_blobs(prefix=self.date_path))
        # If there are no games played yesterday it returns and empty list
        if not blobs:
            print("No files found for " + self.date_parameter)
            return []

        # Use joblib to process multiple files at the same time
        # In this case we have a maximum of 4 games processing at the same time
        # For example if we have 10 games, joblin will automatically create batches and manage the workers
        # To illustrate, lets say this script was running on April 4th 2025 which has 10 games, what it would do is split the games into
        # Groups of 3 (G1=4 games, G2=3 games, G3=3 games) and process the batch one by one
        parallel_results = Parallel(n_jobs=4, prefer="processes")(
            delayed(process_single_file)(self.storage_client, self.bucket_name, blob.name)
            for blob in blobs
        )
        
        # Combine all results from parallel processing into one big list
        # Since the results contain separate list for each each we need to merge them 
        all_records = []
        for i, records in enumerate(parallel_results):
            all_records.extend(records)
            print("  " + blobs[i].name + ": " + str(len(records)) + " total records")
        return all_records
        


    def insert_to_bigquery(self, data):
        """ Insert processed NBA data into BigQuery table using chunked batch processing.
            Splits large datasets into smaller pieces (10,000 records each) to avoid memory issues,
            timeouts, and BigQuery insertion limits. Uses sequential processing for database insertion
            to prevent conflicts and ensure data integrity.
            
            Args:
                data (list): List of dictionaries containing all processed NBA records from all games
                            Each dictionary represents one record (player stats, team stats, etc.)
                
            Why Chunking:
                - BigQuery has limits on insertion size and can timeout with large uploads
                - Memory usage stays manageable with smaller batches
                - If one chunk fails, other chunks can still succeed
                - Progress tracking is easier with smaller pieces
        """
        # Checks to see if we even have data to insert, if not that exits
        if not data:
            print("No data to insert into BigQuery")
            return
        # This sets up the settings for data inseration and tells BigQuery how to deal with incoming data
        config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",       # Add new data to existing table
            autodetect=True,                        # Let BigQuery automatically figure out column types and schema
            create_disposition="CREATE_IF_NEEDED"   # Creates the table if it doesnt exist already
        )
        
        # Builds the full table reference path
        # Creates the complete BigQuery table path in format: "project_id.dataset_name.table_name"
        table_ref = self.bigquery_client.project + "." + self.dataset_name + "." + self.table_name
        # Set up chunking strategy to break large datasets into manageable pieces
        chunk_size = 10000
        total_records = len(data)
        
        # Process data in chunks using sequential batch processing
        # We use sequential (not parallel) processing for BigQuery insertion to avoid database conflicts
        # range(start, stop, step) creates batches: 0-9999, 10000-19999, 20000-29999, etc.
        for i in range(0, total_records, chunk_size):
            # Extract one chunk of data (up to 10,000 records)
            chunk = data[i:i+chunk_size]
            try:
                # Converts the chunk into a DataFrame to insert into bigquery
                dataframe = pd.DataFrame(chunk)
                job = self.bigquery_client.load_table_from_dataframe(dataframe, table_ref, job_config=config)
                job.result()
            except Exception as e:
                print("  Failed to insert chunk ")

    def run(self):
        """ Execute the complete NBA Silver Layer processing workflow.
            This is the main orchestrator function that coordinates the entire data pipeline:
            1. Process all game files from yesterday using parallel + batch processing
            2. Insert all extracted data into BigQuery using sequential chunked processing
            3. Return a summary of what was accomplished
            
            This function acts as the "conductor" - it doesn't do the actual work itself,
            but manages the order and flow of operations to ensure everything happens
            in the right sequence.
            
        Returns:
            str: Summary message indicating success status and total records processed
                Examples: "Processed 1,247 total records from 2025-02-08"
                        "No games found to process"
                        
        Workflow:
            File Processing (Parallel) → Data Validation → Database Insertion (Sequential) → Summary
        """
        # Process all game files from yesterday using parallel and batch processing
        # This function call triggers the entire file processing pipeline:
            # - Finds all parquet files from yesterday's games
            # - Downloads and processes multiple files simultaneously using 4 workers
            # - Extracts all NBA data (player stats, team stats, game info, etc.)
            # - Returns a combined list of all records from all game
        records = self.process_yesterdays_games()
        # Validate that we actually found games to process
        # If no games were played yesterday or all file processing failed,
        # we exit early rather than trying to insert empty data
        if not records:
            return "No games found to process"
        # Step 4: Return summary statistics showing what was accomplished
        # Provides feedback about the total number of records successfully processed
        self.insert_to_bigquery(records)
        return "Processed " + str(len(records)) + " total records from " + self.date_parameter



def run_silver_layer(request):
    processor = NBASilverProcessor(request)
    result = processor.run()
    return result, 200