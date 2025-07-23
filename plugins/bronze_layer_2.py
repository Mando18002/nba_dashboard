from datetime import datetime, timedelta
from google.cloud import storage
import pandas as pd
import json
import requests
import io
import os
import time
from joblib import Parallel, delayed

from constants import username, password, port, bucket_name


# Creating a class to encapsualte the logic to process data from the BoxscoreTraditionalV3 endpoint
class NBABoxscoreProcessor:
    # We use the special class __init__ in order to avoid a pickling problem when using joblib when trying to do parallel processing
    # If we had used regular functions, joblib would have to "pickle" (serialize) our configuration data like our proxies and GCS clients and send it to each parallel process
    # This as a result causes crashing because GCS clients cant be pickled
    # To solve this issue what we did was create a class, and set all configurations in __init__ so when joblib runs parallel processesing each process get the configured settings
    def __init__(self, request):
        """ This function sets up the script with the neccessary configurations which include
            proxy settings, headers, time & date, and storage locations in GCP

        Args:
            request: The HTTP request object is from cloud functions and isnt used directly, but
            needed for GCP compatiability
        
        ENVIROMENT VARIABLES:
            - PROXY_USERNAME: Username for SmartProxy service
            - PROXY_PASSWORD: Password for SmartProxy service
            - PROXY_PORT: Port for SmartProxy service
            - INPUT_BUCKET: GCS bucket name containing scoreboard data
            - OUTPUT_BUCKET: GCS bucket name for storing boxscore data
        """
        username = os.environ.get("PROXY_USERNAME")
        password = os.environ.get("PROXY_PASSWORD")
        port = os.environ.get("PROXY_PORT")
        proxy = f"http://{username}:{password}@gate.smartproxy.com:{port}"
        self.proxies = {"http": proxy, "https": proxy}
        
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Referer": "https://www.nba.com/",
            "Origin": "https://www.nba.com",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive"
        }
        
        # Setting time to yesterday, using logical paritioning to processes only yesterdays file from input bucket
        yesterday = datetime.now() - timedelta(days=1)
        self.date_path = f"year={yesterday.strftime('%Y')}/month={yesterday.strftime('%m')}/day={yesterday.strftime('%d')}"
        
        # GCP Bucket configuration
        self.client = storage.Client()
        self.input_bucket = os.environ.get("INPUT_BUCKET")
        self.output_bucket = os.environ.get("OUTPUT_BUCKET")
        
        
        
    def get_game_ids(self):
        """ This functions purpose is to look through the GCP bucket that contains data from the ScoreboardV2 endpoint, in
            order to obtain the game_ids (unique identifier) of all games played that day, which is a parameter needed for the 
            BoxScoreTraditionalV3 endpoint
            
            The flow of data:
                1) Downloads the ScoreboardV2 parquet from GCP input bucket and puts it into memory using BytesIO() for faster reads than disk
                2) Extracts the raw JSON response from the parquet file and locates the "GameHeader" section which contains data we need
                3) Finds the specific column that contains the game_ids and then returns them as a list to be used later on
        
        Returns:
            - A list full of game_ids (or games played) from yesterday 
        """
        # Creates an empty space in memory, and then downloads the parquet from the bucket to this memory
        buffer = io.BytesIO()
        self.client.bucket(self.input_bucket).blob(f"{self.date_path}/scoreboard.parquet").download_to_file(buffer)
        buffer.seek(0)
        
        # Unpacking the nested data structure to get JSON content
        # Reads through the file and gets the first row of the dataframe
        # Gets the "raw_response" which is the column where the string of data was saved on in the first script
        # Converts that JSON string back into a dictionary so we can navagate it 
        json_data = json.loads(pd.read_parquet(buffer).iloc[0]['raw_response'])
        game_header = next((r for r in json_data["resultSets"] if r["name"] == "GameHeader"), None)
        
        # The nba_api returns multiple sections, so we need "GameHeader" which has the data we want
        # In GameHeader we extract all the game_ids and return a list to be used later on
        if game_header:
            game_id_index = game_header["headers"].index("GAME_ID")
            return [row[game_id_index] for row in game_header["rowSet"]]
        return []
        
        
        
    def batch_config(self):
        """ This function sets up how we are going to process each NBA game and it goes as follows:
                1) Retrieves the game IDs from the list created from the "get_game_ids" function
                2) Organizes the games into batches up to a maximum of 4
                3) Runs each batch in parallel for faster processing

        Returns:
            string: Message indicating success
        """
        # Gets the list of game_ids we extracted from the "get_game_ids" function
        game_ids = self.get_game_ids()
        print(f"Found {len(game_ids)} games to process")
        
        # Split the game ID's into smaller groups (batches of 4) each
        # For example, if we had 10 games playing yesterday, this would create 3 batches: [4 games], [4 games], [2 games]
        batch_size = 4
        game_batches = [game_ids[i:i+batch_size] for i in range(0, len(game_ids), batch_size)]
        print(f"Processing {len(game_batches)} batches with {batch_size} games per batch using 4 workers")
        
        # We then set up parallel processing jobs - one job per batch
        # Each batch will be processed by a separate core at the same time
        # We delay to prepare the work instructions, so when we use joblib the processes execute all at once 
        results = [
            delayed(self.process_game_batch)(batch_index, batch)
            for batch_index, batch in enumerate(game_batches)
        ]
        
        # Calls for the parallel processing 
        Parallel(n_jobs=4, prefer="processes")(results)
        return "Processing complete"
        
        
        
    def process_game_batch(self, batch_id, game_batch):
        """ This functions purpose is to set up a session to reuse the same HTTP connection when were extracting game data
            from the endpoint. Without this function, everytime we're done processing a game we would have to establish a new
            connection to the endpoint costing us time

        Args:
            batch_id (int): The number used to identify which batch this is (mainly used for logging and tracking purposes)
            game_batch (list): A list containing the game IDs that need to be processed in this specific batch

        Returns:
            type: int: The total number of games that were processed in this batch
        """
        session = requests.Session()
        for game_id in game_batch:
            time.sleep(0.1)
            self.process_game_with_retry(session, game_id)
        return len(game_batch)
    
    
    
    def process_game_with_retry(self, session, game_id):
        """ This function uses the game_ids as a parameter for the endpoint, and then gets NBA game data from the endpoint
            BoxscoreTraditionalV3 (endpoint that has the data we need), which is then turned into a parquet and sent to 
            the output GCP bucket. This function also has retry logic in order to attempt to extract the data should the 
            process fail

        Args:
            session (requests.Session): The HTTP session object for reusing connections
            game_id (str): The unique identifier for the specific NBA game to process

        Returns:
            bool: True if the game data was successfully fetched and stored, False if all retry attempts failed
        """
        
        # Setting up retry logic for making requests to the API should the request fail
        max_retries = 3  # Maximum retries allowed
        retry_count = 0  # Current retry count
        retry_delay = 3  # Delay between retries if needed
        
        # The while loop continues until the condition is false
        # The loop is TRUE when the retry count is less than 3, and turns false when the retry_count reaches 3
        # If the request is successful, the loop will break immediately
        while retry_count < max_retries:
            try:
                # This part makes the HTTP request to the endpoint with the necessary parameters and proxies to extract the data
                response = session.get(
                    "ENDPOINT_URL", 
                    headers=self.headers, 
                    params={"GameID": game_id, "LeagueID": "00"}, 
                    proxies=self.proxies, 
                    timeout=30
                )
                response.raise_for_status()
                
                # If the request is successful, then this block will create a dataframe for our data
                # Also storing game_id for tracking purposes
                bronze_layer_df = pd.DataFrame([{
                    "raw_response": json.dumps(response.json())
                }])

                # Logic block to convert dataframe into a Parquet file format
                parquet_buffer = io.BytesIO()
                bronze_layer_df.to_parquet(parquet_buffer, engine="pyarrow", compression="snappy", index=False)
                parquet_buffer.seek(0)
                
                # Uploads the parquet file into the GCP bucket 
                blob = self.client.bucket(self.output_bucket).blob(f"{self.date_path}/{game_id}.parquet")
                blob.upload_from_file(parquet_buffer, content_type="application/octet-stream")
                
                # Success! Break out of retry loop
                break
                
            # If the request fails, code jumps to the except block to handle the error
            # After error handling, the loop continues and attempts the request again
            # Each failure increases retry_count by 1 until it reaches 3
            # When retry_count reaches 3, the while condition becomes false and exits the loop
            except Exception as e:
                print(f"Game {game_id}: Error on attempt {retry_count + 1}/{max_retries}: {str(e)}")
                retry_count += 1
                time.sleep(retry_delay * retry_count)
        
        # Check if we have exhausted all retry attempts
        if retry_count >= max_retries:
            error_message = f"ERROR: Failed to get game {game_id} data after {max_retries} attempts"
            print(error_message)
            return False
        
        # If we get here, the request succeeded
        return True
    
    
    
def get_nba_boxscores(request):
    result = NBABoxscoreProcessor(request).batch_config()
    return result, 200