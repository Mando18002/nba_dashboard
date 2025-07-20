from datetime import datetime, timedelta
from google.cloud import storage
import pandas as pd
import json
import requests
import io
import os
import time

def get_nba_scoreboard(request):
    """ This function acts as the Cloud Functions entry point to extract NBA data from the API and store it in GCS.
        It extracts yesterday's scoreboard data, loads it into a pandas dataframe, converts it into a Parquet format,
        and then stores it in Google Cloud Storage with a paritioned path structure for organization and incremental loading.
        
        This function also uses residential proxies in order to bypass the API's restrictions on cloud services, thus not inccuring
        request timeout errors.
        
        ARGS:
            request (flask.Request): The HTTP object is automatically provided by Google Step Functions when called, and the 
            parameter is required by the Google Functions framework
        
        RETURNS:
            - A string indicating a success or error message
        
        ENV VAR:
            - PROXY_USERNAME: Username for the SmartProxy service 
            - PROXY_PASSWORD: Password for the SmartProxy service 
            - PROXY_PORT: Port for the SmartProxy service 
            - BUCKET_NAME: GCS bucket name 
    """
    
    # Setting up proxy configuration to bypass API's restrictions on requests from cloud services
    username = os.environ.get("PROXY_USERNAME")
    password = os.environ.get("PROXY_PASSWORD")
    port = os.environ.get("PROXY_PORT")
    proxy = f"http://{username}:{password}@gate.smartproxy.com:{port}"
    proxies = {
        "http": proxy,
        "https": proxy
    }
    
    # Creates a dictionary of HTTP headers to mimic a real browers request, rather than an automated one (CloudServices)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Referer": "https://www.nba.com/",
        "Origin": "https://www.nba.com",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive"
    }
    
    # Setting up time function to capture yesterday's NBA games
    # If we capture data while a game is playing, we receive an JSON response filled with zeros 
    # NBA games in rare cases finish late into the night (1:00 am sometimes), thus we run this function at 2:00 am with the date set at yesterday we get complete data
    # For example if we wanted to get April 22nd's game and ran the code at 11:59 pm that same day, if the games finish late (12:30 am on the 23rd) you would get an empty JSON
    # Thus we set the day to yesterday, and run the script at 3:00 am, to ensure we get the complete data on the games
    current_time = datetime.now()
    yesterdays_time = current_time - timedelta(days=1)
    date_parameter = yesterdays_time.strftime("%Y-%m-%d")
    
    # Setting up paritioning logic when saving the response from the API
    # Extracts the date compenents as zero padded strings in order to build a heirarchial file path structure
    year = yesterdays_time.strftime("%Y")
    month = yesterdays_time.strftime("%m")
    day = yesterdays_time.strftime("%d")
    
    # Setting up retry logic for making requests to the API should the request fail
    max_retries = 3 # Maximum retries allowed
    retry_count = 0 # Current retry count
    retry_delay = 2 # Delay inbetween retries if needed
    
    # The while loop continues until the condition is false
    # The loop is TRUE when the retry count is less than 3, and turns false when the retry_count reaches 3
    # If the request is successful, the loop will break
    while retry_count < max_retries:
        try:
            scoreboardv2_url = "ENDPOINT_URL"
            parameters = {"DayOffset": "0","GameDate": date_parameter,"LeagueID": "00"}
            reponse = requests.get(scoreboardv2_url,headers=headers, params=parameters, proxies=proxies)
            scoreboard_data = reponse.json()
            break    
        # If the request fails the code jumps to the except block where the request to handle the error
        # After error handling the loop continues and attempts the request again
        # Each failed attempt increases retry_count by 1 until it reaches a maximum of 3 
        # When this happens the condition in the while loop becomes false, thus exiting the loop                                              
        except Exception as e:
            print(f"Request failed (attempt {retry_count + 1}/{max_retries}): {str(e)}")
            retry_count += 1
            time.sleep(retry_delay * retry_count)
    
    # Checks to see if we have exhasued all retry attempts
    # Should all three attempts fail, prints error message noting the error
    if retry_count >= max_retries:
        error_message = "ERROR: Failed to get NBA scoreboard data after 3 attempts"
        print(error_message)
        return error_message
    
    # Should retry_count be less than the max_retries (meaning we were successful in extracting the data) we then move on to storing the data
    if retry_count < max_retries:
        # Creates a pandas DataFrame 
        # Due to the reponse being heavily nested, I turned the response into a long line of text string that contains everything
        bronze_layer_df = pd.DataFrame([{
        "raw_response": json.dumps(scoreboard_data)
        }])

        # Logic block to convert dataframe into a Parquet file format
        parquet_buffer = io.BytesIO()
        bronze_layer_df.to_parquet(parquet_buffer, engine="pyarrow", compression="snappy", index=False)
        parquet_buffer.seek(0)

        # Setting up configuration for Google Cloud Storage path info
        bucket_name = os.environ.get("BUCKET_NAME")
        partition_path = f"year={year}/month={month}/day={day}/scoreboard.parquet"

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(partition_path)
        blob.upload_from_file(parquet_buffer, content_type="application/octet-stream")
        print(f"Successfully uploaded to gs://{bucket_name}/{partition_path}")
    else:
        print("Could not proceed with data upload due to API failure")