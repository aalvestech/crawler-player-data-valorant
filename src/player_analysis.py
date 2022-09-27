from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime
import pandas as pd
import requests
import json
from datetime import date, datetime
import boto3
from io import StringIO
from dotenv import load_dotenv
import os
import time


load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")



s3_client = boto3.client('s3')

response = s3_client.get_object(Bucket = AWS_S3_BUCKET, Key = 'raw/tracker_player_rayzem_2022-09-26 17:54:44.873258.json')

data = response['Body'].read()

data_str = data.decode('utf-8')
data_json = json.loads(data_str)


expiryDate : str = data_json["data"]["expiryDate"]
requestingPlayerAttributes : dict = data_json["data"]["requestingPlayerAttributes"]
paginationType : str = data_json["data"]["paginationType"]
metadata : dict = data_json["data"]["metadata"]
matches : list = data_json["data"]["matches"]

data = []

for match in matches:
    # data
    attributes : dict = match["attributes"]
    match_metadata : dict = match["metadata"]
    expiryDate : str = match["expiryDate"]
    
    # more data
    segments : list = match["segments"]
    for segment in segments:
        # data
        segment_type: str = segment["type"]
        attributes : dict = segment["attributes"]
        segment_metadata : dict = segment["metadata"]
        expiryDate : str = segment["expiryDate"]
        
        # more data
        stats : dict = segment["stats"]
        for k, v in stats.items():
            row = {}

            row.update(attributes)
            row.update(match_metadata)
            row["expiryDate"] = expiryDate

            row["segment_type"] = segment_type
            row.update(attributes)
            row.update(segment_metadata)
            row["expiryDate"] = expiryDate

            row["stat"] = k
            row.update(v)

            data.append(row)

len(data)

columns = data[0].keys()

df = pd.DataFrame(data, columns=columns)

df

