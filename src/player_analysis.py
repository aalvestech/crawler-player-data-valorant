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

# df_profile = pd.read_json(StringIO(data.decode('utf-8')))
df_profile = pd.read_json(StringIO(data.decode('utf-8')))

df_profile = pd.json_normalize(json.loads(df_profile.to_json(orient='records'))).explode('data')
df_profile = df_profile['data']
df_profile = pd.json_normalize(json.loads(df_profile.to_json(orient='records')))
df_profile = pd.json_normalize(json.loads(df_profile.to_json(orient='records'))).explode('segments')
df_profile = pd.json_normalize(json.loads(df_profile.to_json(orient='records'))).explode('segments')


# df_profile = pd.json_normalize(json.loads(df_profile.to_json()))
# # df_profile = df_profile['data.matches']
# df_profile = pd.json_normalize(json.loads(df_profile.to_json(orient='records')))
# df_profile = pd.json_normalize(json.loads(df_profile.to_json(orient='records'))).explode('data.expiryDate')
# df_profile = pd.json_normalize(json.loads(df_profile.to_json(orient='records'))).explode('data.matches')
# df_profile = pd.json_normalize(json.loads(df_profile.to_json(orient='records'))).explode('data.metadata.schema')



# df_profile = pd.json_normalize(json.loads(df_profile.to_json(orient='records'))).explode('data.matches.segments')







# print(df_profile.columns)


print(df_profile)