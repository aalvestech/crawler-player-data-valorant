from datetime import datetime
import pandas as pd
import json
from datetime import date, datetime
import boto3
from dotenv import load_dotenv
import os
import time


load_dotenv()


AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")

class DataCleaner():
    
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket = AWS_S3_BUCKET, Key = 'raw/trackergg/matches_report/tracker_player_rayzem_2022-09-29 03:05:33.430012.json')
    data = response['Body'].read()


    data_str = data.decode('utf-8')
    data_json = json.loads(data_str)

    path_read = 'raw/trackergg/matches_report/'
path_write = 'cleaned/trackergg/matches_report/'
s3 = boto3.resource('s3')
bucket = s3.Bucket(AWS_S3_BUCKET)
files = bucket.objects.filter(Prefix=path_read)
files = list(files)
del files[0]

df_aux = pd.DataFrame()
for file in files:
    file = file.key

    response = s3_client.get_object(Bucket = AWS_S3_BUCKET, Key = file)
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
            stat_dict = {}
            stats : dict = segment["stats"]
            for stat, stat_data in stats.items():
                stat_keys = stat_data.keys()
                stat_columns = [f'{stat}_{col}' for col in stat_keys]
                stat_values = stat_data.values()
                _stat_dict = {k: v for k, v in zip(stat_columns, stat_values)}
                stat_dict.update(_stat_dict)

            row = {}
            row.update(attributes)
            row.update(match_metadata)
            row["expiryDate"] = expiryDate

            row["segment_type"] = segment_type
            row.update(attributes)
            row.update(segment_metadata)
            row["expiryDate"] = expiryDate

            row.update(stat_dict)

            data.append(row)

    
    columns = data[0].keys()
    df = pd.DataFrame(data, columns=columns)
    df_aux = pd.concat([df_aux, df], axis = 0)



date = datetime.now().strftime("_%Y%m%d_%H%M%S")
file_name = 'matches_result_rayzensama{}{}'.format(date, '.csv')
input = path_write + file_name



df_final = pd.concat([df_aux, df_aux['rank_metadata'].apply(pd.Series)], axis=1)

data_final = df_final.to_csv()


s3_client.put_object(Bucket = AWS_S3_BUCKET, Body = data_final, Key = input)  
  