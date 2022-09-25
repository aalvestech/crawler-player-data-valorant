from datetime import datetime
import pandas as pd
import requests
import json
from datetime import date, datetime
import boto3
from io import StringIO
from dotenv import load_dotenv, find_dotenv
import os

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")

endpoint_matches = 'https://val.dakgg.io/api/v1/accounts/cqD-BjmHUWpiOIsk53scYDHVYl9KxnenL5J11czMRccVPsj0qwd3MifxF4PH_4su3NyF0dAff-VQJQ/matches'
file_name = 'tracker_player_rayzem' 


def upload_s3(data, file_name):

    time = datetime.today()
    file_name = file_name + '_{}.parquet'
    file_name = file_name.format(time)
    data = data.to_parquet()

    s3 = boto3.client("s3", aws_access_key_id = AWS_ACCESS_KEY_ID, aws_secret_access_key = AWS_SECRET_ACCESS_KEY)
    # data = StringIO()
    # data = data.to_json(data)
    # data.seek(0)    
    s3.put_object(Bucket = AWS_S3_BUCKET, Body = data, Key = 'raw/' + file_name)


def _get(endpoint : str) -> json:

    session = requests.Session()
    response = session.get(endpoint)
    data = response.json()

    return data


def get_player_stats(endpoint : str) -> pd.DataFrame:
    
    data = _get(endpoint)
    data = data['matches']
    data = json.dumps(data)
    df_profile = pd.read_json(data)
    df_profile = pd.json_normalize(json.loads(df_profile.to_json(orient='records')))
    
    df_player_stats = df_profile[[
        'matchInfo.shard','matchInfo.matchId', 'matchInfo.mapName', 'matchInfo.isRanked',  
        'matchInfo.queueId', 'matchInfo.gameLengthMillis', 'matchInfo.gameStartMillis',
        
        'stat.roundsPlayed', 'stat.roundsWon', 'stat.bombPlants',
        'stat.bombDefuses', 'stat.damage', 'stat.headshots',
        'stat.bodyshots', 'stat.legshots', 'stat.firstBloods',
        'stat.ceremonyAces', 'stat.ceremonyClosers',
        'stat.ceremonyClutches', 'stat.ceremonyFlawlesses',
        'stat.ceremonyTeamAces', 'stat.ceremonyThrifties',

        'player.puuid', 'player.gameName', 'player.characterName',
        'player.tagLine', 'player.characterCode',
        'player.competitiveTier', 'player.stats.kills',
        'player.stats.score', 'player.stats.roundsPlayed',
        'player.teamId', 'player.stats.deaths',
        'player.stats.assists', 'player.stats.abilityCasts.grenadeCasts',
        'player.stats.abilityCasts.ability1Casts',
        'player.stats.abilityCasts.ability2Casts', 
        'player.stats.abilityCasts.ultimateCasts',
        ]]

    return df_player_stats

# print(get_player_stats(endpoint_matches))

upload_s3(data = get_player_stats(endpoint_matches), file_name = file_name)