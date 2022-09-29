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

# endpoint_matches = 'https://api.tracker.gg/api/v2/valorant/standard/matches/riot/RayzenSama%236999?type=competitive&next={}'
# endpoint_matches = 'https://api.tracker.gg/api/v2/valorant/standard/matches/riot/RayzenSama%236999?type=competitive'
file_name = 'tracker_player_rayzem'


def upload_s3(data, file_name):

    time = datetime.today()
    file_name = file_name + '_{}.json'
    file_name = file_name.format(time)
    

    s3 = boto3.client("s3", aws_access_key_id = AWS_ACCESS_KEY_ID, aws_secret_access_key = AWS_SECRET_ACCESS_KEY)

    s3.put_object(Bucket = AWS_S3_BUCKET, Body = data, Key = 'raw/' + file_name)


# def get_player_stats(endpoint : str) -> json:
def get_player_stats() -> json:

    
    options = webdriver.ChromeOptions()
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)

    for page in range(0,10):
        
        driver.get('https://api.tracker.gg/api/v2/valorant/standard/matches/riot/RayzenSama%236999?type=competitive&next={}'.format(page))
        data_pre = driver.find_element('xpath', '//pre').text
        upload_s3(data_pre, file_name=file_name)
        
        time.sleep(5)

    driver.quit()

    # return data_pre


# upload_s3(data=get_player_stats('https://api.tracker.gg/api/v2/valorant/standard/matches/riot/RayzenSama%236999?type=competitive&next={}'), file_name=file_name)

get_player_stats()