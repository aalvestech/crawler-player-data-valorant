from crawler import Crawler
from aws_s3 import AwsS3
from data_cleaner import DataCleaner


Crawler.get_matches_report()
Crawler.get_gun_report()
DataCleaner.data_cleaner_matches()
DataCleaner.data_cleaner_guns()
