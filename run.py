from crawler import Crawler
from aws_s3 import AwsS3

endpoint_matches_report = 'https://api.tracker.gg/api/v2/valorant/standard/matches/riot/RayzenSama%236999?type=competitive&next='
tag = '//pre'
path_matches_report = '/raw/trackergg/matches_report/'
file_name_matches_report = 'matches_report_RayzenSama'

matches_report = Crawler.get_matches_report(endpoint_matches_report, tag)

# AwsS3.upload_file(matches_report, path_matches_report, file_name_matches_report)