from aws_s3 import AwsS3
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import time

class Crawler():
    
    def get_matches_report() -> str:

        options = webdriver.ChromeOptions()
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)

        path_write = 'raw/trackergg/matches_report/'


        for page in range(0,10):
            
            driver.get('https://api.tracker.gg/api/v2/valorant/standard/matches/riot/RayzenSama%236999?type=competitive&next={}'.format(page))
            
            data_pre = driver.find_element('xpath', '//pre').text

            time.sleep(5)

            file_format = '.txt'
            
            AwsS3.upload_file(data_pre, path_write, file_format)

        driver.quit()

        return data_pre