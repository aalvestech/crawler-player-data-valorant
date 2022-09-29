from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import json
import time

class Crawler():

    def get_matches_report(endpoint : str, tag : str) -> json:

        options = webdriver.ChromeOptions()
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)

        for page in range(0,9):
            
            endpoint = endpoint+{}
            endpoint = endpoint.format(page)
            driver.get(endpoint)
            data_pre = driver.find_element('xpath', tag).text
            time.sleep(5)

        driver.quit()
            
        return data_pre
