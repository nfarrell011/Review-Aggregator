import requests
from requests_html import HTMLSession
from bs4 import BeautifulSoup
import pandas as pd
import json
import nltk
from nltk.tokenize import word_tokenize
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
nltk.download('punkt')

################################################### Helpers ##########################################################
def incremental_scroll(driver):
    """
        This scroll slowly down the page allowing all the elements to load.

        Args:
            driver: the driver object instantiated with the url
        
        Returns:
            None
    """
    last_height = driver.execute_script("return document.body.scrollHeight")
    
    while True:

        # scroll down by a fraction of the page height
        for i in range(1, 11):  # this will used to divide the page into 10 parts
            driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight*{i/10});")
            time.sleep(5)  # this will wait after each scroll to allow load time

        # wait for the page to load completely
        time.sleep(3)

        # check if the height of the page has changed
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

def get_restaurant_urls(path_to_OpenTable_base_page):
    """ 
        This will get the individual restaurant urls from the openTable homepage.

        Args:
            path_to_OpenTable_base_page: (str) - the url to OpenTable

        Returns:
            hrefs: (list) - a list of urls to the various restaurants on the home page.
    """

    # set up ChromeDriver
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service)

    # define the driver
    driver.get(path_to_OpenTable_base_page)

    # this will scroll down page so that all the elements load
    incremental_scroll(driver)

    # this will find all div elements with a specific class, then find 'a' elements within those divs
    restaurant_cards = driver.find_elements(By.XPATH, "//a[contains(@class, 'qCITanV81-Y-')]")
    hrefs = []
    for card in restaurant_cards:
        href = card.get_attribute('href')
        hrefs.append(href)

    driver.quit()
    return hrefs

url = f"https://www.opentable.com/s?dateTime=2024-05-10T17%3A00%3A00&covers=2&metroId=3569&regionIds%5B%5D=10734&neighborhoodIds%5B%5D=&term=&shouldUseLatLongSearch=false&originCorrelationId=a860a793-437a-4bed-b01b-3a9afc98fdcf"

