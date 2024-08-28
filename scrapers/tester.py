import pandas as pd
import json
import nltk
import requests
from requests_html import HTMLSession
from bs4 import BeautifulSoup
from nltk.tokenize import word_tokenize
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
from datetime import date
nltk.download('punkt')


# start sessiona and go to URL
session = HTMLSession()
response = session.get("https://www.yelp.com/biz/duckfat-portland?osq=Restaurants&sort_by=date_desc")
soup = BeautifulSoup(response.text, "html.parser")

# declare results container
res_data_dict = {}

# get the restaurant name
name = soup.find("h1", class_ = "y-css-olzveb")
res_name = name.text.strip()
res_data_dict["name"] = res_name


# get the restuarant price point, if there is one
container = soup.find_all("span", class_ = "y-css-tqu69c")
if len(container) == 1:
    res_data_dict["price_point"] = None

for index, object_ in enumerate(container):
    if index == 0:
        continue
    price_point = object_.find("span", class_  = "y-css-33yfe")
    price_point = price_point.text.strip()
    res_data_dict["price_point"] = price_point

# get the restaurant tags
tags_container_a = soup.find("span", class_ = "y-css-1w2z0ld")
tags_container_b = tags_container_a.find_all("span", class_ = "y-css-kw85nd")
tags_list = []
for tags in tags_container_b:
    tag = tags.find("a", class_ = "y-css-12ly5yx")
    tag = tag.text.strip()
    tags_list.append(tag)
res_data_dict["tags"] = tags_list
print(res_data_dict)
