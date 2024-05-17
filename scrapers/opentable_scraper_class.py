"""
Review Aggregator
Joseph Nelson Farrell
05-14-24

This file contains a class that will scrape OpenTable for review data.
"""
# packages
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

# scraper class
class OpenTableScraper():
    """ 
    This class will scrape OpenTable for review data. It is designed to start on a regions "Book for ..." 
    "view all" page.

    It works in two phases, first it will grab all the restaurant links on the "base_url".
    Second, it will visit each restaurant link and grab the most recent 20 pages of reviews.
    """

    def __init__(self, base_url) -> None:
        """
        OpenTableScraper initializer.

        Args:
            base_url: (str) - This is where the scraper will start. It should be a page that lists restaurant links.

        Attributes:
            hrefs: (list) - This is a list of individual restaurant links that are extracted by the first phase of the
                scraper.
            base_url: (str) - This is where the scraper will start. It should be a page that lists restaurant links.
            service: (Service) - This is where you set the link to your internet driver, here it's a chromedriver. This would
                have to be changed if this class were used on another machine.
            driver: (webdriver.Chrome) - This is the actual driver.
            results_list: (list) - This will be a list of dicts where each dict is the data of single review.
        """
        self.service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service = self.service)
        self.base_url = base_url
        self.hrefs = None
        self.results_list = []

    def incremental_scroll(self):
        """
        Scroll slowly down the page allowing all the elements to load.
        """
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        
        while True:

            # scroll down by a fraction of the page height
            for i in range(1, 11):  # this will used to divide the page into 10 parts
                self.driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight*{i/10});")
                time.sleep(5)  # this will wait after each scroll to allow load time

            # wait for the page to load completely
            time.sleep(3)

            # check if the height of the page has changed
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

    def get_restaurant_urls(self):
        """ 
        Get the individual restaurant urls from the OpenTable homepage.
        """
        # define the driver
        self.driver.get(self.base_url)

        # this will scroll down page so that all the elements load
        self.incremental_scroll()

        # this will find all div elements with a specific class, then find 'a' elements within those divs
        restaurant_cards = self.driver.find_elements(By.XPATH, "//a[contains(@class, 'qCITanV81-Y-')]")
        hrefs_list = []
        for card in restaurant_cards:
            href = card.get_attribute('href')
            hrefs_list.append(href)

        self.hrefs = hrefs_list
        self.driver.quit()
        return None

    def get_total_pages_for_restaurant(self, url):
        """
        Get the number of review pages for an individual restaurant.

        Args:
            url: (str) - The url to an individual restaurant's OpenTable page.

        Returns:
            int - The number of review pages.
        """
        # start html session
        session = HTMLSession()

        # access the url
        response = session.get(url)

        # instantiate Soup object
        soup = BeautifulSoup(response.text, "html.parser")
        
        # find all the 'scripts'
        soup_find_total_page = soup.find_all('script')

        # token the words in the second last script
        words = word_tokenize(str(soup_find_total_page[-2]))

        # this will find the total pages
        l = []
        flag = 0
        for i in words:
            if i == 'totalPages':
                flag = 1
            if flag == 1:
                l.append(i)
        total_pages = int(l[2].replace(":", ""))
        return total_pages

    def grab_review_data(self, review):
        """
        This extact the data from a "review" object taken from the HTML

        Args:
            review: (bs4.element.tag) - A review of object taken from a restaurant HTML.

        Returns:
            dict - A dict containing the extracted data.
        """

        # this extracts the hometown of the reviewer
        reviewer_hometown = review.find('p', class_ = 'POyqzNMT21k- C7Tp-bANpE4-').text

        # this extracts when the review was made
        review_datelike = review.find('p', class_ = 'iLkEeQbexGs-').text

        # this will extract the review text
        review_text = review.find('span', class_ = 'l9bbXUdC9v0- ZatlKKd1hyc- ukvN6yaH1Ds-').text

        # this extracts list of ratings left by the user
        ratings_list = review.find('ol', class_ = 'gUG3MNkU6Hc- ciu9fF9m-z0-')

        # results container
        results_dict = {}

        # this will parse out individual ratings and update results_dict
        for rating in ratings_list:
            category = rating.contents[0].strip()
            value = rating.find('span').text
            results_dict[category] = value

        # this will update results_dict
        results_dict["review_text"] = review_text
        results_dict["hometown"] = reviewer_hometown
        results_dict['datelike'] = review_datelike

        return results_dict

    def scrape_individual_restaurant(self, res_url):
        """
        Scrape a restaurant starting the restaurant home url extracted during phase one of the scraper.

        Args:
            res_url: (url) - The URL to the current restaurant.
        """
        # define page traker
        tracker = 1

        # add tag to the base url 
        url = res_url + f"&page={tracker}&sortBy=newestReview"
        num_pages = self.get_total_pages_for_restaurant(url)

        while (tracker < 20) and (tracker < num_pages):

            # define the url
            url = f"{res_url}&sortBy=newestReview&page={tracker}&sortBy=newestReview"

            try: 
                # start html session
                session = HTMLSession()

                # access the url
                response = session.get(url, timeout = 10)

                if response.status_code != 200:
                    print("Failed to load page...continuing")
                    continue

            except requests.exceptions.RequestException as e:
                print(f"An error occured: {e}")
                continue

            # instantiate Soup object
            soup = BeautifulSoup(response.text, "html.parser")

            # get restaurant name
            main = soup.find('main', class_ = "mwul4aJazVU-")
            name = main.find('script', type = 'application/ld+json')

            # it's a json, so load the json
            if name:
                data = json.loads(name.string)
                restaurant_name = data.get('name', 'name_not_found')

            print(f'This restuarant has: {num_pages} pages of reviews.')
            print(f'Currently scraping page #{tracker}')

            # this grabs the entire review
            reviews = soup.find_all('li', class_ = 'afkKaa-4T28-')
            print("The number of reviews on this page is: ", len(reviews))
            print()

            for review in reviews:
                results_dict = self.grab_review_data(review)
                results_dict['res_name'] = restaurant_name
                results_dict['origins'] = "open_table"
                self.results_list.append(results_dict)
            tracker += 1

        return None
    
if __name__ == "__main__":
    pass