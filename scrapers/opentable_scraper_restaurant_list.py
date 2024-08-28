"""
Review Aggregator
Joseph Nelson Farrell
05-14-24

This file contains a class that will scrape OpenTable for review data.
"""
##########################################################################################################################
# libraries
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
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
from datetime import date
import concurrent.futures
nltk.download('punkt')

##########################################################################################################################
# class
class OpenTableScraperRestaurantList():
    """ 
    This class will scrape OpenTable for review data. It is designed to be used with YelpScraper.
    YelpScraper provides a more precise geographically defined region; the results (list of restaurants) from YelpScraper 
    is designed to provide the "restaurant_list" arguement. However, any restaurant list (within the corresponding region will
    suffice). 

    The scraper works by iteratively entering each restaurant in "restaurant_list" and "region" in the OpenTable 
    search feild and following the links to extract restaurant and review data.
    """
    def __init__(self, base_url, region, state, restaurant_name) -> None:
        """
        OpenTableScraper initializer.

        Parameters:
        - base_url: (str)             - Scraper starting URL. It should be the OpenTable homepage.
        - region: (str)               - Location of restaurant (city, state), this is entered into the search bar of the OpenTable homepage,
                                        the base_url.
        - state (str)                 - The state where all the restaurants should be located, used to verify that the correct restaurant is
                                        being scraped.
        - restaurant_name             - The name of the restaurant being scraped.

        Attributes:
        - service: (Service)          - This is where you set the link to your internet driver, here it's a chromedriver. This would
                                        have to be changed if this class were used on another machine.
        - driver: (webdriver.Chrome)  - This is the actual driver.
        - restuarant_url (str)        - The url to a specific restaurant. Used to switch to BeautifulSoup.
        - base_url: (str)             - This is where the scraper will start. It should be the OpenTable homepage.
        - review_data: (list)         - This will be a list of dicts where each dict is the data of single review.
        - restaurant_data: (list)     - This will be a list of dicts where each dict is the data of single restaurant.
        - region: (str)               - Where the restaurant is located, this is entered into the search bar of the OpenTable homepage,
                                        the base_url.
        - current_state (str)         - The state where all the restaurants should be located, used to verify that the correct restaurant is
                                        being scraped.
        - current_restaurant: (str)   - The restaurant name provided by Yelp and used as input; comparison with extracted restaurant name will be 
                                        used for data verification.
        - date: (str)                 - The date when the scaping took placed, used to name data file name upon completion.
        """
        self.service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service = self.service)
        self.restaurant_url = None
        self.base_url = base_url
        self.review_data = []
        self.restaurant_data = []
        self.region = region
        self.current_state = state
        self.current_restaurant = restaurant_name
        self.reviews = None # this can be removed; VERIFY
        self.date = str(date.today())

    def go_to_base_url(self) -> None:
        """
        Go to the base URL
        """
        self.driver.get(self.base_url)
        return None

    def go_to_restaurant(self) -> bool:
        """
        Go to the restaurant in the region specified.

        Parameters:
        - None

        Return:
        - bool: Indicating success or failure of going to the restaurant.
        """
        try:
            # get the search feild
            input_element = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CLASS_NAME, "Gr6kc2R-bzc-"))
            )

            # clear the feild
            input_element.clear()

            # make search bar entry
            restaurant_input = self.current_restaurant + ", "
            input_element.send_keys(restaurant_input, self.region)
            time.sleep(1)
            input_element.send_keys(Keys.RETURN)
            time.sleep(3)
            print("Successfully went to restaurant...")
            return True # this was newly added; VERIFY

        except Exception as e:
            print(f'Error going to restuarant: {e}')
            return False

    def go_to_restaurant_with_timeout(self, timeout = 10):
        """
        Adds a timer wrapper to go to restaurant. This is prevent the scraper from getting stuck.

        Parameters:
        - timeout: (int) - How long in seconds the function will wait.

        Return:
        - bool: Indicating success or failure of going to the restaurant.
        """
        def target():
            try:
                self.go_to_restaurant()
            except Exception as e:
                return False

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(target)
            try:
                future.result(timeout = timeout)
                return True
            
            except concurrent.futures.TimeoutError:
                print(f"Function took longer than {timeout} seconds to complete and was terminated.")
                return False
            except Exception as e:
                print(f"An error occurred in go_to_restaurant_with_timeout: {e}")
                return False

    def click_res_link(self) -> bool:
        """
        This will click the restaurant link and go to the first page of reviews.

        Parameters:
        - None

        Return:
        - bool: Indicating success or failure of clicking the restaurant link.
        """
        try:
            # this extracts the numebr of reviews; used to check if there is data to scrape
            num_reviews = WebDriverWait(self.driver, 5).until(
                EC.visibility_of_element_located((By.CLASS_NAME, "XmafYPXEv24-"))
            )

            # if there are no reviews exit
            num_reviews = num_reviews.text
            if num_reviews == "(0)":
                print(f"Restaurant: {self.current_restaurant} not located...proceeding to next restaurant")
                self.driver.close()
                self.driver.quit()
                return False
            
            # if there are reviews, click the link the restaurant page
            input_element = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((By.CLASS_NAME, "qCITanV81-Y-"))
            )
            input_element.click()
            time.sleep(3)
            return True
        
        except Exception as e:
            print(f"Restaurant: {self.current_restaurant} not located...proceeding to next restaurant")
            return False
        
    def switch_to_new_tab(self) -> None:
        """
        Switch to the newly opened browser tab.

        Parameters:
        - None

        Return:
        - None
        """
        # Get the list of all open windows/tabs
        handles = self.driver.window_handles
        
        # Switch to the last opened tab
        self.driver.switch_to.window(handles[-1])
        return None

    def get_restaurant_url(self) -> None:
        """
        Gets the current URL, the restaurant URL. Used to switch to BeautifulSoup.

        Parameters:
        - None

        Return:
        - None
        """
        self.restaurant_url = self.driver.current_url
        return None

    def get_total_pages_for_restaurant(self, url) -> int:
        """
        Get the number of review pages for an individual restaurant.

        Parameters:
        - url: (str) - The url to an individual restaurant's OpenTable page.

        Returns:
        - int: The number of review pages.
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

    def grab_review_data(self, review) -> dict:
        """
        Extact the data from a "review" object taken from the HTML

        Parameters:
        - review: (bs4.element.tag) - A review of object taken from a restaurant HTML.

        Returns:
        - dict: A dict containing the extracted data.
        """
        # results container
        results_dict = {}

        try:
            # get reviewer name
            name = review.find('p', class_ = "_1p30XHjz2rI- C7Tp-bANpE4-")
            name = name.text
        except Exception as e:
            print(f'Error loading reviewer name: {e}')
            name = None

        try:
            # this extracts the hometown of the reviewer
            reviewer_hometown = review.find('p', class_ = 'POyqzNMT21k- C7Tp-bANpE4-').text
        except Exception as e:
            print(f'Error loading reviewer hometown: {e}')
            reviewer_hometown = None

        try:
            # this extracts when the review was made
            review_datelike = review.find('p', class_ = 'iLkEeQbexGs-').text
        except Exception as e:
            print(f'Error loading date: {e}')
            review_datelike = None

        try:
            # this will extract the review text
            review_text = review.find('span', class_ = 'l9bbXUdC9v0- ZatlKKd1hyc- ukvN6yaH1Ds-').text
        except Exception as e:
            print(f'Error loading review text: {e}')
            review_text = None

        
        try:
            # this extracts list of ratings left by the user
            ratings_list = review.find('ol', class_ = 'gUG3MNkU6Hc- ciu9fF9m-z0-')

            # this will parse out individual ratings and update results_dict
            for rating in ratings_list:
                category = rating.contents[0].strip()
                value = rating.find('span').text
                results_dict[category] = value
        except Exception as e:
            print(f"Error loading the ratings list: {e}")

            # update results_dict
            results_dict["Overall"] = None
            results_dict['Food'] = None
            results_dict['Service'] = None
            results_dict['Ambience'] = None

        # update results_dict
        results_dict["review_text"] = review_text
        results_dict["hometown"] = reviewer_hometown
        results_dict['datelike'] = review_datelike
        results_dict['reviewer_name'] = name
        results_dict['restaurant_name_input'] = self.current_restaurant

        return results_dict
    
    def get_restaurant_data(self) -> bool:
        """
        Get the restaurant data

        Parameters:
        - None

        Returns:
        - bool: Indicating the restaurant is located in the expected state.
        """
        try:
            # open a session
            session = HTMLSession()
            response = session.get(self.restaurant_url)
            response.raise_for_status()
        except Exception as e:
            print(f'Error loading the URL: {e}')
            return
        
        try:
            # set up Soup
            soup = BeautifulSoup(response.text, "html.parser")
        except Exception as e:
            print(f"Error parsing HTML: {e}")
            return

        # container for results
        results_dict = {}

        try:
            # get the state 
            header_element_list = soup.find_all('li', class_ = "WqMI-RYz0Ok-" )
            state = header_element_list[2].text

            # check if it matches expectation
            if state != self.current_state:
                return False
        except Exception as e:
            print(f"Error extracting state: {e}")
        
        try:
            # get restaurant name
            main = soup.find('main', class_ = "mwul4aJazVU-")
            name = main.find('script', type = 'application/ld+json')

            # load the json
            if name:
                data = json.loads(name.string)
                restaurant_name = data.get('name', 'name_not_found')
            else:
                restaurant_name = None
        except Exception as e:
            print(f'Error getting restaurant name: {e}')
            restaurant_name = None

        # user information update
        print(f"Getting Restaurant data for: {restaurant_name}")     

        try:
            # get the price range
            price_range = soup.find('div', class_ = "HVZgW51iSt4- C7Tp-bANpE4-", id = "priceBandInfo" )
            price_range = price_range.find('span', class_ = '')
            price_point = price_range.text

        except Exception as e:
            print(f'Error getting price point: {e}')
            price_point = None

        try:
            # get the cousine
            cuisine = soup.find('div', class_ = "HVZgW51iSt4- C7Tp-bANpE4-", id = "cuisineInfo" )
            cuisine = cuisine.find('span', class_ = '')
            cuisine = cuisine.text
        except Exception as e:
            print(f'Error getting cuisine: {e}')
            cuisine = None
    
        
        try:
            # get restuarant description
            des = soup.find('div', class_ = 'sn86cyGEeWY-')
            des = des.find('span', class_ = '')
            des = des.text
        except Exception as e:
            print(f'Error getting the restaurant description: {e}')
            des = None

       
        try:
            # get tags
            tags = soup.find('ul', class_ = 'wuo3vcS-Vqo-')
            tags = tags.find_all('span', class_ = "SCM99wuIzbk- BeBapc-NEAM- C7Tp-bANpE4-")
            tags_list = []
            for tag in tags:
                tags_list.append(tag.text)

        except Exception as e:
            print(f'Error getting the tags: {e}')
            tags_list = None

        # update results dict
        results_dict['price_point'] = price_point
        results_dict['cuisine'] = cuisine
        results_dict['description'] = des
        results_dict['tags'] = tags_list
        results_dict['region'] = self.region
        results_dict['restaurant_name_extracted'] = restaurant_name
        results_dict['restaurant_name_input'] = self.current_restaurant

        # add results dict to overall results list
        self.restaurant_data.append(results_dict)
        return True

    def scrape_individual_restaurant(self, max_pages = 20):
        """
        Scrape a restaurant starting the restaurant home url extracted during phase one of the scraper.

        Parameters:
        - max_pages: (int) - Controls how many pages of reviews to scrape; 10 reviews per page (typically).
        """
        # define page traker
        tracker = 1

        # add tag to the base url 
        url = self.restaurant_url + f"&page={tracker}&sortBy=newestReview"
        num_pages = self.get_total_pages_for_restaurant(self.restaurant_url)

        while (tracker < max_pages) and (tracker < num_pages):

            # define the url
            url = f"{self.restaurant_url}&sortBy=newestReview&page={tracker}&sortBy=newestReview"

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

            print(f'Now scrapping: {restaurant_name}')
            print(f'This restuarant has: {num_pages} pages of reviews.')
            print(f'Currently scraping page #{tracker}')

            # this grabs the entire review
            reviews = soup.find_all('li', class_ = 'afkKaa-4T28-')
            print("The number of reviews on this page is: ", len(reviews))
            print()

            # itnerate over reviews extracting data
            for review in reviews:
                results_dict = self.grab_review_data(review)
                results_dict['restaurant_name_extracted'] = restaurant_name
                results_dict['origins'] = "open_table"
                self.review_data.append(results_dict)
            tracker += 1

        return None
    
##########################################################################################################################
# End

if __name__ == "__main__":
   pass
