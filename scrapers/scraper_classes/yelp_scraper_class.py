"""
    Review Aggregator
    Joseph Nelson Farrell
    05-14-24

    This file contains a class that will scrape Yelp for review data.
"""
# libraries
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

# scraper class
class YelpScraper:
    """
    The class will scrape Yelp for restaurant review data starting at a city or region Yelp homepage,
    i.e, https://www.yelp.com/search?find_desc=&find_loc=Portland%2C+ME

    It works in two phases, first it will grab all the restaurant links on the "base_url" and all sebsequent urls.
    Second, it will visit each restaurant link and grab the most recent 300 reviews (or the total amount if < 300)
    """
    def __init__(self, base_url, region, business_type) -> None:
        """
        YelpScraper initializer.

        Args:
            base_url: (str) - This is where the scraper will start. It should be a page that lists restaurant links.

        Attributes:
            hrefs: (list) - This is a list of individual restaurant links that are extracted by the first phase of the
                scraper.
            base_url: (str) - This is where the scraper will start. It should be a page that lists restaurant links.
            service: (Service) - This is where you set the link to your internet driver, here it's a chromedriver. This would
                have to be changed if this class were used on another machine.
            driver: (webdriver.Chrome) - This is the actual driver.
            review_data: (list) - This will be a list of dicts where each dict is the data of single review.
            reviews: (list) - This will be reused. For each restaurant, on each page of reviews, this will be a list of "review" classes
                extracted from the HTML.
        """
        self.service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service = self.service)
        self.hrefs = []
        self.base_url = base_url
        self.restaurant_data = []
        self.review_data = []
        self.reviews = None
        self.region = region
        self.buiness_type = business_type
        self.date = str(date.today())

    def go_to_region(self):
        """
        Go to the city, state specified
        """
        try:
            self.driver.get(self.base_url)
        except Exception as e:
            print(f"Error loading the url: {e}")
            return
        
        try:
            # grab the entry box for the region
            input_element = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "search_location"))
            )
            # clear the default value
            input_element.clear()
            time.sleep(1)

            # enter the entry box
            input_element.click()

            # the default value (current location) will still interfere with the entry
            # this will get the value and delete it manually.
            current_value = input_element.get_attribute('value')
            for _ in range(len(current_value)):
                input_element.send_keys(Keys.ARROW_RIGHT)
            for i in range(len(current_value)):
                input_element.send_keys(Keys.BACK_SPACE)
            time.sleep(1)
            input_element.send_keys(self.region)
            time.sleep(1)

        except Exception as e:
            print(f"Error fetching the region entry box: {e}")
            return
        
    def enter_business_type(self):
        """
        Enter the business type of the reviews we are scraping
        """
        try:
            # grab the entry box for the region
            input_element = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "search_description"))
            )
            # clear the default value
            input_element.clear()
            time.sleep(1)

            # enter the entry box
            input_element.click()

            # enter the value
            input_element.send_keys(self.buiness_type)
            input_element.send_keys(Keys.RETURN)

        except Exception as e:
            print(f"Error fetching the business type entry box: {e}")
            return

    def navigate_pages_get_res_urls(self):
        """
        This function will extract the restaurant URLs form the base_url. It will navigate all the pages
        of restaurants before terminating.
        """
        # iterate over the pages of restaurants, grabbing url to each
        while True:

            try:
                # this will get the restaurant url off each page
                self.get_restaurant_urls()

                # this will locate the buttons, there are two with the same class name
                buttons = self.driver.find_elements(By.CLASS_NAME, "y-css-1ewzev")

                # this will pull out the next page button
                next_page_button_list = [button for button in buttons if button.text.strip() == "Next Page"]
                next_page_button = next_page_button_list[0]

                # this break the loop when the button is no longer active, i.e., no more pages
                if next_page_button.get_attribute('disabled') is not None:
                    print("Last page has been reached...Next Page is disabled")
                    break
                
                # this will click the button and go to the next page
                next_page_button.click()
                time.sleep(5)

            except Exception as e:
                continue

    def get_restaurant_urls(self):
        """
        This function will extract all the restaurants on a particular page and extract the urls
        for each restaurant.
        """
        # this will grab the area on the page that contains the restaurant url
        WebDriverWait(self.driver, 5).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "y-css-12ly5yx"))
        )
        restaurant_cards = self.driver.find_elements(By.CLASS_NAME, "y-css-12ly5yx")

        # this will extract the url
        for card in restaurant_cards:
            href = card.get_attribute('href')
            self.hrefs.append(href)
    
    def remove_unwanted_urls(self):
        """
        This function will remove links that are not links to restaurants
        """
        # container for the kept links
        hrefs = []

        # iterate over links
        for href in self.hrefs:

            # only keep if link contains this
            if "https://www.yelp.com/biz/" in href:
                hrefs.append(href)
            else:
                continue

        # update hrefs attribute
        self.hrefs = hrefs
    
    def get_restuarant_data(self, href):
        """
        Extract the restaurant data, i.e., name, price point, and tags (destriptors). 
        """
        price_point = None
        tags = None
        res_name = None
        print("Entering get restaurant data...")
        try:
            # start sessiona and go to URL
            session = HTMLSession()
            response = session.get(href)
            soup = BeautifulSoup(response.text, "html.parser")
        except Exception as e:
            print(f"Error loading the URL: {e}")

        # declare results container
        res_data_dict = {}

        try:
            # get the restaurant name
            name = soup.find("h1", class_ = "y-css-olzveb")
            res_name = name.text.strip()

        except Exception as e:
            print(f"Error extracting restaurant name: {e}")
            res_name = None

        try: 
            # get the restuarant price point, if there is one
            container = soup.find_all("span", class_ = "y-css-tqu69c")
            if len(container) == 1:
                price_point = None

            for index, object_ in enumerate(container):
                if index == 0:
                    continue
                price_point = object_.find("span", class_  = "y-css-33yfe")
                price_point = price_point.text.strip()
        except Exception as e:
            print(f"Error extracting price point: {e}")
            price_point = None
            
        try:
            # get the restaurant tags
            tags_container_a = soup.find("span", class_ = "y-css-1w2z0ld")
            tags_container_b = tags_container_a.find_all("span", class_ = "y-css-kw85nd")
            tags_list = []
            for tags in tags_container_b:
                tag = tags.find("a", class_ = "y-css-12ly5yx")
                tag = tag.text.strip()
                tags_list.append(tag)
        except Exception as e:
            print(f"Error extracting tags: {e}")
            tags_list = None

        # add results to res_data_dict
        res_data_dict["restaurant_name"] = res_name
        res_data_dict["price_point"] = price_point
        res_data_dict["tags"] = tags_list
        res_data_dict["region"] = self.region
        self.restaurant_data.append(res_data_dict)

    def get_restuarant_name(self):
        """
        This function will will extract the restaurant name
        """
        # this will grab the res name
        WebDriverWait(self.driver, 5).until(
            EC.presence_of_element_located((By.CLASS_NAME, "y-css-olzveb")) 
        )
        res_name = self.driver.find_element(By.CLASS_NAME, "y-css-olzveb")
        print("Restaurant name:", res_name)

        return res_name.text
    
    def get_reviews(self):
        """
        This function will generate a list of review objects from the restaurant url
        """
        print("Entering get reviews...")
        try:
            element_present = EC.presence_of_all_elements_located((By.XPATH, '//*[@id="reviews"]/section/div[2]/ul/li'))
            WebDriverWait(self.driver, 10).until(element_present)
        except TimeoutError:
            print("Timed out waiting for elementes to load.")

        try:
            # get reviews
            reviews = self.driver.find_elements(By.XPATH, '//*[@id="reviews"]/section/div[2]/ul/li')
            self.reviews = reviews
            print(f"The length of self.reviews is: {len(self.reviews)}")

        except Exception as e:
            print(f"Error extracting the reviews: {e}")
            self.reviews = []


    def extract_review_data(self, res_name):
        """
        This function will extract review data from the review object storing data in a dict.
        The dict will be added to the review_data attribute.

        Args:
            res_name: (str) - the name of the current restaurant.

        Returns:
            None
        """
        # iterate over all the review objects stored in self.reviews
        for review in self.reviews:

            # results container
            results_dict = {}

            try:   
                # get reviewer name
                name_element = review.find_element(By.CLASS_NAME, "y-css-w3ea6v")
                name = name_element.text
            except Exception as e:
                print(f'Error extracting restaurant name: {e}')
                name = None

            try:
                # get the date
                date_element = review.find_element(By.CLASS_NAME, "y-css-wfbtsu")
                date = date_element.text
            except Exception as e:
                print(f"Error extracting the date: {e}")
                date = None

            try:
                # get reviewer hometown
                hometown_element = review.find_element(By.CLASS_NAME, "y-css-12kfwpw")
                hometown = hometown_element.text
            except Exception as e:
                print(f"Error extracting the hometown: {e}")
                hometown = None

            try:
                # get the review rating
                rating = review.find_element(By.CLASS_NAME, "y-css-9tnml4")
                rating = rating.get_attribute("aria-label")
            except Exception as e:
                print(f"Error extracting the rating: {e}")
                rating = None

            try:
                # get the review text
                review_text_element = review.find_element(By.CLASS_NAME, "comment__09f24__D0cxf")
                review_text = review_text_element.find_element(By.CLASS_NAME, "raw__09f24__T4Ezm").text
            except Exception as e:
                print(f"Error extracting the review text: {e}")
                review_text = None

            # update results_dict
            results_dict["restaurant"] = res_name
            results_dict["reviewer_name"] = name
            results_dict["datelike"] = date
            results_dict["hometown"] = hometown
            results_dict["rating"] = rating
            results_dict["text"] = review_text
            results_dict["origins"] = "Yelp"

            # append results dict to the results list
            self.review_data.append(results_dict)

    def go_to_restaurant_url_extract_data(self):
        """
        This function will visit all the review pages for a particular restaurant
        """
        # print statement
        print("Going to restuarant URLs...")

        # iterate over each restaurant link
        for href in self.hrefs:

            # sort the reviews by most recent
            URL = f'{href}&sort_by=date_desc'

            # visit the first page
            self.driver.get(URL)
            time.sleep(3)

            # get the res name
            res_name = self.get_restuarant_name()
            print(f"Currently Scraping: {res_name} \n")

            # get the restaurant data
            self.get_restuarant_data(href)

            # this is used to limit the number of reviews extracted to 800
            tracker = 0

            # this loop visits each review page by clicking "next-link"
            while True:

                try:
                    # get the review objects
                    print("Getting reviews...")
                    self.get_reviews()

                    # extract review data
                    print("Extracting review data...")
                    self.extract_review_data(res_name)

                    # this just monitors progress
                    df = pd.DataFrame(self.review_data)
                    df_2 = pd.DataFrame(self.restaurant_data)
                    print(df_2)
                    print(df)

                    # get the next button
                    buttons = self.driver.find_element(By.CLASS_NAME, "next-link")

                    # this will click the button and go to the next page
                    buttons.click()
                    time.sleep(1)

                    # update tracker
                    tracker += 1

                    # sets a review limit
                    if tracker == 30:
                        break
                
                # this is activated that "buttons" is no loner active
                except Exception as e:
                    break
        
        # close the driver
        self.driver.close()

if __name__ == "__main__":
    pass




