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
from nltk.tokenize import word_tokenize
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
nltk.download('punkt')

# scraper class
class YelpScraper:
    """
        The class will scrap Yelp for restaurant review data starting a city or region's Yelp homepage,
        i.e, https://www.yelp.com/search?find_desc=&find_loc=Portland%2C+ME

        It works in two phases, first it will grab all the restaurant links on the "base_url" and all sebsequent urls.
        Second, it will visit each restaurant link and grab the most recent 300 reviews (or the total amount if < 300)
    """
    def __init__(self, base_url) -> None:
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
                results_list: (list) - This will be a list of dicts where each dict is the data of single review.
                reviews: (list) - This will be reused. For each restaurant, on each page of reviews, this will be a list of "review" classes
                    extracted from the HTML.
        """
        self.hrefs = []
        self.base_url = base_url
        self.service = Service(executable_path = "chromedriver-mac-arm64/chromedriver")
        self.driver = webdriver.Chrome(service = self.service)
        self.results_list = []
        self.reviews = None

    def navigate_pages_get_res_urls(self):
        """
            This function will extract the restaurant URLs form the base_url. It will navigate all the pages
            of restaurants before terminating.
        """
        # go to the base URL
        self.driver.get(self.base_url)

        # allow time to load
        time.sleep(5)

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
        #restaurant_cards = self.driver.find_elements(By.CLASS_NAME, "y-css-12ly5yx")
        WebDriverWait(self.driver, 5).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "y-css-12ly5yx"))
        )
        restaurant_cards = self.driver.find_elements(By.CLASS_NAME, "y-css-12ly5yx")
        #print(len(restaurant_cards))

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

    def get_restuarant_name(self):
        """
            This function will will extract the restaurant name
        """
        # this will grab the res name
        WebDriverWait(self.driver, 5).until(
            EC.presence_of_element_located((By.CLASS_NAME, "y-css-olzveb")) 
        )
        res_name = self.driver.find_element(By.CLASS_NAME, "y-css-olzveb")

        return res_name.text
    
    def get_reviews(self):
        """
            This function will generate a list of review objects from restaurant url
        """
        print("Entering get reviews...")

        # swap this out for the proper way to wait with Sel
        time.sleep(3)

        # this will grab all the reviews
        reviews = self.driver.find_elements(By.CLASS_NAME, "y-css-1jp2syp")

        # assign reviews to reviews attribute
        self.reviews = reviews
        print(len(self.reviews))


    def extract_review_data(self, res_name):
        """
            This function will extract review data from the review object storing data in a dict.
            The dict will be added to the results_list attribute.

            Args:
                res_name: (str) - the name of the current restaurant.

            Returns:
                None
        """
        # iterate over all the review objects stored in self.reviews
        for review in self.reviews:

            try:
                # results container
                results_dict = {}
                
                # get reviewer name
                name_element = review.find_element(By.CLASS_NAME, "y-css-w3ea6v")
                name = name_element.text

                # get the date
                date_element = review.find_element(By.CLASS_NAME, "y-css-wfbtsu")
                date = date_element.text

                # get reviewer hometown
                hometown_element = review.find_element(By.CLASS_NAME, "y-css-12kfwpw")
                hometown = hometown_element.text

                # get the review rating
                rating = review.find_element(By.CLASS_NAME, "y-css-9tnml4")
                rating = rating.get_attribute("aria-label")

                # get the review text
                review_text_element = review.find_element(By.CLASS_NAME, "comment__09f24__D0cxf")
                review_text = review_text_element.find_element(By.CLASS_NAME, "raw__09f24__T4Ezm").text

                # update results_dict
                results_dict["restaurant"] = res_name
                results_dict["reviewer_name"] = name
                results_dict["datelike"] = date
                results_dict["hometown"] = hometown
                results_dict["rating"] = rating
                results_dict["text"] = review_text
                results_dict["origins"] = "Yelp"

                # append results dict to the results list
                self.results_list.append(results_dict)

            except Exception as e:
                print(f"An error occurred while processing a review: {e}")

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
                    df = pd.DataFrame(self.results_list)
                    print(df)

                    # get the next button
                    buttons = self.driver.find_element(By.CLASS_NAME, "next-link")

                    # this will click the button and go to the next page
                    buttons.click()
                    time.sleep(3)

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




