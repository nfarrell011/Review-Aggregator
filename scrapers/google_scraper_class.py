"""
    Review Aggregator
    Michael Massone
    05-23-24

    This file contains a class that will scrape Google Reviews for review data.
"""
#############################################################################################
## Libraries
#############################################################################################

#import pandas as pd
#import json
#import nltk
#from nltk.tokenize import word_tokenize
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from time import sleep
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import re

#nltk.download('punkt')

#############################################################################################
## Class
#############################################################################################

class GoogleScraper:
    '''
    '''

    def __init__(self) -> None:
        '''
        '''
        self.service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service = self.service)
        self.results_list = [] # review data
        self.reviews = []

#############################################################################################
    def google_search(self, city ="portland", state="maine", business="restaurants"):
        '''
        Description:

            Searches google to find area restaurants.

        Args:

            search_input (str): search input for Google. 
            Should be in the format 'city state restuarnts'
        
        '''
        # combine search input
        search_input = city + " " + state + " " + business

        # navigate to google.com
        self.driver.get("https://www.google.com")
        print(self.driver.title)  # Prints the title of the page

        # waits for gogle search bar to load
        WebDriverWait(self.driver, 2).until(
            EC.presence_of_element_located((By.CLASS_NAME, "gLFyf"))
        )

        # Find Google search field and input search criteria
        input_element = self.driver.find_element(By.CLASS_NAME, "gLFyf")
        input_element.clear() # clears any existing content from the element
        input_element.send_keys(search_input + Keys.ENTER) # enters text and activates ENTER key

        # click on the more results button
        try:
            # Wait for the element to be clickable
            link = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "a.CHn7Qb.pYouzb"))
            )
            
            # Click on the element
            link.click()
            print("Link clicked successfully.")

        except Exception as e:
            print(f"An error occurred: {e}")

        # Time to keep window open
        sleep(2)

#############################################################################################
    def get_reviews(self):
        """Collect all links to restuarants on results page"""

        try:
            # wait for the page to load and find all restaurant pages
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".vwVdIc.wzN8Ac.rllt__link"))
            )
            
            elements = self.driver.find_elements(By.CSS_SELECTOR, ".vwVdIc.wzN8Ac.rllt__link")
            
            # click each element in the list
            flag = True
            review_list = []
            for element in elements:
                
                # click on restaurant
                WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable(element)
                )
                element.click()

                # isolate restuarant name
                name = element.text.split('\n', 1)[0]
                print(f"Clicked on {name}.")
                
                # polite pause
                sleep(1)

                # select review tab if first run
                if flag:
                    self.click_reviews_tab()
                    flag = False

                # click 'newest' button
                self.click_newest_button()

                # polite pause
                sleep(1)

                # scroll until all reviews for the past year are visible
                reviews = self.scroll_by_elements()
                review_list.append(reviews)

            # package reviews in dictionary and append to self.reviews
            self.reviews.append({name: review_list})

        except Exception as e:
            print("Error clicking elements:", e)

#############################################################################################
    def click_reviews_tab(self):
        """Expands reviews"""
        try:
            xpath_selector = "//a[@jsname='AznF2e' and @data-index='2' and contains(@class, 'zzG8g') and contains(@class, 'kvxLRc') and contains(@class, 'QmGLUd') and .//span[text()='Reviews']]"

            # wait for the element to be clickable
            reviews_tab = WebDriverWait(self.driver, 2).until(
                EC.element_to_be_clickable((By.XPATH, xpath_selector))
            )
            # click the "More Google reviews" link
            reviews_tab.click()
            print("Clicked on 'Reviews' tab.")

        except Exception as e:
            print("Error:", e)

#############################################################################################
    def click_newest_button(self):
        '''Click the "Newest" button or restaurant page to order the reviews.''' 
        
        try:
            # wait for the element to be present
            element = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//div[@class='YCQwEb btku5b k0Jjg A29zgf LwdV0e zqrO0 sLl7de rlt7Ub PrjL8c ' and @data-sort='2' and @jsaction='click:pwGece;' and @jsname='XPtOyb' and @role='radio' and @aria-checked='false']"))
            )
            # Scroll the element into view using JavaScript
            self.driver.execute_script("arguments[0].scrollIntoView(true);", element)
            sleep(1)  # wait for any scrolling effects to settle

            # click the element using JavaScript to avoid interception issues
            self.driver.execute_script("arguments[0].click();", element)
            print("CLicked on 'Newest' button.")
        except Exception as e:
            print("Error:", e)   

#############################################################################################
    def next_page(self):
        """Advance to next page in restuarant search results"""

        try:
            # Wait for the element to be clickable
            next_button = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((By.ID, "pnnext"))
            )
            # Click the next button
            next_button.click()
            print("Next page link clicked successfully.")
        
        except Exception as e:
            print("Error clicking the next page link:", e)


#############################################################################################
    def scroll_by_elements(self):
        ''' Scroll down restaurant page review by review until all reviews in the past year have been collected.'''

        try:
            while True:

                reviews = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_all_elements_located((By.CLASS_NAME, "bwb7ce"))
                )
                print("Found reviews container.")

                more_buttons = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_all_elements_located((By.CLASS_NAME, "MtCSLb"))
                )

                # click all the buttons
                for button in more_buttons:
                    self.driver.execute_script("arguments[0].click();", button)
                    print("Clicked 'More' button")

                # polite pause
                sleep(1)

                # check a review
                print(reviews[-1].text)

                self.driver.execute_script("arguments[0].scrollIntoView(true);", reviews[-1])
                print('scrolling...')

                immersive_container = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, 'immersive-container'))
                        )            
                print("Found immersive container.")

                # check if the phrase "one year ago" is in the element's text content
                if "a year ago" in immersive_container.text.lower():
                    print('One year of reviews loaded.')
                    break
                sleep(1)

        except Exception as e:
            print("Error scrolling:", e) 

        return reviews        

#############################################################################################
    def extract_review_data(self):
        """Scrape review data from restuarnt's google page."""
        
        # iterate over all the review objects stored in self.reviews
        for res_dict in self.reviews:
            for res_name, reviews in res_dict.items():
                for review in reviews:
    
                    try:
                        # results container
                        results_dict = {}
                        
                        # get reviewer name
                        name_element = review.find_element(By.CLASS_NAME, "Vpc5Fe")
                        name = name_element.text
                        print(name)

                        # get reviewers link
                        link_element = review.find_element(By.CLASS_NAME, "yC3ZMb")
                        link = link_element.get_attribute("href")
                        print(link)

                        # get the date
                        date_element = review.find_element(By.CLASS_NAME, "y3Ibjb")
                        date_phrase = date_element.text
                        date = self.get_date_from_phrase(date_phrase)
                        print(date)

                        # get the review rating
                        rating = review.find_element(By.CLASS_NAME, "dHX2k")
                        rating = rating.get_attribute("aria-label")

                        # get the review text
                        review_text_element = review.find_element(By.CLASS_NAME, "OA1nbd")
                        review_text = review_text_element.text

                        # update results_dict
                        results_dict["restaurant"] = res_name
                        results_dict["reviewer_name"] = name
                        results_dict["reviewer_link"] = link
                        results_dict["datelike"] = date
                        results_dict["rating"] = rating
                        results_dict["text"] = review_text
                        results_dict["origins"] = "Google"

                        # append results dict to the results list
                        self.results_list.append(results_dict)

                    except Exception as e:
                        print(f"An error occurred while processing a review: {e}")
#############################################################################################
    def parse_phrase_to_timedelta(self, phrase):

        # Match patterns like "a week ago", "3 days ago", "2 months ago", etc.
        match = re.match(r'(\d+|a)\s+(day|week|month|year)s?\s+ago', phrase)
        
        if not match:
            raise ValueError("Invalid phrase format")
        
        # Extract number and unit from the phrase
        number, unit = match.groups()
        
        if number == 'a':
            number = 1
        else:
            number = int(number)
        
        # Define mapping from unit to timedelta arguments
        if unit == 'day':
            delta = timedelta(days=number)
        elif unit == 'week':
            delta = timedelta(weeks=number)
        elif unit == 'month':
            delta = timedelta(days=30 * number)  # Approximate month as 30 days
        elif unit == 'year':
            delta = timedelta(days=365 * number)  # Approximate year as 365 days
        else:
            raise ValueError("Unknown time unit")
        
        return delta
    
#############################################################################################
    def get_date_from_phrase(self, phrase, current_date=None):
        if current_date is None:
            current_date = datetime.now()
        
        delta = self.parse_phrase_to_timedelta(phrase)
        past_date = current_date - delta
        return past_date

#############################################################################################
## END
#############################################################################################