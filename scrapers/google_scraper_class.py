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
        self.hrefs = [] # links
        self.service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service = self.service)
        self.results_list = [] # review data
        self.source = None

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
    def open_restuarant_page(self):
        """Collect all links to restuarants on results page"""

        try:
            # wait for the page to load and find all restaurant pages
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".vwVdIc.wzN8Ac.rllt__link"))
            )
            
            elements = self.driver.find_elements(By.CSS_SELECTOR, ".vwVdIc.wzN8Ac.rllt__link")
            
            # click each element in the list
            flag = True
            for element in elements:

                WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable(element)
                )
                element.click()

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
                self.scroll_to_bottom()

        except Exception as e:
            print("Error clicking elements:", e)

#############################################################################################
    def click_reviews_tab(self):
        """Expands reviews"""
        try:
            xpath_selector = "//a[@jsname='AznF2e' and @data-index='2' and contains(@class, 'zzG8g') and contains(@class, 'kvxLRc') and contains(@class, 'QmGLUd') and .//span[text()='Reviews']]"

            # Wait for the element to be clickable
            reviews_tab = WebDriverWait(self.driver, 2).until(
                EC.element_to_be_clickable((By.XPATH, xpath_selector))
            )
            # Click the "More Google reviews" link
            reviews_tab.click()
            print("Clicked on 'Reviews' tab.")

        except Exception as e:
            print("Error:", e)

#############################################################################################
    def click_newest_button(self):
        '''Click the "Newest" button or restaurant page to order the reviews.''' 
        
        try:
            # Wait for the element to be present
            element = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//div[@class='YCQwEb btku5b k0Jjg A29zgf LwdV0e zqrO0 sLl7de rlt7Ub PrjL8c ' and @data-sort='2' and @jsaction='click:pwGece;' and @jsname='XPtOyb' and @role='radio' and @aria-checked='false']"))
            )
            # Scroll the element into view using JavaScript
            self.driver.execute_script("arguments[0].scrollIntoView(true);", element)
            sleep(1)  # Wait for any scrolling effects to settle

            # Click the element using JavaScript to avoid interception issues
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
    def auto_scroller(self, pixels=1000):
        """Scroll to the bottom of page to load more reviews."""

        try:
            # locate reviews window
            immersive_container = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CLASS_NAME, 'immersive-container'))
                    )            
            print("Found immersive container.")

            one_year_of_reviews = False
            while not one_year_of_reviews:
                self.driver.find_element(By.XPATH, "//div[@class='TQc1id k5T88b' and @id='rhs' and @role='complementary']").send_keys(Keys.END)
                print('scrolling...')
                sleep(2)
                print(immersive_container.text)

                # check if the phrase "one year ago" is in the element's text content
                if "one year ago" in immersive_container.text.lower():
                    one_year_of_reviews = True
                    print('One year of reviews loaded.')
                    break

        except Exception as e:
            print("Error scrolling reviews:", e)

#############################################################################################
    def scroll_to_bottom(self):
            
            try:
                element = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//c-wiz[@jsrenderer='uif9Kd' and @class='u1M3kd ucRBdc Y0KcTc']"))
                )
                print("Found c-wiz element.")
                
                last_height = self.driver.execute_script("return arguments[0].scrollHeight", element)
                while True:
                    # Scroll down to bottom
                    self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", element)
                    print('scrolling...')
                    # Wait to load page
                    sleep(2)
                    # Calculate new scroll height and compare with last scroll height
                    new_height = self.driver.execute_script("return arguments[0].scrollHeight", element)
                    if new_height == last_height:
                        break
                    last_height = new_height

                # locate reviews window
                immersive_container = WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located((By.CLASS_NAME, 'immersive-container'))
                        )            
                print("Found immersive container.")
                print(immersive_container.text)

            except Exception as e:
                print("Error scrolling reviews:", e)
            

#############################################################################################
    def scrape_resturant_page_reviews(self):
        """Scrape review data from restuarnt's google page."""
        
        pass

