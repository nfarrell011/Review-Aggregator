"""
    Review Aggregator
    Michael Massone
    05-23-24

    This file will scrape Google Reviews using GoogleScraper Class
"""
#############################################################################################
## packages and modules
#############################################################################################

# packages and modules
import pandas as pd
from pathlib import Path
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from scrapers.scraper_classes.google_scraper_class import GoogleScraper
from time import sleep

#############################################################################################
## Main
#############################################################################################

def main():
    #URL = f"https://www.google.com/"

    scraper = GoogleScraper()
    scraper.google_search()
    scraper.get_reviews()
    scraper.extract_review_data()
    scraper.next_page()
    

    scraper.driver.quit()

    """
    df = pd.DataFrame(scraper.results_list)
    SAVE_PATH = "scrapers/raw/google_reviews_data.csv"
    df.to_csv(SAVE_PATH)
    """

#############################################################################################
## END
#############################################################################################

if __name__ == "__main__":
    main()
