"""
    Review Aggregator
    Joseph Nelson Farrell
    05-14-24

    This file will scrape OpenTable using OpenTableScraper Class
"""
# packages and modules
import pandas as pd
from pathlib import Path
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from scrapers.scraper_classes.opentable_scraper_region_class import OpenTableScraper

# main
def main():
    # get the base dir
    HOME = Path.cwd()

    # set the base_url to be OpenTable homepage    
    URL = f"https://www.opentable.com"

    # set the regoin you want to scrape
    region = "Portland, ME"

    # instaniate scraper
    scraper = OpenTableScraper(URL, region)
    scraper.go_to_region()
    scraper.get_restaurant_urls()
    for href in scraper.hrefs:
        scraper.get_restaurant_data(href)
        scraper.scrape_individual_restaurant(href)

    # modify the regoin variable to use as part of file name
    region_modified = scraper.region.replace(", ", "_")
    
    # save review data as csv
    open_table_review_data_df = pd.DataFrame(scraper.review_data)
    SAVE_PATH = HOME / "data" /"raw" / f"open_table_review_data_{region_modified}_{scraper.date}.csv"
    open_table_review_data_df.to_csv(str(SAVE_PATH))

    # save restaurant data as csv
    open_table_restaurant_data_df = pd.DataFrame(scraper.restaurant_data)
    SAVE_PATH = HOME / "data" / "raw" / f"open_table_restaurant_data_{region_modified}_{scraper.date}.csv"
    open_table_restaurant_data_df.to_csv(str(SAVE_PATH))

if __name__ == "__main__":
    main()
