"""
    Review Aggregator
    Joseph Nelson Farrell
    05-14-24

    This file will scrape OpenTable using OpenTableScraper Class
"""
###################################################################################################################
# libraries
import pandas as pd
from pathlib import Path
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from scrapers.scraper_classes.opentable_scraper_restaurant_list import OpenTableScraperRestaurantList

###################################################################################################################
# main
def main():
    # get the base dir
    HOME = Path.cwd()

    # set the base_url to be OpenTable homepage    
    URL = f"https://www.opentable.com"

    # set the region you want to scrape
    region = "Portland, ME"
    state = "Maine"
    max_pages = 1

    # use yelp as restaurant guide; i.e., run Yelp scraper first to extract restaurants in a region
    yelp_res_data_df = pd.read_csv(str(HOME) + "/data/raw/yelp_restaurant_data_Portland_ME_2024-06-29.csv")

    res_list = list(yelp_res_data_df["name"])
    num_res = len(res_list)

    restaurant_data = []
    review_data = []
    failed_list = []
    for index, res in enumerate(res_list):
        print(f"Scraping restaurant: {res} - {index + 1}/{num_res}")
        try:
            scraper = OpenTableScraperRestaurantList(URL, region, state, res)
            scraper.go_to_base_url()
            got_to_res = scraper.go_to_restaurant_with_timeout(10)
            if not got_to_res:
                scraper.driver.close()
                scraper.driver.quit()
                print(f"Failed to navigate to restaurant: {res}")
                continue
            res_located = scraper.click_res_link()
            if not res_located:
                scraper.driver.close()
                scraper.driver.quit()
                continue
            scraper.switch_to_new_tab()
            scraper.get_restaurant_url()
            correct_state = scraper.get_restaurant_data()
            if not correct_state:
                print("Restaurant is in incorrect state.")
                scraper.driver.close()
                scraper.driver.quit()
                continue
            scraper.scrape_individual_restaurant(max_pages)
            restaurant_data.extend(scraper.restaurant_data)
            review_data.extend(scraper.review_data)
            scraper.driver.close()
            scraper.driver.quit()
            
        except Exception as e:
            print(f"Error processing restaraunt: {res}")
            failed_list.append(res)
            continue

    # modify the region variable to use as part of file name
    region_modified = scraper.region.replace(", ", "_")
    
    # save review data as csv
    open_table_review_data_df = pd.DataFrame(review_data)
    SAVE_PATH = HOME / "data" / "raw" / f"open_table_review_data_{region_modified}_{scraper.date}.csv"
    open_table_review_data_df.to_csv(str(SAVE_PATH))

    # save restaurant data as csv
    open_table_restaurant_data_df = pd.DataFrame(restaurant_data)
    SAVE_PATH = HOME / "data" / "raw" / f"open_table_restaurant_data_{region_modified}_{scraper.date}.csv"
    open_table_restaurant_data_df.to_csv(str(SAVE_PATH))

if __name__ == "__main__":
    main()