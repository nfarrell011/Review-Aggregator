"""
    Review Aggregator
    Joseph Nelson Farrell
    05-14-24

    This file will scrape Yelp using YelpScraper Class
"""
# packages and modules
from yelp_scraper_class import YelpScraper
import pandas as pd
from pathlib import Path

# main
def main():
    """
        This will scrape Yelp starting at the URL varible page.
    """
    HOME = Path.cwd()
    URL = "https://www.yelp.com"
    region = "Portland, ME"
    business_type = "Restaurants"
    scraper = YelpScraper(URL, region, business_type)
    scraper.go_to_region()
    scraper.enter_business_type()
    scraper.navigate_pages_get_res_urls()
    scraper.remove_unwanted_urls()
    scraper.go_to_restaurant_url_extract_data()

    # modify the regoin variable to use as part of file name
    region_modified = scraper.region.replace(", ", "_")

    # save review data as csv
    yelp_review_data_df = pd.DataFrame(scraper.review_data)
    SAVE_PATH = HOME / "data" / "raw" / f"yelp_review_data_{region_modified}_{scraper.date}.csv"
    yelp_review_data_df.to_csv(str(SAVE_PATH))

    # save restaurant data as csv
    yelp_restaurant_data_df = pd.DataFrame(scraper.restaurant_data)
    SAVE_PATH = HOME / "data" / "raw" / f"yelp_restaurant_data_{region_modified}_{scraper.date}.csv"
    yelp_restaurant_data_df.to_csv(str(SAVE_PATH))

if __name__ == "__main__":
    main()