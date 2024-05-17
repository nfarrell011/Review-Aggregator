"""
    Review Aggregator
    Joseph Nelson Farrell
    05-14-24

    This file will scrape Yelp using YelpScraper Class
"""
# packages and modules
from yelp_scraper_class import YelpScraper
import pandas as pd  

# main
def main():
    """
        This will scrape Yelp starting at the URL varible page.
    """
    URL = "https://www.yelp.com/search?find_desc=Restaurants&find_loc=Portland%2C+ME"
    scraper = YelpScraper(URL)
    scraper.navigate_pages_get_res_urls()
    scraper.remove_unwanted_urls()
    scraper.go_to_restaurant_url_extract_data()


    df = pd.DataFrame(scraper.results_list)
    SAVE_PATH = "scrapers/raw/yelp_data.csv"
    df.to_csv(SAVE_PATH)

if __name__ == "__main__":
    main()