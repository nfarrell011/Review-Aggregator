"""
    Review Aggregator
    Joseph Nelson Farrell
    05-14-24

    This file will scrape OpenTable using OpenTableScraper Class
"""
# packages and modules
import pandas as pd
from opentable_scraper_class import OpenTableScraper

# main
def main():
    URL = f"https://www.opentable.com/s?dateTime=2024-05-10T17%3A00%3A00&covers=2&metroId=3569&regionIds%5B%5D= \
            10734&neighborhoodIds%5B%5D=&term=&shouldUseLatLongSearch=false&originCorrelationId=a860a \
            793-437a-4bed-b01b-3a9afc98fdcf"

    scraper = OpenTableScraper(URL)
    scraper.get_restaurant_urls()
    for href in scraper.hrefs:
        scraper.scrape_individual_restaurant(href)

    df = pd.DataFrame(scraper.results_list)
    SAVE_PATH = "scrapers/raw/open_table_data.csv"
    df.to_csv(SAVE_PATH)

if __name__ == "__main__":
    main()
