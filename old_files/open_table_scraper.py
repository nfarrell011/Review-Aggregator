import requests
from requests_html import HTMLSession
from bs4 import BeautifulSoup
import pandas as pd
import json
import nltk
from nltk.tokenize import word_tokenize
nltk.download('punkt')

from find_the_restaurants import incremental_scroll, get_restaurant_urls


def get_total_pages_for_restaurant(url):
    
    # start html session
    session = HTMLSession()

    # access the url
    response = session.get(url)

    # instantiate Soup object
    soup = BeautifulSoup(response.text, "html.parser")
    
    # find all the 'scripts'
    soup_find_total_page = soup.find_all('script')

    # token the words in the second last script
    words = word_tokenize(str(soup_find_total_page[-2]))

    # this will find the total pages
    l = []
    flag = 0
    for i in words:
        if i == 'totalPages':
            flag = 1
        if flag == 1:
            l.append(i)
    total_pages = int(l[2].replace(":", ""))
    return total_pages

def grab_review_data(review):

    # this extracts the hometown of the reviewer
    reviewer_hometown = review.find('p', class_ = 'POyqzNMT21k- C7Tp-bANpE4-').text

    # this extracts when the review was made
    review_datelike = review.find('p', class_ = 'iLkEeQbexGs-').text

    # this will extract the review text
    review_text = review.find('span', class_ = 'l9bbXUdC9v0- ZatlKKd1hyc- ukvN6yaH1Ds-').text

    # this extracts list of ratings left by the user
    ratings_list = review.find('ol', class_ = 'gUG3MNkU6Hc- ciu9fF9m-z0-')

    # results container
    results_dict = {}

    # this will parse out individual ratings and update results_dict
    for rating in ratings_list:
        category = rating.contents[0].strip()
        value = rating.find('span').text
        results_dict[category] = value

    # this will update results_dict
    results_dict["review_text"] = review_text
    results_dict["hometown"] = reviewer_hometown
    results_dict['datelike'] = review_datelike

    return results_dict

def scrape_individual_restaurant(base_url, results_list):
    
    # define page traker
    tracker = 1

    # add tag to the base url 
    url = base_url + f"&page={tracker}&sortBy=newestReview"
    num_pages = get_total_pages_for_restaurant(url)

    while (tracker < 20) and (tracker < num_pages):

        # define the url
        url = f"{base_url}&sortBy=newestReview&page={tracker}&sortBy=newestReview"

        try: 
            # start html session
            session = HTMLSession()

            # access the url
            response = session.get(url, timeout = 10)

            if response.status_code != 200:
                print("Failed to load page...continuing")
                continue

        except requests.exceptions.RequestException as e:
            print(f"An error occured: {e}")

        # instantiate Soup object
        soup = BeautifulSoup(response.text, "html.parser")

        # get restaurant name
        main = soup.find('main', class_ = "mwul4aJazVU-")
        name = main.find('script', type = 'application/ld+json')

        # it's a json, so load the json
        if name:
            data = json.loads(name.string)
            restaurant_name = data.get('name', 'name_not_found')

        print(f'This restuarant has: {num_pages}')
        print(f'Currently scraping page {tracker}')

        # this grabs the entire review
        reviews = soup.find_all('li', class_ = 'afkKaa-4T28-')
        print("Length of Reviews is: ", len(reviews))
        print()

        for review in reviews:
            results_dict = grab_review_data(review)
            results_dict['res_name'] = restaurant_name
            results_dict['origins'] = "open_table"
            results_list.append(results_dict)
        tracker += 1

    return results_list

url = f"https://www.opentable.com/s?dateTime=2024-05-10T17%3A00%3A00&covers=2&metroId=3569&regionIds%5B%5D=10734&neighborhoodIds%5B%5D=&term=&shouldUseLatLongSearch=false&originCorrelationId=a860a793-437a-4bed-b01b-3a9afc98fdcf"

restaurant_urls = get_restaurant_urls(url)

print(len(restaurant_urls))
results_list = []
for url in restaurant_urls:
    results_list = scrape_individual_restaurant(url, results_list)

results_df = pd.DataFrame(results_list)
print(results_df.shape)

SAVE_PATH = "review_app/scrappers/raw/open_table_data.csv"

results_df.to_csv(SAVE_PATH)