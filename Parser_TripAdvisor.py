# importing libraries we need
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import pandas as pd
import time
import csv
import re
from multiprocessing import Pool
import tqdm

# reading base dataset
df = pd.read_csv('main_task.csv')

# Creating the dict from parts of URL given in original CSV. They will be used in parsing
url_dict = df.URL_TA.to_dict()

# updating fake user agent
user_agent = UserAgent()
user_agent.update()

# setting up a csv File for full parsing
# ROWS: 'Link', 'Status', 'Supervised','Kitchen_style_new','Rating', 'Price_range_new', 'Reviews_10',
# 'Reviews_10_head', "Number_of_reviews_new"))
csvFile = open('Food_parse.csv', 'w', encoding='utf-8')
writer = csv.writer(csvFile)
writer.writerow(('Link', 'Status', 'Supervised', 'Kitchen_style_new', 'Rating',
                 'Price_range_new', 'Reviews_10', 'Head_Reviews_head', "Number_of_reviews_new"))
csvFile.close()


# Technical part revisited

# Defining csv writer function. It will be used from the start. You need to pass 7 lines

def write_to_csv(link, status_code, registration, kitchen_style, rating, price, clean_reviews,
                 review_headers, number_of_reviews):
    # this link is for final parse
    with open('Food_parse.csv', 'a', encoding='utf-8') as append_file:
        writer2 = csv.writer(append_file)
        writer2.writerow((link, status_code, registration, kitchen_style, rating, price, clean_reviews,
                          review_headers, number_of_reviews))

    # csvFile = open('Food_parse.csv', 'a', encoding = 'utf-8')
    # writer.writerow((link, status_code, registration, kitchen_style, rating, price, clean_reviews,
    #              review_headers, number_of_reviews))
    # csvFile.close()


# crawling function - try to reach site or try to switch user-agent

def crawler(link):
    try:
        site = requests.get(link, headers={'User-Agent': user_agent.chrome})
        status_code = site.status_code
        soup = BeautifulSoup(site.text, 'html.parser')

    except:
        time.sleep(10)
        site = requests.get(link, headers={'User-Agent': user_agent.random})
        status_code = site.status_code
        soup = BeautifulSoup(site.text, 'html.parser')

    return site, status_code, soup


# checking if the restaurant page is being revised by owner or not. Also we check if the restaurant is closed
# and its page is deleted

def supervised_restaurant(soup):
    try:
        is_registered = soup.find('div', class_='_3bSXp6ba').text.strip()

        if 'Claimed' in is_registered:
            registration = 1

        else:
            registration = 0

    except:

        registration = 404

    return registration


# This function is helper, needed to track the position of CUISINE block.
# At some pages it can be first, second or... ABSENT.
def index_find(soup):
    try:
        cuisine_index = soup.find_all('div', class_='_14zKtJkz')
        cuisine_index = [link.text for link in cuisine_index]
        cuisine_index = cuisine_index.index('CUISINES')

    except:
        cuisine_index = 404

    return cuisine_index


# This function tracks kitchen style. We look at the first place, then second. If unsuccessful we put "none".
def kitchen_style_grabber(kitchen_style, soup):
    cuisine_index = index_find(soup)

    if cuisine_index == 404:
        kitchen_style.append('None')
        return kitchen_style

    else:
        try:
            cuisine_index = index_find(soup)
            kitchen_style.extend(
                soup.find_all('div', class_='_1XLfiSsv')[cuisine_index].text.replace(', ', ',').split(','))
            return kitchen_style

        except:

            kitchen_style.extend(soup.find('div', class_='_3dyNdB6_').text.replace(', ', ',').split(','))
            return kitchen_style


#             kitchen_style.append('None')
#             return kitchen_style


# parsing rating (1.0 - 5.0) and price. If price is not defined we put 0 (price marked with $ symbol)

def get_rating(soup):
    try:
        rating = float(soup.find('span', class_="r2Cf69qf").text)
        return rating

    except:
        rating = 0
        return rating


def get_price(soup):
    try:
        price = str(soup.find('a', class_="_2mn01bsa").text)
        if '$' not in price:
            price = 0
        return price

    except:
        price = 0
        return price


# parsing number of reviews
def get_number_of_reviews(soup):
    try:
        number_raw = soup.find('a', class_='_10Iv7dOs').text
        number_of_reviews = int(re.match(r'\d+', number_raw).group(0))

        return number_of_reviews

    except:
        number_of_reviews = 0

        return number_of_reviews


# parsing reviews. We take top-10
def get_reviews(soup):
    try:
        pre_review = soup.find_all('p', class_='partial_entry')
        reviews10 = [link.text for link in pre_review]

        if (len(reviews10) == 1) & ('More' in reviews10):
            reviews10 = ['NOT REVIEWED']
        clean_reviews = [review[:-4] if review[-4:] == 'More' else review for review in reviews10]

        pre_review_header = soup.find_all('span', class_='noQuotes')
        review_headers = [link.text for link in pre_review_header]

        if (len(review_headers) == 1) & (not review_headers[0]):
            review_headers = ['NOT REVIEWED']

        return clean_reviews, review_headers

    except:
        review_headers = ['NOT REVIEWED']
        clean_reviews = ['NOT REVIEWED']

        return clean_reviews, review_headers


# this function executes all other parsing functions
def soup_execute(link):
    print('Parsing url:', link)

    site, status_code, soup = crawler(link)
    if status_code != 200:
        write_to_csv(link, status_code, 'failed', 'failed', 'failed', 'failed', 'failed',
                     'failed', 'failed')
        return

    else:
        registration = supervised_restaurant(soup)

        if registration == 404:
            write_to_csv(link, status_code, 'closed', 'closed', 'closed', 'closed', 'closed',
                         'closed', 'closed')
            return

        else:

            kitchen_style = []
            kitchen_style_grabber(kitchen_style, soup)

            rating = get_rating(soup)
            price = get_price(soup)

            clean_reviews, review_headers = get_reviews(soup)
            number_of_reviews = get_number_of_reviews(soup)

            write_to_csv(link, status_code, registration, kitchen_style, rating, price, clean_reviews,
                         review_headers, number_of_reviews)

    time.sleep(3)


# creating list for iteration

site_list = list('https://www.tripadvisor.com' + record for record in url_dict.values())

# and finally we launch

if __name__ == '__main__':
    with Pool(8) as p:
        list(tqdm.tqdm(p.imap(soup_execute, site_list), total=len(site_list)))
        p.close()
        p.join()
