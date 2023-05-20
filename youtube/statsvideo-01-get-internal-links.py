"""
This script collects YouTube channel_id data from the site https://stats.video
"""
import time
import csv
import requests
from bs4 import BeautifulSoup
import pandas as pd
import tqdm
from multiprocessing import Pool
from settings import BIGQUERY
from google.oauth2 import service_account


def get_html(url):
    """Return HTML page"""

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7'
    }

    r = requests.get(url, headers=headers)
    return r.text


def get_internal_links(html):
    """Returns all internal links to channels shown on the results page"""

    soup = BeautifulSoup(html, 'lxml')
    trs = soup.find('table', class_='table table-striped').find('tbody').find_all('tr')

    for tr in trs:
        tds = tr.find_all('td')
        internal_link = tds[0].find('a', class_="channelLink").get('href')

        data = {
            'country': 'russia',
            'rating_name': 'of-all-time',
            'timestamp': int(time.time()),
            'internal_link': 'https://stats.video' + str(internal_link)
        }

        write_csv(data)


def write_csv(data):
    """Write to csv file"""

    with open('statsvideo-internal-links.csv', 'a') as file:
        meta = ['country', 'rating_name', 'timestamp', 'internal_link']
        writer = csv.DictWriter(file, fieldnames=meta)
        writer.writerow(data)


def make_all(url):
    try:
        html = get_html(url)
        get_internal_links(html)
        time.sleep(0.5)
    except:
        print(url)


def main():
    url = 'https://stats.video/top/most-viewed/youtube-channels/{}/{}/page/{}'
    target_country = 'russia'
    rating_name = 'of-all-time'
    total_page = 631

    urls = [url.format(target_country, rating_name, str(i)) for i in range(1, total_page + 1)]

    with Pool(50) as p:
        r = list(tqdm.tqdm(p.imap(make_all, urls), total=len(urls)))


if __name__ == '__main__':
    main()

    credentials = service_account.Credentials.from_service_account_file(BIGQUERY['service_account_key'])
    df = pd.read_csv('statsvideo-internal-links.csv', header=None,
                     names=['country', 'rating_name', 'timestamp', 'internal_link'])

    df.to_gbq(destination_table=BIGQUERY['table_statsvideo_internal_links'],
              project_id=BIGQUERY['project_id'],
              if_exists='append'
              )