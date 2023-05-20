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


def get_channel_link(url):
    """"""

    try:
        html = get_html(url)
        soup = BeautifulSoup(html, 'lxml')
        youtube_link = soup.find('div', class_='col-12 p-2 greyGradient').find('a').get('href')

        # trs = soup.find('table', class_='table table-striped').find('tbody').find_all('tr')
        # youtube_link = soup.find('div', class_='col-12 p-2 greyGradient').find('a').get('href')

        data = {
            'internal_link': url,
            'youtube_link': youtube_link
        }

        write_csv(data)

    except:

        data = {
            'internal_link': url,
            'youtube_link': 'error'
        }

        write_csv(data)


def write_csv(data):
    """Write to csv file"""

    with open('statsvideo-youtube-links2.1.csv', 'a') as file:
        meta = ['internal_link', 'youtube_link']
        writer = csv.DictWriter(file, fieldnames=meta)
        writer.writerow(data)


def make_all(url):
    try:
        get_channel_link(url)
        time.sleep(0.5)
    except:
        print(url)


def main():

    df = pd.read_csv('statsvideo-youtube-links2.csv', header=None,
                     names=['internal_link', 'youtube'])

    all_links = df[df['youtube']=='error']['internal_link'].to_list()

    with Pool(50) as p:
        r = list(tqdm.tqdm(p.imap(make_all, all_links), total=len(all_links)))


if __name__ == '__main__':
    main()
