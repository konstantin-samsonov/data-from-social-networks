"""
This script collects YouTube channel_id data from the site https://stats.video
"""
import time
import requests
from bs4 import BeautifulSoup
from sqlite3 import connect
import pandas as pd
from tqdm.auto import tqdm
from multiprocessing.pool import ThreadPool
from settings import BIGQUERY
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(BIGQUERY['service_account_key'])


def get_html(url):
    """Return HTML page"""

    r = requests.get(url)
    return r.text


def get_internal_links(html):
    """Returns all internal links to channels shown on the results page"""

    soup = BeautifulSoup(html, 'lxml')
    trs = soup.find('table', class_='table table-striped').find('tbody').find_all('tr')
    all_rows = []

    for tr in trs:
        tds = tr.find_all('td')
        internal_link = tds[0].find('a', class_="channelLink").get('href')
        all_rows.append('https://stats.video' + internal_link)

    return all_rows


def get_channel_link(url):
    pass
    # youtube_link = soup.find('div', class_='col-12 p-2 greyGradient').find('a').get('href')


def save_data():
    pass


def make_all(url):
    ts = int(time.time())
    text = get_html(url)
    result = get_internal_links(text)
    df = pd.DataFrame(data={'internal_link': result,
                            'country': 'russia',
                            'ts': ts
                            })

    df.to_gbq(destination_table=BIGQUERY['table_statsvideo_internal_links'],
              project_id=BIGQUERY['project_id'],
              if_exists='append',
              credentials=credentials)



def main():
    url = 'https://stats.video/top/most-viewed/youtube-channels/{}/of-all-time/page/{}'
    target_country = 'russia'
    total_page = 2

    urls = [url.format(str(target_country), str(i)) for i in range(1, total_page + 1)]

    with ThreadPool(20) as p:
        tqdm(p.map(make_all, urls))



if __name__ == '__main__':
    main()
