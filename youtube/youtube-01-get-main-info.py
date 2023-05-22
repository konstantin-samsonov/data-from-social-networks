"""
Google YouTube API Guide:
- https://developers.google.com/youtube/v3/docs
- https://developers.google.com/youtube/v3/determine_quota_cost
"""

import logging
import sqlite3
from datetime import datetime, timezone
from tqdm import tqdm
import pandas as pd
import googleapiclient.discovery
from google.oauth2 import service_account
from settings import BIGQUERY

logger = logging.getLogger('logger')
handler = logging.FileHandler('youtube_channels.log')
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)


def yt_connect():
    """ Return connect to YouTube API """

    scopes = ["https://www.googleapis.com/auth/youtube.readonly"]
    credentials = service_account.Credentials.from_service_account_file(BIGQUERY['service_account_key'])
    connect = googleapiclient.discovery.build('youtube', 'v3', credentials=credentials)

    return connect


def yt_get_channel_info(yt_connect=None, channel_id=None,
                        sql_connect=None, sql_table=None, sql_if_exists=None, sql_index=False):
    def parse_dict(item, target):
        """ Returns a dictionary of target elements """

        result = {}
        for i in target:
            if i in item.keys():
                result = result | {i: item[i]}
            else:
                result = result | {i: None}

        return result

    parts = 'snippet, contentDetails, statistics, status, brandingSettings, auditDetails'

    request = yt_connect.channels().list(
        id=channel_id,
        part=parts,
        maxResults=50
    ).execute()

    results = []
    for item in request['items']:
        main = {
            'ts_utc': datetime.now(tz=timezone.utc),
            'channel_id': item['id']
        }

        # snippet
        snippet_item = item['snippet']
        snippet_targets = ['title', 'description', 'customUrl', 'publishedAt', 'defaultLanguage', 'country']
        snippet_results = parse_dict(item=snippet_item, target=snippet_targets)

        # contentDetails
        content_item = item['contentDetails']['relatedPlaylists']
        content_targets = ['uploads']
        content_results = parse_dict(item=content_item, target=content_targets)

        # statistics
        statistics_item = item['statistics']
        statistics_targets = ['viewCount', 'subscriberCount', 'hiddenSubscriberCount', 'videoCount']
        statistics_results = parse_dict(item=statistics_item, target=statistics_targets)

        # status
        status_item = item['status']
        status_targets = ['madeForKids']
        status_results = parse_dict(item=status_item, target=status_targets)

        # brandingSettings
        branding_item = item['brandingSettings']['channel']
        branding_targets = ['keywords', 'moderateComments']
        branding_results = parse_dict(item=branding_item, target=branding_targets)

        row = {**main, **snippet_results, **content_results,
               **statistics_results, **status_results, **branding_results
               }

        results.append(row)

    df = pd.DataFrame(results)
    df.to_sql(con=sql_connect, name=sql_table, if_exists=sql_if_exists, index=False)


def main():
    yt_con = yt_connect()
    sql_con = sqlite3.connect('data.db')

    sql = """SELECT youtube_id FROM statsvideo"""

    channels_id = pd.read_sql_query(con=sql_con, sql=sql, chunksize=50)
    for chunk in tqdm(channels_id):
        chunk = ','.join(chunk['youtube_id'])

        yt_get_channel_info(yt_connect=yt_con, channel_id=chunk,
                            sql_connect=sql_con, sql_table='youtube_channels', sql_if_exists='append')


if __name__ == '__main__':
    main()
