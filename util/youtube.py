"""
Google YouTube API Guide:
- https://developers.google.com/youtube/v3/docs
- https://developers.google.com/youtube/v3/determine_quota_cost
"""

from datetime import datetime, timezone

import googleapiclient.discovery
from google.oauth2 import service_account
from settings import GCP


def yt_connect():
    """ Return connect to YouTube API """

    scopes = ["https://www.googleapis.com/auth/youtube.readonly"]
    credentials = service_account.Credentials.from_service_account_file(GCP['service_account_key'])
    connect = googleapiclient.discovery.build('youtube', 'v3', credentials=credentials)

    return connect


def yt_get_all_info_about_channel(yt_connect=None, channel_id=None):
    parts = 'snippet, contentDetails, statistics, topicDetails, status, brandingSettings, auditDetails, contentOwnerDetails, localizations'

    request = yt_connect.channels().list(
        id=channel_id,
        part=parts,
        maxResults=50
    ).execute()

    return request


def yt_get_channel_stats(yt_connect=None, channel_id=None):
    def parse_dict(item, target):
        """ Returns a dictionary of target elements """

        result = {}
        for i in target:
            if i in item.keys():
                result = result | {i: item[i]}
            else:
                result = result | {i: None}

        return result

    parts = 'statistics'

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

        # statistics
        statistics_item = item['statistics']
        statistics_targets = ['viewCount', 'subscriberCount', 'videoCount']
        statistics_results = parse_dict(item=statistics_item, target=statistics_targets)

        row = {**main, **statistics_results}

        results.append(row)


    return results

    # df = pd.DataFrame(results)
    # df.to_sql(con=sql_connect, name=sql_table, if_exists=sql_if_exists, index=False)
