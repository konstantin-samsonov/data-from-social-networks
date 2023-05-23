import sqlite3
import pandas as pd
from tqdm import tqdm

from util import yt_connect, yt_get_channel_stats


def main():
    yt_con = yt_connect()
    sql_con = sqlite3.connect('data.db')

    sql = """SELECT channel_id FROM youtube_channels"""

    channels_id = pd.read_sql_query(con=sql_con, sql=sql, chunksize=50)
    for chunk in tqdm(channels_id):
        chunk = ','.join(chunk['channel_id'])
        result = yt_get_channel_stats(yt_connect=yt_con, channel_id=chunk)
        df = pd.DataFrame(result)
        df.to_sql(con=sql_con, name='youtube_channels_daily', if_exists='append', index=False)


if __name__ == "__main__":
    main()
