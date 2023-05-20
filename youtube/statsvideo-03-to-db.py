import pandas as pd
import sqlite3
from settings import BIGQUERY
from google.oauth2 import service_account

con = sqlite3.connect('data.db')

one = pd.read_csv('statsvideo-internal-links.csv', header=None,
                  names=['country', 'rating_name', 'timestamp', 'internal_link'])

two = pd.read_csv('statsvideo-youtube-links2.csv', header=None,
                  names=['internal_link', 'youtube_link'])

three = pd.read_csv('statsvideo-youtube-links2.1.csv', header=None,
                    names=['internal_link', 'youtube_link'])

res_one = one.merge(two, how='left', left_on='internal_link', right_on='internal_link')

res_two = res_one[res_one['youtube_link'] == 'error'].copy()
res_two.drop('youtube_link', axis=1, inplace=True)

res_two = res_two.merge(three, how='left', left_on='internal_link', right_on='internal_link')
res_one = res_one[res_one['youtube_link'] != 'error'].copy()

res = pd.concat([res_one, res_two])
res['youtube_id'] = res['youtube_link'].str.split('channel/').str[1]

res.to_sql(con=con, name='statsvideo', if_exists='replace', index=False)

credentials = service_account.Credentials.from_service_account_file(BIGQUERY['service_account_key'])
res.to_gbq(
    destination_table=BIGQUERY['table_statsvideo'],
    project_id=BIGQUERY['project_id'],
    if_exists='append',
    credentials=credentials
              )

