import os
from dotenv import load_dotenv

load_dotenv('../credentials/.env')

GCP_BIGQUERY = os.getenv('SKV_GCP_BIGQUERY')
