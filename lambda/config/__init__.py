import os

PAGE_BUCKET_NAME = os.environ["PAGE_BUCKET_NAME"]
URL_TABLE_NAME = os.environ["URL_TABLE_NAME"]
YELP_TABLE_NAME = os.environ["YELP_TABLE_NAME"]

FETCH_BATCH_SIZE = int(os.environ["FETCH_BATCH_SIZE"])
URL_TABLE_TTL = int(os.environ["URL_TABLE_TTL"])
YELP_TABLE_TTL = int(os.environ["YELP_TABLE_TTL"])
