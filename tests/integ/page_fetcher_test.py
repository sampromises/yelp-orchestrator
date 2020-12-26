import os
import time
from collections import namedtuple
from urllib.parse import quote_plus

import boto3

URL_TABLE_NAME = os.environ["URL_TABLE_NAME"]
PAGE_BUCKET_NAME = os.environ["PAGE_BUCKET_NAME"]
PAGE_FETCHER_LAMBDA_NAME = os.environ["PAGE_FETCHER_LAMBDA_NAME"]

DDB_RESOURCE = boto3.resource("dynamodb")
URL_TABLE = DDB_RESOURCE.Table(URL_TABLE_NAME)
PAGE_FETCHER_LAMBDA = boto3.client("lambda")
S3 = boto3.resource("s3")

MIN_TIME = -1000000  # A value that will safely be less than any timestamp

Url = namedtuple("Url", ["url", "last_fetched"])


def test_batch():
    """Note: Cannot run other tests in parallel because PageFetcher will get the N oldest."""
    items = (
        Url("https://google.com", MIN_TIME),
        Url("https://duckduckgo.com", MIN_TIME),
        Url("https://yahoo.com", MIN_TIME),
        Url("https://bing.com", MIN_TIME),
        Url("https://apple.com", MIN_TIME),
        Url("https://espn.com", MIN_TIME),
        Url("https://yelp.com", MIN_TIME),
        Url("https://reddit.com", MIN_TIME),
        Url("https://python.org", MIN_TIME),
        Url("ERROR_BUT_SHOULDNT_AFFECT_OTHER_URLS", MIN_TIME),
        Url("https://stackoverflow.com", MIN_TIME + 1),  # Should be ignored
    )
    valid_items = items[:9]
    invalid_item = items[-2]
    try:
        # Prepare UrlTable
        _put_items(items)

        # Invoke PageFetcher
        invoke_time = int(time.time())
        assert _invoke_page_fetcher()

        # Verify UrlTable timestamps updated
        updated_items = _get_items(valid_items)
        for item in updated_items:
            assert int(item.get("LastFetched")) >= invoke_time

        # Verify invalid URL marked with Error
        updated_invalid_item = _get_items([invalid_item])[0]
        assert updated_invalid_item.get("ErrorMessage")

        # Verify S3 pages uploaded
        pages = _get_pages(valid_items)
        for page in pages:
            assert page["ContentLength"]
            assert page["LastModified"].timestamp() >= invoke_time
    finally:
        _delete_items(items)
        _delete_pages(items)


def _put_items(items):
    with URL_TABLE.batch_writer() as batch:
        for url, last_fetched in items:
            batch.put_item(Item={"Url": url, "LastFetched": last_fetched})


def _get_items(items):
    keys = list(map(lambda item: {"Url": item.url}, items))
    resp = DDB_RESOURCE.batch_get_item(RequestItems={URL_TABLE_NAME: {"Keys": keys}})
    if resp.get("UnprocessedKeys"):
        raise Exception("Unable to get all items in one batch_get_item call")
    return resp["Responses"][URL_TABLE_NAME]


def _delete_items(items):
    with URL_TABLE.batch_writer() as batch:
        for url, _ in items:
            batch.delete_item(Key={"Url": url})


def _get_pages(items):
    return [
        S3.Object(PAGE_BUCKET_NAME, key).get()
        for key in map(lambda item: quote_plus(item.url), items)
    ]


def _delete_pages(items):
    for item in items:
        key = quote_plus(item.url)
        S3.Object(PAGE_BUCKET_NAME, key).delete()


def _invoke_page_fetcher():
    resp = PAGE_FETCHER_LAMBDA.invoke(FunctionName=PAGE_FETCHER_LAMBDA_NAME)
    return resp["StatusCode"] == 200
