import os
import time

import boto3
from yelp.persistence.url_table import get_all_url_items, update_fetched_url, upsert_new_url

URL_TABLE_NAME = os.environ["URL_TABLE_NAME"]
URL_TABLE = boto3.resource("dynamodb").Table(URL_TABLE_NAME)


def test_upserting_new_urls():
    # Given
    urls = (
        "https://github.com",
        "https://github.com/explore",
        "https://github.com/marketplace",
    )
    ttl = 1000

    try:
        # When
        insert_time = int(time.time())
        for url in urls:
            upsert_new_url(url, ttl)
        fetched_test_items = list(filter(lambda item: item["Url"] in urls, get_all_url_items()))

        # Then
        assert len(fetched_test_items) == len(urls)
        assert all(map(lambda item: item.get("TimeToLive") > insert_time, fetched_test_items))

    finally:
        # Cleanup
        _delete_urls(urls)


def test_updating_fetched_urls():
    # Given
    urls = (
        "https://github.com",
        "https://github.com/explore",
        "https://github.com/marketplace",
    )
    ok_urls = urls[:2]
    error_urls = urls[2:]

    try:
        # When
        for url in urls:
            upsert_new_url(url, 1000)

        update_time = int(time.time())
        for url in ok_urls:
            update_fetched_url(url)
        for url in error_urls:
            update_fetched_url(url, "FakeErrorMessage")
        items = get_all_url_items()

        # Then
        fetched_ok_urls_items = list(filter(lambda item: item["Url"] in ok_urls, items))
        assert len(fetched_ok_urls_items) == len(ok_urls)
        for item in fetched_ok_urls_items:
            assert item.get("LastFetched") >= update_time

        fetched_error_urls_items = list(filter(lambda item: item["Url"] in error_urls, items))
        assert len(fetched_error_urls_items) == len(error_urls)
        for item in fetched_error_urls_items:
            assert item.get("LastFetched") >= update_time
            assert item.get("ErrorMessage") == "FakeErrorMessage"
    finally:
        # Cleanup
        _delete_urls(urls)


def _delete_urls(urls):
    with URL_TABLE.batch_writer() as batch:
        for key in map(lambda url: {"Url": url}, urls):
            batch.delete_item(Key=key)
