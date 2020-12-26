import os
import time

import boto3
import requests
from boto3.dynamodb.conditions import Key
from tests.integ.persistence.yelp_table_test import _delete_user_id
from tests.util import random_string, wait_until
from yelp.persistence.page_bucket import KeyUtils, upload_page
from yelp.persistence.yelp_table import ReviewId, ReviewMetadata, get_all_records, upsert_review

PAGE_BUCKET_NAME = os.environ["PAGE_BUCKET_NAME"]
YELP_TABLE_NAME = os.environ["YELP_TABLE_NAME"]

S3 = boto3.resource("s3")
YELP_TABLE = boto3.resource("dynamodb").Table(YELP_TABLE_NAME)


def test_user_metadata_page():
    # Given
    user_id = "9ABW4HCBanLHseVM4XPpaw"
    url = f"https://www.yelp.com/user_details?userid={user_id}"
    html = requests.get(url).text

    try:
        # When
        upload_time = int(time.time())
        upload_page(url, html)

        # Then
        record = wait_until(
            _query,
            (user_id, "Metadata"),
            lambda record: record and record.get("LastUpdated") >= upload_time,
        )
        assert record.get("UserId") == user_id
        assert record.get("SortKey") == "Metadata"
        assert record.get("UserName")
        assert record.get("City")
        assert record.get("ReviewCount")
        assert record.get("LastUpdated") >= upload_time
        assert record.get("TimeToLive") >= upload_time
    finally:
        # Cleanup
        _delete_user_id(user_id)
        _delete_page(url)


def test_reviews_page():
    # Given
    user_id = "9ABW4HCBanLHseVM4XPpaw"
    url = f"https://www.yelp.com/user_details_reviews_self?userid={user_id}&rec_pagestart=0"
    html = requests.get(url).text

    try:
        # When
        upload_time = int(time.time())
        upload_page(url, html)

        # Then
        records = wait_until(
            lambda user_id: list(
                filter(
                    lambda record: record.get("SortKey").startswith("Review#")
                    and record.get("LastUpdated") >= upload_time,
                    get_all_records(user_id),
                )
            ),
            (user_id,),
            lambda records: len(records) == 10,
        )
        for record in records:
            assert record.get("UserId") == user_id
            _, biz_id = record.get("SortKey").split("#")
            assert record.get("BizId") == biz_id
            assert record.get("ReviewId")
            assert record.get("BizName")
            assert record.get("BizAddress")
            assert record.get("ReviewDate")
            assert record.get("LastUpdated") >= upload_time
            assert record.get("TimeToLive") >= upload_time
    finally:
        # Cleanup
        _delete_user_id(user_id)
        _delete_page(url)


def test_review_status_page_alive():
    # Given
    biz_id, review_id = "boiling-point-irvine", "_Y7JKSAnR15bj9mO-n2pUg"
    url = f"https://www.yelp.com/biz/{biz_id}?hrid={review_id}"
    html = requests.get(url).text

    user_id = random_string()
    upsert_review(
        user_id,
        ReviewId(biz_id=biz_id, review_id=review_id),
        ReviewMetadata(random_string(), random_string(), random_string()),
    )

    try:
        # When
        upload_time = int(time.time())
        upload_page(url, html)

        # Then
        record = wait_until(
            _query,
            (user_id, f"Review#{biz_id}"),
            lambda record: record
            and record.get("LastUpdated") >= upload_time
            and record.get("ReviewStatus") is not None,
        )
        assert record.get("ReviewStatus") == True
        assert record.get("LastUpdated") >= upload_time
    finally:
        # Cleanup
        _delete_user_id(user_id)
        _delete_page(url)


def test_review_status_page_dead():
    # Given
    biz_id, review_id = "aloha-family-billiards-buena-park", "MryCU0KVNRvJRYGWtaI9AA"
    url = f"https://www.yelp.com/biz/{biz_id}?hrid={review_id}"
    html = requests.get(url).text

    user_id = random_string()
    upsert_review(
        user_id,
        ReviewId(biz_id=biz_id, review_id=review_id),
        ReviewMetadata(random_string(), random_string(), random_string()),
    )

    try:
        # When
        upload_time = int(time.time())
        upload_page(url, html)

        # Then
        record = wait_until(
            _query,
            (user_id, f"Review#{biz_id}"),
            lambda record: record
            and record.get("LastUpdated") >= upload_time
            and record.get("ReviewStatus") is not None,
        )
        assert record.get("ReviewStatus") == False
    finally:
        # Cleanup
        _delete_user_id(user_id)
        _delete_page(url)


def _query(user_id, sort_key):
    records = YELP_TABLE.query(
        KeyConditionExpression=Key("UserId").eq(user_id) & Key("SortKey").eq(sort_key)
    )
    if len(records.get("Items", [])) == 1:
        return records["Items"][0]


def _delete_page(url):
    key = KeyUtils.to_key(url)
    S3.Object(PAGE_BUCKET_NAME, key).delete()
