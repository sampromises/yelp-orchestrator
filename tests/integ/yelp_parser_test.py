"""
TODO:
- These tests interfere with real data because YELP_USER_ID is hard-coded.
"""

import os
import time

import boto3
import requests
from boto3.dynamodb.conditions import Key
from tests.util import wait_until
from yelp.persistence.page_bucket import KeyUtils, upload_page
from yelp.persistence.yelp_table import get_all_records

PAGE_BUCKET_NAME = os.environ["PAGE_BUCKET_NAME"]
YELP_TABLE_NAME = os.environ["YELP_TABLE_NAME"]
YELP_USER_ID = os.environ["YELP_USER_ID"]

S3 = boto3.resource("s3")
YELP_TABLE = boto3.resource("dynamodb").Table(YELP_TABLE_NAME)


def test_user_metadata_page():
    # Given
    url = f"https://www.yelp.com/user_details?userid={YELP_USER_ID}"
    html = requests.get(url).text

    # When
    upload_time = int(time.time())
    upload_page(url, html)

    # Then
    record = wait_until(
        _query,
        (YELP_USER_ID, "Metadata"),
        lambda record: record and record.get("LastUpdated") >= upload_time,
    )
    assert record.get("UserId") == YELP_USER_ID
    assert record.get("SortKey") == "Metadata"
    assert record.get("UserName")
    assert record.get("City")
    assert record.get("ReviewCount")
    assert record.get("LastUpdated") >= upload_time
    assert record.get("TimeToLive") >= upload_time


def test_reviews_page():
    # Given
    url = f"https://www.yelp.com/user_details_reviews_self?userid={YELP_USER_ID}&rec_pagestart=0"
    html = requests.get(url).text

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
        (YELP_USER_ID,),
        lambda records: len(records) == 10,
    )
    for record in records:
        assert record.get("UserId") == YELP_USER_ID
        _, biz_id = record.get("SortKey").split("#")
        assert record.get("BizId") == biz_id
        assert record.get("ReviewId")
        assert record.get("BizName")
        assert record.get("BizAddress")
        assert record.get("ReviewDate")
        assert record.get("LastUpdated") >= upload_time
        assert record.get("TimeToLive") >= upload_time


def test_review_status_page_alive():
    # Given
    biz_id, review_id = "cyprych-landscaping-pittsburgh", "nfB25BH5lHBXItzXAXp27w"
    url = f"https://www.yelp.com/biz/{biz_id}?hrid={review_id}"
    html = requests.get(url).text

    # When
    upload_time = int(time.time())
    upload_page(url, html)

    # Then
    record = wait_until(
        _query,
        (YELP_USER_ID, f"Review#{biz_id}"),
        lambda record: record and record.get("LastUpdated") >= upload_time,
    )
    assert record.get("ReviewStatus") == True
    assert record.get("LastUpdated") >= upload_time


def test_review_status_page_dead():
    # Given
    biz_id, review_id = "aloha-family-billiards-buena-park", "U_yiTX51HWo78jOaV0pIxQ"
    url = f"https://www.yelp.com/biz/{biz_id}?hrid={review_id}"
    html = requests.get(url).text

    # When
    upload_time = int(time.time())
    upload_page(url, html)

    # Then
    record = wait_until(
        _query,
        (YELP_USER_ID, f"Review#{biz_id}"),
        lambda record: record and record.get("LastUpdated") >= upload_time,
    )
    assert record.get("ReviewStatus") == False


def _query(user_id, sort_key):
    records = YELP_TABLE.query(
        KeyConditionExpression=Key("UserId").eq(user_id) & Key("SortKey").eq(sort_key)
    )
    assert len(records.get("Items", [])) == 1, "Expected exactly 1 record to be found from query."
    return records["Items"][0]


def _delete_page(url):
    key = KeyUtils.to_key(url)
    S3.Object(PAGE_BUCKET_NAME, key).delete()
