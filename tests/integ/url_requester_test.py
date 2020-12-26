import json
import os

import boto3
from boto3.dynamodb.conditions import Key
from tests.integ.persistence.yelp_table_test import _delete_user_id
from tests.util import random_string, wait_until
from yelp.persistence.config_table import upsert_user_id
from yelp.persistence.yelp_table import (
    ReviewId,
    ReviewMetadata,
    UserMetadata,
    upsert_metadata,
    upsert_review,
)

URL_REQUESTER_LAMBDA_NAME = os.environ["URL_REQUESTER_LAMBDA_NAME"]
CONFIG_TABLE_NAME = os.environ["CONFIG_TABLE_NAME"]
URL_TABLE_NAME = os.environ["URL_TABLE_NAME"]

AWS_LAMBDA = boto3.client("lambda")
CONFIG_TABLE = boto3.resource("dynamodb").Table(CONFIG_TABLE_NAME)
URL_TABLE = boto3.resource("dynamodb").Table(URL_TABLE_NAME)


def test_cron_event_expect_user_metadata_url():
    try:
        # Given
        user_id = random_string()
        upsert_user_id(user_id)

        # When
        _invoke_cron(user_id)

        # Then
        expected_url = f"https://www.yelp.com/user_details?userid={user_id}"
        wait_until(_query_url, (expected_url,), bool)
    finally:
        _cleanup(user_id=user_id, urls=[expected_url])


def test_cron_event_expect_review_page_urls():
    try:
        # Given
        user_id = random_string()
        review_count = 24
        upsert_metadata(
            user_id=user_id,
            user_metadata=UserMetadata(
                name=random_string(), city=random_string(), review_count=review_count
            ),
        )

        # When
        _invoke_cron(user_id)

        # Then
        expected_urls = [
            f"https://www.yelp.com/user_details_reviews_self?userid={user_id}&rec_pagestart=0",
            f"https://www.yelp.com/user_details_reviews_self?userid={user_id}&rec_pagestart=10",
            f"https://www.yelp.com/user_details_reviews_self?userid={user_id}&rec_pagestart=20",
        ]
        wait_until(lambda urls: all(_query_url(url) for url in urls), (expected_urls,), bool)
    finally:
        _cleanup(user_id=user_id, urls=expected_urls)


def test_cron_event_expect_review_status_url():
    try:
        # Given
        user_id = random_string()
        review_id, biz_id = random_string(), random_string()
        upsert_review(
            user_id=user_id,
            review_id=ReviewId(biz_id=biz_id, review_id=review_id),
            review_metadata=ReviewMetadata(
                biz_name=random_string(), biz_address=random_string(), review_date=random_string()
            ),
        )

        # When
        _invoke_cron(user_id)

        # Then
        expected_url = f"https://www.yelp.com/biz/{biz_id}?hrid={review_id}"
        wait_until(_query_url, (expected_url,), bool)
    finally:
        _cleanup(user_id=user_id, urls=[expected_url])


def test_yelp_table_event_user_metadata_record():
    try:
        # Given
        user_id = random_string()
        review_count = 24
        upsert_metadata(
            user_id=user_id,
            user_metadata=UserMetadata(
                name=random_string(), city=random_string(), review_count=review_count
            ),
        )

        # Lambda should be invoked by DDB stream from YelpTable

        # Then
        expected_urls = [
            f"https://www.yelp.com/user_details_reviews_self?userid={user_id}&rec_pagestart=0",
            f"https://www.yelp.com/user_details_reviews_self?userid={user_id}&rec_pagestart=10",
            f"https://www.yelp.com/user_details_reviews_self?userid={user_id}&rec_pagestart=20",
        ]
        wait_until(lambda urls: all(_query_url(url) for url in urls), (expected_urls,), bool)
    finally:
        _cleanup(user_id=user_id, urls=expected_urls)


def test_yelp_table_event_review():
    try:
        # Given
        user_id = random_string()
        review_id, biz_id = random_string(), random_string()
        upsert_review(
            user_id=user_id,
            review_id=ReviewId(biz_id=biz_id, review_id=review_id),
            review_metadata=ReviewMetadata(
                biz_name=random_string(), biz_address=random_string(), review_date=random_string()
            ),
        )

        # Lambda should be invoked by DDB stream from YelpTable

        # Then
        expected_url = f"https://www.yelp.com/biz/{biz_id}?hrid={review_id}"
        wait_until(_query_url, (expected_url,), bool)
    finally:
        _cleanup(user_id=user_id, urls=[expected_url])


def _invoke_cron(user_id=None):
    AWS_LAMBDA.invoke(
        FunctionName=URL_REQUESTER_LAMBDA_NAME,
        InvocationType="Event",
        Payload=json.dumps({"source": "aws.events", "user_id (DEBUG)": user_id}),
    )


def _query_url(url):
    records = URL_TABLE.query(KeyConditionExpression=Key("Url").eq(url))
    if len(records.get("Items", [])) == 1:
        return records["Items"][0]


def _cleanup(user_id=None, urls=[]):
    CONFIG_TABLE.delete_item(Key={"UserId": user_id})
    _delete_user_id(user_id)  # YelpTable cleanup
    with URL_TABLE.batch_writer() as batch:
        for key in ({"Url": url} for url in urls):
            batch.delete_item(Key=key)
