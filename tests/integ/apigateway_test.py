import json
import os

import boto3
import requests
from tests.util import random_string

URL = os.environ["APIGATEWAY_URL"]
YELP_USER_ID = os.environ["YELP_USER_ID"]
YELP_TABLE_NAME = os.environ["YELP_TABLE_NAME"]


def test_found():
    # Given
    table = boto3.resource("dynamodb").Table(YELP_TABLE_NAME)
    user_id = random_string()
    items = [
        {"UserId": user_id, "SortKey": "Metadata", "Name": "Sam", "ReviewCount": 42},
        {
            "UserId": user_id,
            "SortKey": "Review#0",
            "BizId": "0",
            "ReviewId": "0",
            "Status": "ALIVE",
        },
        {"UserId": user_id, "SortKey": "Review#1", "BizId": "1", "ReviewId": "1", "Status": "DEAD"},
    ]
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)
    try:
        # When
        resp = requests.get(URL, params={"userId": user_id})

        # Then
        assert resp.status_code == 200
        assert json.loads(resp.content) == {
            user_id: {
                "Metadata": {"Name": "Sam", "ReviewCount": "42"},
                "Review#0": {"BizId": "0", "ReviewId": "0", "Status": "ALIVE"},
                "Review#1": {"BizId": "1", "ReviewId": "1", "Status": "DEAD"},
            }
        }
    finally:
        with table.batch_writer() as batch:
            for key in map(
                lambda item: {"UserId": item["UserId"], "SortKey": item["SortKey"]}, items
            ):
                batch.delete_item(Key=key)


def test_no_result():
    # Given
    user_id = random_string()

    # When
    resp = requests.get(URL, params={"userId": user_id})

    # Then
    assert resp.status_code == 200
    assert json.loads(resp.content) == {}


def test_not_implemented():
    # When
    resp = requests.post(URL)

    # Then
    assert resp.status_code == 501
