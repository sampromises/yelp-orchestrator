from datetime import datetime
from unittest.mock import patch

import pytest
from freezegun import freeze_time
from tests.util import random_string
from yelp.persistence.url_table import get_all_url_items, update_fetched_url, upsert_new_url


@patch("yelp.persistence.url_table.URL_TABLE")
def test_get_all_items(mock_table):
    # Given
    mock_table.scan.side_effect = (
        {"Items": ["a"], "LastEvaluatedKey": True},
        {"Items": ["b", "c"], "LastEvaluatedKey": True},
        {"Items": ["d", "e"]},
    )

    # When
    result = get_all_url_items()

    # Then
    assert result == ["a", "b", "c", "d", "e"]


@pytest.mark.parametrize(
    "url,expected_sort_key",
    [
        ("https://www.yelp.com/user_details?userid=random-user-id", "SortKey#Metadata"),
        (
            "https://www.yelp.com/user_details?userid=random-user-id&ignored=ignored",
            "SortKey#Metadata",
        ),
        (
            "https://www.yelp.com/user_details_reviews_self?userid=random-user-id&rec_pagestart=42",
            "SortKey#UserReviewPage#42",
        ),
        (
            "https://www.yelp.com/user_details_reviews_self?userid=random-user-id&rec_pagestart=42&ignored=ignored",
            "SortKey#UserReviewPage#42",
        ),
        ("https://www.yelp.com/biz/random-biz-name", "SortKey#ReviewStatusPage#random-biz-name"),
        (
            "https://www.yelp.com/biz/random-biz-name?ignored=ignored",
            "SortKey#ReviewStatusPage#random-biz-name",
        ),
    ],
)
@freeze_time("2020-08-23")
@patch("yelp.persistence.url_table.URL_TABLE")
def test_upsert_new_url(mock_table, url, expected_sort_key):
    # Given
    user_id = random_string()
    ttl = 24

    # When
    upsert_new_url(user_id, url, ttl)

    # Then
    mock_table.update_item.assert_called_once_with(
        Key={"UserId": user_id, "SortKey": expected_sort_key},
        UpdateExpression="set PageUrl=:url, TimeToLive=:ttl",
        ExpressionAttributeValues={
            ":url": url,
            ":ttl": int(datetime(2020, 8, 23).timestamp()) + ttl,
        },
    )


@pytest.mark.parametrize(
    "url,expected_sort_key",
    [
        ("https://www.yelp.com/user_details?userid=random-user-id", "SortKey#Metadata"),
        (
            "https://www.yelp.com/user_details?userid=random-user-id&ignored=ignored",
            "SortKey#Metadata",
        ),
        (
            "https://www.yelp.com/user_details_reviews_self?userid=random-user-id&rec_pagestart=42",
            "SortKey#UserReviewPage#42",
        ),
        (
            "https://www.yelp.com/user_details_reviews_self?userid=random-user-id&rec_pagestart=42&ignored=ignored",
            "SortKey#UserReviewPage#42",
        ),
        ("https://www.yelp.com/biz/random-biz-name", "SortKey#ReviewStatusPage#random-biz-name"),
        (
            "https://www.yelp.com/biz/random-biz-name?ignored=ignored",
            "SortKey#ReviewStatusPage#random-biz-name",
        ),
    ],
)
@freeze_time("2020-08-23")
@patch("yelp.persistence.url_table.get_user_id_from_url")
@patch("yelp.persistence.url_table.URL_TABLE")
def test_update_fetched_url(mock_table, mock_get_user_id_from_url, url, expected_sort_key):
    # Given
    status_code = 42

    user_id = random_string()
    mock_get_user_id_from_url.return_value = user_id

    # When
    update_fetched_url(url, status_code)

    # Then
    mock_get_user_id_from_url.assert_called_once_with(url)
    mock_table.update_item.assert_called_once_with(
        Key={"UserId": user_id, "SortKey": expected_sort_key},
        UpdateExpression="set StatusCode=:status_code, LastFetched=:last_fetched",
        ExpressionAttributeValues={
            ":status_code": status_code,
            ":last_fetched": int(datetime(2020, 8, 23).timestamp()),
        },
    )
