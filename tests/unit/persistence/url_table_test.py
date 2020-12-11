from datetime import datetime
from unittest.mock import patch

from freezegun import freeze_time

from persistence import url_table
from persistence.url_table import get_all_url_items, update_fetched_url, upsert_new_url


@patch("persistence.url_table.URL_TABLE")
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


@freeze_time("2020-08-23")
@patch("persistence.url_table.URL_TABLE")
def test_upsert_new_url(mock_table):
    # Given
    url = "https://foo.com"
    ttl = 24

    url_table.URL_TABLE_TTL = ttl

    # When
    upsert_new_url(url)

    # Then
    mock_table.update_item.assert_called_once_with(
        Key={"Url": url},
        UpdateExpression="set TimeToLive=:ttl",
        ExpressionAttributeValues={":ttl": int(datetime(2020, 8, 23).timestamp()) + ttl},
    )


@freeze_time("2020-08-23")
@patch("persistence.url_table.URL_TABLE")
def test_update_fetched_url(mock_table):
    # Given
    url = "https://foo.com"

    # When
    update_fetched_url(url)

    # Then
    mock_table.update_item.assert_called_once_with(
        Key={"Url": url},
        UpdateExpression="set LastFetched=:last_fetched",
        ExpressionAttributeValues={":last_fetched": int(datetime(2020, 8, 23).timestamp())},
    )


@freeze_time("2020-08-23")
@patch("persistence.url_table.URL_TABLE")
def test_update_fetched_url_erro(mock_table):
    # Given
    url = "https://foo.com"
    error = "test-error-message"

    # When
    update_fetched_url(url, error)

    # Then
    mock_table.update_item.assert_called_once_with(
        Key={"Url": url},
        UpdateExpression="set LastFetched=:last_fetched, ErrorMessage=:error",
        ExpressionAttributeValues={
            ":last_fetched": int(datetime(2020, 8, 23).timestamp()),
            ":error": error,
        },
    )
