from unittest.mock import patch

from tests.util import random_string
from yelp.apig_handler import handle, items_to_response


def test_items_to_response():
    # Given
    items = [
        {"UserId": "foo", "SortKey": "sk1", "Color": "Orange"},
        {"UserId": "foo", "SortKey": "sk2", "k1": "v1", "k2": "v2"},
        {"UserId": "foo", "SortKey": "sk3", "k": 100},
    ]

    # When
    result = items_to_response(items)

    # Then
    assert result == {
        "foo": {
            "sk1": {"Color": "Orange"},
            "sk2": {"k1": "v1", "k2": "v2"},
            "sk3": {"k": "100"},
        }
    }


@patch("yelp.apig_handler.items_to_response")
@patch("yelp.apig_handler.yelp_table")
def test_handle_get(_, mock_items_to_response):
    # Given
    response = {"foo": "bar", "biz": 42}
    mock_items_to_response.return_value = response

    # When
    result = handle({"httpMethod": "GET", "queryStringParameters": {}})

    # Then
    assert result == {"statusCode": 200, "body": '{"foo": "bar", "biz": 42}'}


@patch("yelp.apig_handler.upsert_user_id")
def test_handle_post(mock_upsert_user_id):
    # Given
    user_id = random_string()

    # When
    result = handle({"httpMethod": "POST", "queryStringParameters": {"userId": user_id}})

    # Then
    mock_upsert_user_id.assert_called_once_with(user_id)
    assert result == {"statusCode": 200}


@patch("yelp.apig_handler.yelp_table")
@patch("yelp.apig_handler.url_table")
@patch("yelp.apig_handler.config_table")
def test_handle_delete(mock_config_table, mock_url_table, mock_yelp_table):
    # Given
    user_id = random_string()

    # When
    result = handle({"httpMethod": "DELETE", "queryStringParameters": {"userId": user_id}})

    # Then
    mock_config_table.delete_user_id.assert_called_once_with(user_id)
    mock_url_table.delete_user_id.assert_called_once_with(user_id)
    mock_yelp_table.delete_user_id.assert_called_once_with(user_id)
    assert result == {"statusCode": 200}
