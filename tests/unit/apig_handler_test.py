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
