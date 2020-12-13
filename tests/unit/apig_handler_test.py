from unittest.mock import Mock, patch

from boto3.dynamodb.conditions import Key
from tests.util import random_string
from yelp.apig_handler import get_items, handle, items_to_response


@patch("yelp.apig_handler.boto3.resource")
def test_get_items(mock_resource):
    # Given
    user_id = random_string()
    mock_table = Mock()
    mock_table.query.return_value = {"Items": "TestResult"}
    mock_resource.return_value.Table.return_value = mock_table

    # When
    result = get_items(user_id)

    # Then
    assert result == "TestResult"
    mock_table.query.assert_called_once_with(KeyConditionExpression=Key("UserId").eq(user_id))


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
@patch("yelp.apig_handler.get_items")
def test_handle(mock_get_items, mock_items_to_response):
    # Given
    response = {"foo": "bar", "biz": 42}
    mock_items_to_response.return_value = response

    # When
    result = handle({"httpMethod": "GET", "queryStringParameters": {}})

    # Then
    assert result == {"statusCode": 200, "body": '{"foo": "bar", "biz": 42}'}
