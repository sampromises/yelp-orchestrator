from decimal import Decimal
from unittest.mock import Mock, patch

from tests.util import random_string
from yelp.apig_handler import get_all_user_records, handle


@patch("yelp.apig_handler.yelp_table.get_all_records")
def test_get_all_user_records(mock_get_all_records):
    # Given
    mock_get_all_records.return_value = [
        {"UserId": "foo", "SortKey": "Metadata", "Color": "Orange"},
        {"UserId": "foo", "SortKey": "Review#", "k1": "v1", "k2": "v2"},
        {"UserId": "foo", "SortKey": "Review#", "k": 100},
        {"UserId": "bar", "SortKey": "Review#", "k": Decimal(42)},
    ]

    # When
    result = get_all_user_records(Mock())

    # Then
    assert result == {
        "foo": {
            "Metadata": {"Color": "Orange"},
            "Reviews": [{"k1": "v1", "k2": "v2"}, {"k": "100"}],
        },
        "bar": {
            "Reviews": [{"k": "42"}],
        },
    }


@patch("yelp.apig_handler.get_all_user_records")
@patch("yelp.apig_handler.config_table.get_all_user_ids")
def test_handle_users_get(mock_get_all_user_ids, mock_get_all_user_records):
    # Given
    mock_get_all_user_ids.return_value = ["foo", "bar"]
    user_records_1 = {"foo": 42}
    user_records_2 = {"bar": 24}
    mock_get_all_user_records.side_effect = [user_records_1, user_records_2]

    # When
    result = handle({"resource": r"/users", "httpMethod": "GET"})

    # Then
    assert result == {"statusCode": 200, "body": '{"foo": 42, "bar": 24}'}


@patch("yelp.apig_handler.get_all_user_records")
@patch("yelp.apig_handler.yelp_table")
def test_handle_user_get(_, mock_get_all_user_records):
    # Given
    user_id = random_string()
    response = {"foo": "bar", "biz": 42}
    mock_get_all_user_records.return_value = response

    # When
    result = handle(
        {"resource": r"/{userId}", "httpMethod": "GET", "pathParameters": {"userId": user_id}}
    )

    # Then
    assert result == {"statusCode": 200, "body": '{"foo": "bar", "biz": 42}'}


@patch("yelp.apig_handler.upsert_user_id")
def test_handle_user_post(mock_upsert_user_id):
    # Given
    user_id = random_string()

    # When
    result = handle(
        {
            "resource": r"/{userId}",
            "httpMethod": "POST",
            "pathParameters": {"userId": user_id},
        }
    )

    # Then
    mock_upsert_user_id.assert_called_once_with(user_id)
    assert result == {"statusCode": 200}


@patch("yelp.apig_handler.yelp_table")
@patch("yelp.apig_handler.url_table")
@patch("yelp.apig_handler.config_table")
def test_handle_user_delete(mock_config_table, mock_url_table, mock_yelp_table):
    # Given
    user_id = random_string()

    # When
    result = handle(
        {
            "resource": r"/{userId}",
            "httpMethod": "DELETE",
            "pathParameters": {"userId": user_id},
        }
    )

    # Then
    mock_config_table.delete_user_id.assert_called_once_with(user_id)
    mock_url_table.delete_user_id.assert_called_once_with(user_id)
    mock_yelp_table.delete_user_id.assert_called_once_with(user_id)
    assert result == {"statusCode": 200}
