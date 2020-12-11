from datetime import datetime
from unittest.mock import Mock, patch

from boto3.dynamodb.conditions import Key
from freezegun import freeze_time

from persistence import yelp_table
from persistence.yelp_table import (
    ReviewId,
    ReviewMetadata,
    UserMetadata,
    _calculate_ttl,
    _upsert_record,
    get_all_records,
    update_review_status,
    upsert_metadata,
    upsert_review,
)


@freeze_time("2020-08-23")
def test_calculate_ttl():
    ttl = 24
    yelp_table.YELP_TABLE_TTL = ttl
    assert _calculate_ttl() == int(datetime(2020, 8, 23).timestamp()) + ttl


@freeze_time("2020-08-23")
def test_upsert_record():
    # Given
    mock_yelp_table = Mock()
    yelp_table.YELP_TABLE = mock_yelp_table

    user_id = Mock()
    sort_key = Mock()
    update_expression = "Id=:id"
    expression_attribute_values = {":id": "test-id"}

    # When
    _upsert_record(user_id, sort_key, update_expression, expression_attribute_values)

    # Then
    mock_yelp_table.update_item.assert_called_once_with(
        Key={"UserId": user_id, "SortKey": sort_key},
        UpdateExpression="Id=:id, LastUpdated=:last_updated",
        ExpressionAttributeValues={
            ":id": "test-id",
            ":last_updated": int(datetime(2020, 8, 23).timestamp()),
        },
    )


def test_get_all_records():
    # Given
    user_id = "test-user-id"
    items = ["foo", "bar"]

    mock_yelp_table = Mock()
    mock_yelp_table.query.return_value = {"Items": items}
    yelp_table.YELP_TABLE = mock_yelp_table

    # When
    result = get_all_records(user_id)

    # When
    assert result == items
    mock_yelp_table.query.assert_called_once_with(KeyConditionExpression=Key("UserId").eq(user_id))


@patch("persistence.yelp_table._calculate_ttl")
@patch("persistence.yelp_table._upsert_record")
def test_upsert_metadata(mock_upsert_record, mock_calculate_ttl):
    # Given
    user_id = "test-user-id"
    user_metadata = UserMetadata("test-name", "test-city", "test-review-count")

    ttl = "test-ttl"
    mock_calculate_ttl.return_value = ttl
    yelp_table.YELP_TABLE_TTL = ttl

    # When
    upsert_metadata(user_id, user_metadata)

    # Then
    mock_upsert_record.assert_called_once_with(
        user_id,
        "Metadata",
        "set UserName=:name, City=:city, ReviewCount=:review_count, TimeToLive=:ttl",
        {
            ":name": user_metadata.name,
            ":city": user_metadata.city,
            ":review_count": user_metadata.review_count,
            ":ttl": "test-ttl",
        },
    )


@patch("persistence.yelp_table._calculate_ttl")
@patch("persistence.yelp_table._upsert_record")
def test_upsert_review(mock_upsert_record, mock_calculate_ttl):
    # Given
    user_id = "test-user-id"
    review_id = ReviewId("test-biz-id", "test-review-id")
    review_metadata = ReviewMetadata("test-biz-name", "test-biz-address", "test-review-data")

    ttl = "test-ttl"
    mock_calculate_ttl.return_value = ttl

    # When
    upsert_review(user_id, review_id, review_metadata)

    # Then
    mock_upsert_record.assert_called_once_with(
        user_id,
        "Review#test-biz-id",
        "set BizId=:biz_id, ReviewId=:review_id, BizName=:biz_name, BizAddress=:biz_address, ReviewDate=:review_date, TimeToLive=:ttl",
        {
            ":biz_id": review_id.biz_id,
            ":review_id": review_id.review_id,
            ":biz_name": review_metadata.biz_name,
            ":biz_address": review_metadata.biz_address,
            ":review_date": review_metadata.review_date,
            ":ttl": ttl,
        },
    )


@patch("persistence.yelp_table._calculate_ttl")
@patch("persistence.yelp_table._upsert_record")
def test_update_review_status(mock_upsert_record, mock_calculate_ttl):
    # Given
    user_id = "test-user-id"
    review_id = ReviewId("test-biz-id", "test-review-id")

    status = "test-status"

    # When
    update_review_status(user_id, review_id, status)

    # Then
    mock_upsert_record.assert_called_once_with(
        user_id,
        "Review#test-biz-id",
        "set ReviewStatus=:status",
        {":status": status},
    )
