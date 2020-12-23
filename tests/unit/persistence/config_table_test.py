from datetime import datetime
from unittest.mock import patch

from freezegun import freeze_time
from yelp.persistence.config_table import get_all_user_ids, upsert_user_id


@freeze_time("2020-08-23")
@patch("yelp.persistence.config_table.CONFIG_TABLE")
def test_upser_user_id(mock_table):
    # Given
    user_id = "test-user-id"

    # When
    upsert_user_id(user_id)

    # Then
    mock_table.put_item.assert_called_once_with(
        Item={"UserId": user_id, "LastModified": datetime(2020, 8, 23).timestamp()}
    )


@patch("yelp.persistence.config_table.CONFIG_TABLE")
def test_get_all_user_ids(mock_table):
    # Given
    mock_table.scan.side_effect = (
        {"Items": [{"UserId": "a"}], "LastEvaluatedKey": True},
        {"Items": [{"UserId": "b"}, {"UserId": "c"}], "LastEvaluatedKey": True},
        {"Items": [{"UserId": "d"}, {"UserId": "e"}]},
    )

    # When
    result = get_all_user_ids()

    # Then
    assert result == ["a", "b", "c", "d", "e"]
