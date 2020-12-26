from decimal import Decimal
from unittest.mock import call, patch

from tests.util import random_string
from yelp.url_requester import (
    _create_review_status_url,
    _create_user_metadata_url,
    _create_user_review_pages_urls,
    _parse_ddb_record,
    handle,
)


@patch("yelp.url_requester.upsert_new_url")
def test_create_user_metadata_url(mock_upsert_new_url):
    # Given
    user_id = random_string()

    # When
    _create_user_metadata_url(user_id)

    # Then
    mock_upsert_new_url.assert_called_once_with(
        user_id, f"https://www.yelp.com/user_details?userid={user_id}"
    )


@patch("yelp.url_requester.upsert_new_url")
def test_create_user_review_pages_urls(mock_upsert_new_url):
    # Given
    user_id = random_string()
    review_count = Decimal(32)
    user_metadata_record = {"UserId": user_id, "ReviewCount": review_count}

    # When
    _create_user_review_pages_urls(user_metadata_record)

    # Then
    mock_upsert_new_url.assert_has_calls(
        [
            call(
                user_id,
                f"https://www.yelp.com/user_details_reviews_self?userid={user_id}&rec_pagestart=0",
            ),
            call(
                user_id,
                f"https://www.yelp.com/user_details_reviews_self?userid={user_id}&rec_pagestart=10",
            ),
            call(
                user_id,
                f"https://www.yelp.com/user_details_reviews_self?userid={user_id}&rec_pagestart=20",
            ),
        ]
    )


@patch("yelp.url_requester.get_user_id_from_review_id")
@patch("yelp.url_requester.upsert_new_url")
def test_create_review_status_url(mock_upsert_new_url, mock_get_user_id_from_review_id):
    # Given
    biz_id, review_id = random_string(), random_string()
    review_record = {"BizId": biz_id, "ReviewId": review_id}

    user_id = random_string()
    mock_get_user_id_from_review_id.return_value = user_id

    # When
    _create_review_status_url(review_record)

    # Then
    mock_upsert_new_url.assert_called_once_with(
        user_id, f"https://www.yelp.com/biz/{biz_id}?hrid={review_id}"
    )


def test_parse_ddb_record():
    # Given
    event_record = {
        "eventName": "INSERT",
        "dynamodb": {
            "Keys": {"UserId": {"S": "OPBdTgFkXkyTfAnzAIFr"}, "SortKey": {"S": "Metadata"}},
            "NewImage": {
                "TimeToLive": {"N": "1608949569"},
                "UserName": {"S": "qnYvIxzJbcuMCdhMSyBs"},
                "UserId": {"S": "OPBdTgFkXkyTfAnzAIFr"},
                "SortKey": {"S": "Metadata"},
                "City": {"S": "huMyUJsIAOUmRIQAdGds"},
                "LastUpdated": {"N": "1608948569"},
                "ReviewCount": {"N": "57"},
            },
        },
    }

    # When
    result = _parse_ddb_record(event_record)

    # Then
    assert result == {
        "City": "huMyUJsIAOUmRIQAdGds",
        "LastUpdated": Decimal("1608948569"),
        "ReviewCount": Decimal("57"),
        "SortKey": "Metadata",
        "TimeToLive": Decimal("1608949569"),
        "UserId": "OPBdTgFkXkyTfAnzAIFr",
        "UserName": "qnYvIxzJbcuMCdhMSyBs",
    }


@patch("yelp.url_requester._create_review_status_url")
@patch("yelp.url_requester._create_user_review_pages_urls")
@patch("yelp.url_requester._create_user_metadata_url")
@patch("yelp.url_requester.get_all_records")
@patch("yelp.url_requester.get_all_user_ids")
def test_handle_cron_event(
    mock_get_all_user_ids,
    mock_get_all_records,
    mock_create_user_metadata_url,
    mock_create_user_review_pages_urls,
    mock_create_review_status_url,
):
    # Given
    event = {"source": "aws.events"}

    user_id_1, user_id_2, user_id_3 = random_string(), random_string(), random_string()
    mock_get_all_user_ids.return_value = [user_id_1, user_id_2, user_id_3]

    record_1 = {"UserId": user_id_1, "SortKey": "Metadata"}
    record_2 = {"UserId": user_id_1, "SortKey": "Review"}
    record_3 = {"UserId": user_id_2, "SortKey": "Review"}
    record_4 = {"UserId": user_id_3, "SortKey": "Metadata"}
    mock_get_all_records.side_effect = [
        [record_1, record_2],
        [record_3],
        [record_4],
    ]

    # When
    handle(event)

    # Then
    mock_create_user_metadata_url.assert_has_calls(
        [call(user_id_1), call(user_id_2), call(user_id_3)]
    )
    mock_create_user_review_pages_urls.assert_has_calls([call(record_1), call(record_4)])
    mock_create_review_status_url.assert_has_calls([call(record_2), call(record_3)])


@patch("yelp.url_requester._create_review_status_url")
@patch("yelp.url_requester._create_user_review_pages_urls")
def test_handle_yelp_table_event(mock_create_user_review_pages_urls, mock_create_review_status_url):
    # Given
    event = {
        "Records": [
            {
                "eventName": "REMOVE",
                "dynamodb": {"NewImage": "This record should be ignored."},
            },
            {
                "eventName": "INSERT",
                "dynamodb": {
                    "Keys": {"UserId": {"S": "OPBdTgFkXkyTfAnzAIFr"}, "SortKey": {"S": "Metadata"}},
                    "NewImage": {
                        "TimeToLive": {"N": "1608949569"},
                        "UserName": {"S": "qnYvIxzJbcuMCdhMSyBs"},
                        "UserId": {"S": "OPBdTgFkXkyTfAnzAIFr"},
                        "SortKey": {"S": "Metadata"},
                        "City": {"S": "huMyUJsIAOUmRIQAdGds"},
                        "LastUpdated": {"N": "1608948569"},
                        "ReviewCount": {"N": "57"},
                    },
                },
                "eventSourceARN": "arn:aws:dynamodb:us-west-1:123456789012:table/YelpTable/stream/2015-06-27T00:48:05.899",
            },
            {
                "eventName": "INSERT",
                "dynamodb": {
                    "Keys": {
                        "UserId": {"S": "wpOJlCdBneHmozPVoNZI"},
                        "SortKey": {"S": "Review#WvyGaSiHJhfoYsgDVtOL"},
                    },
                    "NewImage": {
                        "BizAddress": {"S": "OFsGCKmAbEjXGbijXwbU"},
                        "ReviewDate": {"S": "eqqWkDdLeHxOfSoBmLhj"},
                        "TimeToLive": {"N": "1608949570"},
                        "UserId": {"S": "wpOJlCdBneHmozPVoNZI"},
                        "BizName": {"S": "ChSsPJenxLdLSBoBjgKf"},
                        "SortKey": {"S": "Review#WvyGaSiHJhfoYsgDVtOL"},
                        "LastUpdated": {"N": "1608948570"},
                        "ReviewId": {"S": "NhekAcdfldDzLzlzereZ"},
                        "BizId": {"S": "WvyGaSiHJhfoYsgDVtOL"},
                    },
                },
                "eventSourceARN": "arn:aws:dynamodb:us-west-1:123456789012:table/YelpTable/stream/2015-06-27T00:48:05.899",
            },
        ]
    }

    # When
    handle(event)

    # Then
    mock_create_user_review_pages_urls.assert_called_once_with(
        {
            "TimeToLive": Decimal("1608949569"),
            "UserName": "qnYvIxzJbcuMCdhMSyBs",
            "UserId": "OPBdTgFkXkyTfAnzAIFr",
            "SortKey": "Metadata",
            "City": "huMyUJsIAOUmRIQAdGds",
            "LastUpdated": Decimal("1608948569"),
            "ReviewCount": Decimal("57"),
        }
    )
    mock_create_review_status_url.assert_called_once_with(
        {
            "BizAddress": "OFsGCKmAbEjXGbijXwbU",
            "ReviewDate": "eqqWkDdLeHxOfSoBmLhj",
            "TimeToLive": Decimal("1608949570"),
            "UserId": "wpOJlCdBneHmozPVoNZI",
            "BizName": "ChSsPJenxLdLSBoBjgKf",
            "SortKey": "Review#WvyGaSiHJhfoYsgDVtOL",
            "LastUpdated": Decimal("1608948570"),
            "ReviewId": "NhekAcdfldDzLzlzereZ",
            "BizId": "WvyGaSiHJhfoYsgDVtOL",
        }
    )


@patch("yelp.url_requester.upsert_new_url")
def test_handle_config_table_event(mock_upsert_new_url):
    # Given
    user_id = random_string()
    event = {
        "Records": [
            {
                "eventName": "INSERT",
                "dynamodb": {
                    "NewImage": {"UserId": {"S": user_id}},
                },
                "eventSourceARN": "arn:aws:dynamodb:us-west-1:316936913708:table/ConfigTable/stream/2020-12-26T06:39:42.594",
            }
        ]
    }

    # When
    handle(event)

    # Then
    mock_upsert_new_url.assert_called_once_with(
        user_id, f"https://www.yelp.com/user_details?userid={user_id}"
    )
