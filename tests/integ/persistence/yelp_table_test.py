import os
import time

import boto3
from tests.util import random_int, random_string
from yelp.persistence.yelp_table import (
    ReviewId,
    ReviewMetadata,
    UserMetadata,
    get_all_records,
    update_review_status,
    upsert_metadata,
    upsert_review,
)

YELP_TABLE_NAME = os.environ["YELP_TABLE_NAME"]
YELP_TABLE = boto3.resource("dynamodb").Table(YELP_TABLE_NAME)

TEST_TTL = 1000


def test_upsert_metadata():
    # Given
    user_id = random_string()
    user_metadata = UserMetadata(random_string(), random_string(), random_int())

    try:
        update_time = int(time.time())

        # When
        upsert_metadata(user_id, user_metadata, TEST_TTL)

        # Then
        records = get_all_records(user_id)
        assert len(records) == 1
        assert records[0].get("UserId") == user_id
        assert records[0].get("SortKey") == "Metadata"
        assert records[0].get("UserName") == user_metadata.name
        assert records[0].get("City") == user_metadata.city
        assert records[0].get("ReviewCount") == user_metadata.review_count
        assert records[0].get("LastUpdated") >= update_time
        assert records[0].get("TimeToLive") >= (update_time + TEST_TTL)

        # Cleanup
        _delete_records(records)
    except:
        _delete_user_id(user_id)
        raise


def test_upsert_review():
    # Given
    user_id = random_string()
    review_id = ReviewId(random_string(), random_string())
    review_metadata = ReviewMetadata(random_string(), random_string(), random_string())

    try:
        update_time = int(time.time())

        # When
        upsert_review(user_id, review_id, review_metadata, TEST_TTL)

        # Then
        records = get_all_records(user_id)
        assert len(records) == 1
        assert records[0].get("UserId") == user_id
        assert records[0].get("SortKey") == f"Review#{review_id.biz_id}"
        assert records[0].get("BizId") == review_id.biz_id
        assert records[0].get("ReviewId") == review_id.review_id
        assert records[0].get("BizName") == review_metadata.biz_name
        assert records[0].get("BizAddress") == review_metadata.biz_address
        assert records[0].get("ReviewDate") == review_metadata.review_date
        assert records[0].get("LastUpdated") >= update_time
        assert records[0].get("TimeToLive") >= (update_time + TEST_TTL)

        # Cleanup
        _delete_records(records)
    except:
        _delete_user_id(user_id)
        raise


def test_update_review_status():
    # Given
    user_id = random_string()
    review_id = ReviewId(random_string(), random_string())
    review_metadata = ReviewMetadata(random_string(), random_string(), random_string())

    status = random_string()

    try:
        # Make first review upsert
        upsert_review(user_id, review_id, review_metadata, TEST_TTL)
        records = get_all_records(user_id)
        assert len(records) == 1
        prev_user_id = records[0].get("UserId")
        prev_sort_key = records[0].get("SortKey")
        prev_biz_id = records[0].get("BizId")
        prev_review_id = records[0].get("ReviewId")
        prev_biz_name = records[0].get("BizName")
        prev_biz_address = records[0].get("BizAddress")
        prev_review_date = records[0].get("ReviewDate")
        prev_last_updated = records[0].get("LastUpdated")
        prev_ttl = records[0].get("TimeToLive")
        prev_last_updated = records[0].get("LastUpdated")
        prev_ttl = records[0].get("TimeToLive")

        # Update the status
        update_review_status(user_id, review_id, status)
        records = get_all_records(user_id)
        assert len(records) == 1

        # Verify old attributes were unchanged
        assert records[0].get("UserId") == prev_user_id
        assert records[0].get("SortKey") == prev_sort_key
        assert records[0].get("BizId") == prev_biz_id
        assert records[0].get("ReviewId") == prev_review_id
        assert records[0].get("BizName") == prev_biz_name
        assert records[0].get("BizAddress") == prev_biz_address
        assert records[0].get("ReviewDate") == prev_review_date
        assert records[0].get("TimeToLive") == prev_ttl

        # Verify status updated
        assert records[0].get("ReviewStatus") == status
        assert records[0].get("LastUpdated") >= prev_last_updated

        # Cleanup
        _delete_records(records)
    except:
        _delete_user_id(user_id)
        raise


def _delete_user_id(user_id):
    _delete_records(get_all_records(user_id))


def _delete_records(records):
    keys = [{"UserId": item["UserId"], "SortKey": item["SortKey"]} for item in records]
    with YELP_TABLE.batch_writer() as batch:
        for key in keys:
            batch.delete_item(Key=key)
