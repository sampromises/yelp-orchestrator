import os

import boto3
from tests.util import random_string
from yelp.persistence.config_table import get_all_user_ids, upsert_user_id

CONFIG_TABLE_NAME = os.environ["CONFIG_TABLE_NAME"]
CONFIG_TABLE = boto3.resource("dynamodb").Table(CONFIG_TABLE_NAME)


def test_upserting_new_user_ids():
    # Given
    user_ids = [random_string() for _ in range(10)]

    try:
        # When
        for user_id in user_ids:
            upsert_user_id(user_id)

        # Then
        fetched_user_ids = get_all_user_ids()
        assert set(user_ids) == set(fetched_user_ids)
    finally:
        # Cleanup
        _delete_user_ids(*user_ids)


def _delete_user_ids(*user_ids):
    with CONFIG_TABLE.batch_writer() as batch:
        for key in map(lambda user_id: {"UserId": user_id}, user_ids):
            batch.delete_item(Key=key)
