import traceback
from typing import Dict

import boto3

from yelp.persistence.config_table import get_all_user_ids
from yelp.persistence.url_table import upsert_new_url
from yelp.persistence.yelp_table import (
    _MetadataSchema,
    _ReviewSchema,
    _YelpTableSchema,
    get_all_records,
)

USER_METADATA_URL = "https://www.yelp.com/user_details?userid={}"
USER_REVIEW_PAGES_URL = "https://www.yelp.com/user_details_reviews_self?userid={}&rec_pagestart={}"
REVIEW_STATUS_URL = "https://www.yelp.com/biz/{}?hrid={}"

DDB_TYPE_DESERIALIZER = boto3.dynamodb.types.TypeDeserializer()


def _create_user_metadata_url(user_id: str):
    upsert_new_url(USER_METADATA_URL.format(user_id))


def _create_user_review_pages_urls(user_metadata_record: Dict):
    if user_metadata_record:
        user_id = user_metadata_record.get(_YelpTableSchema.USER_ID)
        review_count = int(user_metadata_record.get(_MetadataSchema.REVIEW_COUNT))
        for i in range(0, review_count, 10):
            upsert_new_url(USER_REVIEW_PAGES_URL.format(user_id, i))


def _create_review_status_url(review_record: Dict):
    if review_record:
        biz_id = review_record.get(_ReviewSchema.BIZ_ID)
        review_id = review_record.get(_ReviewSchema.REVIEW_ID)
        upsert_new_url(REVIEW_STATUS_URL.format(biz_id, review_id))


def handle_cron_event():
    for user_id in get_all_user_ids():
        # () => Metadata URL
        _create_user_metadata_url(user_id)

        for record in get_all_records(user_id):
            print(f"UserId: {user_id}, record: {record}")
            if record.get(_YelpTableSchema.SORT_KEY) == _MetadataSchema.SORT_KEY_VALUE:
                # (MetadataRecord) => Review Page URLs
                _create_user_review_pages_urls(record)
            elif record.get(_YelpTableSchema.SORT_KEY).startswith(_ReviewSchema.SORT_KEY_VALUE):
                # (ReviewRecord) => Biz Review Status URL
                _create_review_status_url(record)


def _parse_ddb_stream(event):
    upsert_records = filter(
        lambda record: record["eventName"] in ("INSERT", "MODIFY"), event["Records"]
    )
    ddb_records = map(lambda record: record["dynamodb"]["NewImage"], upsert_records)
    return list(
        map(
            lambda data: {k: DDB_TYPE_DESERIALIZER.deserialize(v) for k, v in data.items()},
            ddb_records,
        )
    )


def handle_yelp_table_event(event):
    errors = []
    for record in _parse_ddb_stream(event):
        print(f"Processing record: {record}")
        try:
            if record["SortKey"] == _MetadataSchema.SORT_KEY_VALUE:
                _create_user_review_pages_urls(record)
            elif record["SortKey"].startswith(_ReviewSchema.SORT_KEY_VALUE):
                _create_review_status_url(record)
        except Exception as e:
            traceback.print_exc()
            errors.append(e)
    if errors:
        raise Exception(
            f"Encountered {len(errors)} total error(s) during processing. See execution log for errors."
        )


def handle(event, context=None):
    print(f"Triggered for event: {event}")
    if event.get("source") == "aws.events":
        handle_cron_event()
    elif event.get("Records"):
        handle_yelp_table_event(event)
    return {"statusCode": 200}
