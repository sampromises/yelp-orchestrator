import time
from collections import namedtuple

import boto3
from boto3.dynamodb.conditions import Key
from yelp.config import YELP_TABLE_NAME, YELP_TABLE_TTL
from yelp.persistence._util import calculate_ttl

YELP_TABLE = boto3.resource("dynamodb").Table(YELP_TABLE_NAME)


class _YelpTableSchema:
    USER_ID = "UserId"
    SORT_KEY = "SortKey"
    LAST_MODIFIED = "LastModified"
    TTL = "TimeToLive"


class _MetadataSchema(_YelpTableSchema):
    SORT_KEY_VALUE = "Metadata"
    NAME = "UserName"
    CITY = "City"
    REVIEW_COUNT = "ReviewCount"


class _ReviewSchema(_YelpTableSchema):
    SORT_KEY_VALUE = "Review"
    BIZ_ID = "BizId"
    BIZ_NAME = "BizName"
    BIZ_ADDRESS = "BizAddress"
    REVIEW_ID = "ReviewId"
    REVIEW_DATE = "ReviewDate"
    REVIEW_STATUS = "ReviewStatus"


UserMetadata = namedtuple("UserMetadata", "name city review_count")
ReviewId = namedtuple("ReviewId", "biz_id review_id")
ReviewMetadata = namedtuple("ReviewMetadata", "biz_name biz_address review_date")


def _upsert_record(user_id, sort_key, update_expression, expression_attribute_values):
    update_expression += ", LastUpdated=:last_updated"
    expression_attribute_values[":last_updated"] = int(time.time())
    kwargs = {
        "Key": {_YelpTableSchema.USER_ID: user_id, _YelpTableSchema.SORT_KEY: sort_key},
        "UpdateExpression": update_expression,
        "ExpressionAttributeValues": expression_attribute_values,
    }
    YELP_TABLE.update_item(**kwargs)
    print(f"Updated {YELP_TABLE_NAME}. [{kwargs=}]")


def get_all_records(user_id):
    return YELP_TABLE.query(KeyConditionExpression=Key(_YelpTableSchema.USER_ID).eq(user_id))[
        "Items"
    ]


def upsert_metadata(user_id, user_metadata: UserMetadata, ttl=YELP_TABLE_TTL):
    _upsert_record(
        user_id,
        _MetadataSchema.SORT_KEY_VALUE,
        (
            f"set {_MetadataSchema.NAME}=:name"
            f", {_MetadataSchema.CITY}=:city"
            f", {_MetadataSchema.REVIEW_COUNT}=:review_count"
            f", {_YelpTableSchema.TTL}=:ttl"
        ),
        {
            ":name": user_metadata.name,
            ":city": user_metadata.city,
            ":review_count": user_metadata.review_count,
            ":ttl": calculate_ttl(ttl),
        },
    )


def upsert_review(
    user_id, review_id: ReviewId, review_metadata: ReviewMetadata, ttl=YELP_TABLE_TTL
):
    _upsert_record(
        user_id,
        f"{_ReviewSchema.SORT_KEY_VALUE}#{review_id.biz_id}",
        (
            f"set {_ReviewSchema.BIZ_ID}=:biz_id"
            f", {_ReviewSchema.REVIEW_ID}=:review_id"
            f", {_ReviewSchema.BIZ_NAME}=:biz_name"
            f", {_ReviewSchema.BIZ_ADDRESS}=:biz_address"
            f", {_ReviewSchema.REVIEW_DATE}=:review_date"
            f", {_YelpTableSchema.TTL}=:ttl"
        ),
        {
            ":biz_id": review_id.biz_id,
            ":review_id": review_id.review_id,
            ":biz_name": review_metadata.biz_name,
            ":biz_address": review_metadata.biz_address,
            ":review_date": review_metadata.review_date,
            ":ttl": calculate_ttl(ttl),
        },
    )


def update_review_status(user_id, review_id: ReviewId, status):
    _upsert_record(
        user_id,
        f"{_ReviewSchema.SORT_KEY_VALUE}#{review_id.biz_id}",
        (f"set {_ReviewSchema.REVIEW_STATUS}=:status"),
        {":status": status},
    )


class NoUserIdFoundError(Exception):
    pass


class MultipleUserIdsFoundError(Exception):
    pass


def get_user_id_from_review_id(review_id: str):
    items = YELP_TABLE.query(
        KeyConditionExpression=Key(_ReviewSchema.REVIEW_ID).eq(review_id),
        IndexName=_ReviewSchema.REVIEW_ID,
    )["Items"]
    if not items:
        raise NoUserIdFoundError(f"No UserId found for ReviewId. [{review_id=}]")
    if len(items) > 1:
        raise MultipleUserIdsFoundError(
            f"More than 1 UserId found for ReviewId. [{review_id=}, {items=}]"
        )
    return items[0][_YelpTableSchema.USER_ID]


def delete_records(records):
    keys = [
        {
            _YelpTableSchema.USER_ID: item[_YelpTableSchema.USER_ID],
            _YelpTableSchema.SORT_KEY: item[_YelpTableSchema.SORT_KEY],
        }
        for item in records
    ]
    with YELP_TABLE.batch_writer() as batch:
        for key in keys:
            batch.delete_item(Key=key)


def delete_user_id(user_id):
    delete_records(get_all_records(user_id))
