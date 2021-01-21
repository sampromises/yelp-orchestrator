import re
import time
from enum import Enum

import boto3
from boto3.dynamodb.conditions import Key
from yelp.config import URL_TABLE_NAME, URL_TABLE_TTL
from yelp.persistence._util import calculate_ttl

URL_TABLE = boto3.resource("dynamodb").Table(URL_TABLE_NAME)


class UrlTableSchema:
    USER_ID = "UserId"
    SORT_KEY = "SortKey"
    URL = "PageUrl"
    LAST_FETCHED = "LastFetched"
    STATUS_CODE = "StatusCode"
    TTL = "TimeToLive"


class UrlType(Enum):
    Metadata = "Metadata"
    UserReviewPage = "UserReviewPage"
    ReviewStatusPage = "ReviewStatusPage"


class UnknownUrlTypeError(Exception):
    pass


# TODO: Similar code in yelp_parser.py
def infer_url_type(url) -> UrlType:
    if "user_details?userid=" in url:
        return UrlType.Metadata
    if "user_details_reviews_self" in url:
        return UrlType.UserReviewPage
    if "/biz/" in url:
        return UrlType.ReviewStatusPage
    raise UnknownUrlTypeError(url)


def get_sort_key_from_url(url):
    url_type = infer_url_type(url)
    sort_key_prefix = f"{UrlTableSchema.SORT_KEY}#{url_type.value}"
    if url_type == UrlType.Metadata:
        return sort_key_prefix
    if url_type == UrlType.UserReviewPage:
        page_num = re.search(r"rec_pagestart=([0-9]+)[\?*]?", url).group(1)
        return f"{sort_key_prefix}#{page_num}"
    if url_type == UrlType.ReviewStatusPage:
        biz_id = re.search(r"/biz/([a-z0-9-]+)[\?&]?", url).group(1)
        return f"{sort_key_prefix}#{biz_id}"


def get_all_url_items():
    items = []
    response = URL_TABLE.scan()
    items = response["Items"]
    while response.get("LastEvaluatedKey"):
        response = URL_TABLE.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items += response["Items"]
    return items


def upsert_new_url(user_id, url, ttl=URL_TABLE_TTL):
    URL_TABLE.update_item(
        Key={
            UrlTableSchema.USER_ID: user_id,
            UrlTableSchema.SORT_KEY: get_sort_key_from_url(url),
        },
        UpdateExpression=f"set {UrlTableSchema.URL}=:url, {UrlTableSchema.TTL}=:ttl",
        ExpressionAttributeValues={
            ":url": url,
            ":ttl": calculate_ttl(ttl),
        },
    )
    print(f"Upserted new URL. [{user_id=}, {url=}]")


class MultipleUserIdsFoundError(Exception):
    pass


# TODO: Pull out shared GSI query method from yelp_table
def get_user_id_from_url(url):
    items = URL_TABLE.query(
        KeyConditionExpression=Key(UrlTableSchema.URL).eq(url),
        IndexName=UrlTableSchema.URL,
    )["Items"]
    if not items:
        return None
    if len(items) > 1:
        raise MultipleUserIdsFoundError(f"More than 1 UserId found for URL. [{url=}, {items=}]")
    return items[0][UrlTableSchema.USER_ID]


def update_fetched_url(url, status_code=-1):
    user_id, sort_key = get_user_id_from_url(url), get_sort_key_from_url(url)
    URL_TABLE.update_item(
        Key={
            UrlTableSchema.USER_ID: user_id,
            UrlTableSchema.SORT_KEY: sort_key,
        },
        UpdateExpression=(
            f"set {UrlTableSchema.STATUS_CODE}=:status_code"
            f", {UrlTableSchema.LAST_FETCHED}=:last_fetched"
        ),
        ExpressionAttributeValues={
            ":status_code": int(status_code),
            ":last_fetched": int(time.time()),
        },
    )
    print(f"Updated fetched URL. [{user_id=}, {sort_key=}, {url=}, {status_code=}]")


def get_all_records(user_id):
    return URL_TABLE.query(KeyConditionExpression=Key(UrlTableSchema.USER_ID).eq(user_id))["Items"]


def delete_records(records):
    keys = [
        {
            UrlTableSchema.USER_ID: item[UrlTableSchema.USER_ID],
            UrlTableSchema.SORT_KEY: item[UrlTableSchema.SORT_KEY],
        }
        for item in records
    ]
    with URL_TABLE.batch_writer() as batch:
        for key in keys:
            batch.delete_item(Key=key)


def delete_user_id(user_id):
    delete_records(get_all_records(user_id))
