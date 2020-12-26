import time

import boto3
from boto3.dynamodb.conditions import Key
from yelp.config import URL_TABLE_NAME, URL_TABLE_TTL
from yelp.persistence._util import calculate_ttl

URL_TABLE = boto3.resource("dynamodb").Table(URL_TABLE_NAME)


class UrlTableSchema:
    USER_ID = "UserId"
    URL = "PageUrl"
    SORT_KEY = URL
    LAST_FETCHED = "LastFetched"
    STATUS_CODE = "StatusCode"
    TTL = "TimeToLive"


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
        Key={UrlTableSchema.USER_ID: user_id, UrlTableSchema.SORT_KEY: url},
        UpdateExpression=(f"set {UrlTableSchema.TTL}=:ttl"),
        ExpressionAttributeValues={":ttl": calculate_ttl(ttl)},
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
    user_id = get_user_id_from_url(url)
    URL_TABLE.update_item(
        Key={UrlTableSchema.USER_ID: user_id, UrlTableSchema.SORT_KEY: url},
        UpdateExpression=(
            f"set {UrlTableSchema.STATUS_CODE}=:status_code"
            f", {UrlTableSchema.LAST_FETCHED}=:last_fetched"
        ),
        ExpressionAttributeValues={
            ":status_code": int(status_code),
            ":last_fetched": int(time.time()),
        },
    )
    print(f"Updated fetched URL. [{user_id=}, {url=}, {status_code=}]")
