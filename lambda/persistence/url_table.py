import time

import boto3
from config import URL_TABLE_NAME, URL_TABLE_TTL

from persistence._util import calculate_ttl

URL_TABLE = boto3.resource("dynamodb").Table(URL_TABLE_NAME)


class UrlTableSchema:
    URL = "Url"
    LAST_FETCHED = "LastFetched"
    ERROR = "ErrorMessage"
    TTL = "TimeToLive"


def _update_item(url, update_expression, expression_attribute_values):
    kwargs = {
        "Key": {UrlTableSchema.URL: url},
        "UpdateExpression": update_expression,
        "ExpressionAttributeValues": expression_attribute_values,
    }
    URL_TABLE.update_item(**kwargs)
    print(f"Updated {URL_TABLE_NAME}. [{kwargs=}]")


def get_all_url_items():
    items = []
    response = URL_TABLE.scan()
    items = response["Items"]
    while response.get("LastEvaluatedKey"):
        response = URL_TABLE.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items += response["Items"]
    return items


def upsert_new_url(url, ttl=URL_TABLE_TTL):
    print(f"upsert_new_url({ttl=})")
    _update_item(
        url=url,
        update_expression=f"set {UrlTableSchema.TTL}=:ttl",
        expression_attribute_values={":ttl": calculate_ttl(ttl)},
    )


def update_fetched_url(url, error=None):
    update_expression = f"set {UrlTableSchema.LAST_FETCHED}=:last_fetched"
    expression_attribute_values = {":last_fetched": int(time.time())}
    if error:
        update_expression += f", {UrlTableSchema.ERROR}=:error"
        expression_attribute_values.update({":error": error})
    _update_item(
        url=url,
        update_expression=update_expression,
        expression_attribute_values=expression_attribute_values,
    )
