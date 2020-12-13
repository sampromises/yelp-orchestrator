import json
from collections import defaultdict
from http import HTTPStatus

import boto3
from boto3.dynamodb.conditions import Key

from yelp.config import YELP_TABLE_NAME


def get_items(user_id):
    table = boto3.resource("dynamodb").Table(YELP_TABLE_NAME)
    return table.query(KeyConditionExpression=Key("UserId").eq(user_id)).get("Items")


def items_to_response(items):
    response = defaultdict(lambda: defaultdict(dict))
    for item in items:
        item_content = {k: str(v) for k, v in item.items() if k not in ("UserId", "SortKey")}
        response[item["UserId"]][item["SortKey"]] = item_content
    return response


def handle(event, context=None):
    print(f"Triggered for event: {event}")
    if event["httpMethod"] == "GET":
        user_id = event["queryStringParameters"].get("userId")
        items = get_items(user_id)
        response = items_to_response(items)
        return {"statusCode": HTTPStatus.OK, "body": json.dumps(response)}
    return {"statusCode": HTTPStatus.NOT_IMPLEMENTED}
