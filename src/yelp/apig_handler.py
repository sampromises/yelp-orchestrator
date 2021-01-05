import json
from collections import defaultdict
from http import HTTPStatus

from yelp.persistence import config_table, url_table, yelp_table
from yelp.persistence.config_table import upsert_user_id


def items_to_response(items):
    response = defaultdict(lambda: defaultdict(dict))
    for item in items:
        item_content = {k: str(v) for k, v in item.items() if k not in ("UserId", "SortKey")}
        response[item["UserId"]][item["SortKey"]] = item_content
    return response


def handle(event, context=None):
    print(f"Triggered for event: {event}")
    method, user_id = event["httpMethod"], event["queryStringParameters"].get("userId")
    print(f"{method=}/{user_id=}")

    if method == "GET":
        items = yelp_table.get_all_records(user_id)
        response = items_to_response(items)
        return {"statusCode": HTTPStatus.OK, "body": json.dumps(response)}
    return {"statusCode": HTTPStatus.NOT_IMPLEMENTED}
