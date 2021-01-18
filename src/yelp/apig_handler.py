import json
from http import HTTPStatus

from yelp.persistence import config_table, url_table, yelp_table
from yelp.persistence.config_table import upsert_user_id

USERS_PATH = r"/users"
USER_PATH = r"/{userId}"

GET = "GET"
POST = "POST"
DELETE = "DELETE"


def get_all_user_records(user_id):
    result = {}
    for item in yelp_table.get_all_records(user_id):
        item_content = {k: str(v) for k, v in item.items() if k not in ("UserId", "SortKey")}
        user_id, sort_key = item["UserId"], item["SortKey"]

        if user_id not in result:
            result[user_id] = {}

        if sort_key == "Metadata":
            result[user_id]["Metadata"] = item_content
        elif sort_key.startswith("Review#"):
            if "Reviews" not in result[user_id]:
                result[user_id]["Reviews"] = []
            result[user_id]["Reviews"].append(item_content)
        else:
            raise Exception(f"Unrecognized sort key while processing. [{user_id=}, {sort_key=}]")
    return result


def handle_users(method):
    if method == GET:
        all_user_records = {}
        for user_id in config_table.get_all_user_ids():
            all_user_records.update(get_all_user_records(user_id))
        return {"statusCode": HTTPStatus.OK, "body": json.dumps(all_user_records)}
    return {"statusCode": HTTPStatus.NOT_IMPLEMENTED}


def handle_user(user_id, method):
    if method == GET:
        return {"statusCode": HTTPStatus.OK, "body": json.dumps(get_all_user_records(user_id))}
    if method == POST:
        upsert_user_id(user_id)
        return {"statusCode": HTTPStatus.OK}
    if method == DELETE:
        config_table.delete_user_id(user_id)
        yelp_table.delete_user_id(user_id)
        url_table.delete_user_id(user_id)
        return {"statusCode": HTTPStatus.OK}
    return {"statusCode": HTTPStatus.NOT_IMPLEMENTED}


def handle(event, context=None):
    print(f"Triggered for event: {event}")
    resource, method = event["resource"], event["httpMethod"]
    print(f"{resource=}, {method=}")
    if resource == USERS_PATH:
        return handle_users(method)
    if resource == USER_PATH:
        user_id = event["pathParameters"]["userId"]
        print(f"{user_id=}")
        return handle_user(user_id, method)
    return {"statusCode": HTTPStatus.NOT_IMPLEMENTED}
