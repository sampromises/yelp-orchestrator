import time

import boto3
from yelp.config import CONFIG_TABLE_NAME

CONFIG_TABLE = boto3.resource("dynamodb").Table(CONFIG_TABLE_NAME)


class ConfigTableSchema:
    USER_ID = "UserId"
    LAST_MODIFIED = "LastModified"


def upsert_user_id(user_id):
    CONFIG_TABLE.put_item(
        Item={ConfigTableSchema.USER_ID: user_id, ConfigTableSchema.LAST_MODIFIED: int(time.time())}
    )


def get_all_user_ids():
    response = CONFIG_TABLE.scan()
    items = response["Items"]
    while response.get("LastEvaluatedKey"):
        response = CONFIG_TABLE.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items += response["Items"]
    return list(map(lambda item: item[ConfigTableSchema.USER_ID], items))


def delete_user_id(user_id):
    CONFIG_TABLE.delete_item(Key={ConfigTableSchema.USER_ID: user_id})
