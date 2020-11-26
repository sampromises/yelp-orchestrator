from http import HTTPStatus

HTTP_METHOD = "httpMethod"
GET = "GET"


STATUS_CODE = "statusCode"
BODY = "body"


def handle(event, context=None):
    print(f"Triggered for event: {event}")
    if event[HTTP_METHOD] == GET:
        print("Handling GET")
        return {STATUS_CODE: HTTPStatus.OK, BODY: "Hello GET"}
    return {STATUS_CODE: HTTPStatus.NOT_IMPLEMENTED}
