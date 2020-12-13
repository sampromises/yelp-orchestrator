def handle(event, context=None):
    print(f"Triggered for event: {event}")
    return {"statusCode": 200}
