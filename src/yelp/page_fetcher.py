import requests

from yelp.config import FETCH_BATCH_SIZE
from yelp.persistence.page_bucket import upload_page
from yelp.persistence.url_table import UrlTableSchema, get_all_url_items, update_fetched_url


def gather_batch():
    all_items = get_all_url_items()
    ok_items = filter(lambda item: not item.get("ErrorMessage"), all_items)
    sorted_items = sorted(ok_items, key=lambda x: x.get(UrlTableSchema.LAST_FETCHED, 0))
    return sorted_items[:FETCH_BATCH_SIZE]


def process_item(item):
    url = item["Url"]
    print(f"Processing item. [{url=}]")
    try:
        # Fetch HTML
        resp = requests.get(url)
        print(
            f"GET request finished. [status_code={resp.status_code}, content_length={len(resp.content)}]"
        )
        if resp.status_code != 200:
            raise Exception(f"Fetch error. [status_code={resp.status_code}, text={resp.text}]")

        upload_page(url, resp.content)
        update_fetched_url(url)
    except Exception as err:
        update_fetched_url(url, str(err))


def handle(event, context=None):
    print(f"Triggered for event: {event}")

    items = gather_batch()
    print(f"Gathered batch:\n{items}")

    for item in items:
        process_item(item)

    return {"statusCode": 200}
