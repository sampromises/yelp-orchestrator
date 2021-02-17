import traceback
from queue import Queue
from threading import Thread
from typing import Dict

import requests

from yelp.config import FETCH_BATCH_SIZE
from yelp.persistence.page_bucket import upload_page
from yelp.persistence.url_table import UrlTableSchema, get_all_url_items, update_fetched_url


class FetchError(Exception):
    def __init__(self, status_code, *args):
        self.status_code = status_code
        super().__init__(args)


def fetch(url):
    resp = requests.get(url)
    print(
        f"GET request finished. [status_code={resp.status_code}, content_length={len(resp.content)}]"
    )
    if resp.status_code != 200:
        raise FetchError(
            resp.status_code,
            f"Fetch error. [status_code={resp.status_code}, text={resp.text}]",
        )
    return resp.content


# TODO: Replace with ThreadPoolExecutor
class BatchProcessor:
    def __init__(self):
        self.queue = Queue()
        self.errors = []

    def consumer(self):
        item = self.queue.get()
        url = item[UrlTableSchema.URL]
        try:
            try:
                content = fetch(url)
                upload_page(url, content)
                status_code = 200
            except FetchError as err:
                self.errors.append(err)
                traceback.print_exc()
                status_code = err.status_code

            update_fetched_url(url, status_code)

        except Exception as err:
            update_fetched_url(url)
            self.errors.append(err)
            traceback.print_exc()
        finally:
            self.queue.task_done()

    def process(self, items):
        # Initialize Queue with sufficient size
        self.queue = Queue(len(items) * 2)

        # Create threads
        for _ in range(len(items)):
            t = Thread(target=self.consumer)
            t.daemon = True
            t.start()

        # Enqueue requests
        for item in items:
            self.queue.put(item)

        # Wait for all threads to finish
        self.queue.join()


def gather_batch():
    sorted_items = sorted(get_all_url_items(), key=lambda x: x.get(UrlTableSchema.LAST_FETCHED, 0))
    return sorted_items[:FETCH_BATCH_SIZE]


def handle(event, context=None):
    print(f"Triggered for event: {event}")

    items = gather_batch()
    print(f"Gathered batch:\n{items}")

    batch = BatchProcessor()
    batch.process(items)
    if batch.errors:
        raise Exception(
            f"Encountered {len(batch.errors)} total error(s) during processing. See execution log for errors."
        )

    return {"statusCode": 200}
