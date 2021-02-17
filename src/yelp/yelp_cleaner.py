import json
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List

from yelp.page_fetcher import fetch
from yelp.parser.reviews_page_parser import ReviewsPageParser
from yelp.parser.user_metadata_parser import UserMetadataParser
from yelp.parser.util import to_soup
from yelp.persistence import url_table, yelp_table
from yelp.persistence.config_table import get_all_user_ids
from yelp.persistence.yelp_table import ReviewId
from yelp.url_requester import get_user_metadata_url, get_user_review_page_urls


def emit_emf_metric(url_records_deleted, yelp_records_deleted):
    emf = {
        "_aws": {
            "CloudWatchMetrics": [
                {
                    "Namespace": "YelpOrchestrator",
                    "Dimensions": [],
                    "Metrics": [
                        {"Name": "UrlTableRecordsDeleted", "Unit": "Count"},
                        {"Name": "YelpTableRecordsDeleted", "Unit": "Count"},
                    ],
                }
            ],
            "Timestamp": int(time.time()) * 1000,
        },
        "UrlTableRecordsDeleted": url_records_deleted,
        "YelpTableRecordsDeleted": yelp_records_deleted,
    }
    print(json.dumps(emf))


def fetch_soup(url):
    return to_soup(fetch(url))


def fetch_review_count(user_id):
    soup = fetch_soup(get_user_metadata_url(user_id))
    return UserMetadataParser.get_review_count(soup)


def fetch_biz_ids(review_page_url):
    soup = fetch_soup(review_page_url)
    return [parsed_review.biz_id for parsed_review in ReviewsPageParser.get_user_biz_reviews(soup)]


def get_biz_ids(user_id) -> List[ReviewId]:
    review_count = fetch_review_count(user_id)
    review_page_urls = get_user_review_page_urls(user_id, review_count)

    result = []
    with ThreadPoolExecutor(max_workers=8) as tp:
        for sub_result in tp.map(fetch_biz_ids, review_page_urls):
            result += sub_result

    return result


def cleanup_table(user_id, biz_ids, table, sort_key_prefix):
    current_sort_keys = set([sort_key_prefix + biz_id for biz_id in biz_ids])
    all_review_status_urls = filter(
        lambda record: record["SortKey"].startswith(sort_key_prefix),
        table.get_all_records(user_id),
    )
    records_to_delete = list(
        filter(
            lambda record: record["SortKey"] not in current_sort_keys,
            all_review_status_urls,
        )
    )
    table.delete_records(records_to_delete)
    print(f"Deleted records from {table.__name__}: {records_to_delete}")
    return records_to_delete


def process_user(user_id):
    biz_ids = get_biz_ids(user_id)
    deleted_url_records = cleanup_table(user_id, biz_ids, url_table, "SortKey#ReviewStatusPage#")
    deleted_yelp_records = cleanup_table(user_id, biz_ids, yelp_table, "Review#")
    emit_emf_metric(len(deleted_url_records), len(deleted_yelp_records))


def handle(event, context=None):
    print(f"Triggered for event: {event}")

    for user_id in get_all_user_ids():
        print(f"Processing user_id: {user_id}")
        process_user(user_id)

    return {"statusCode": 200}
