import traceback
from urllib.parse import unquote_plus

from yelp.config import YELP_USER_ID
from yelp.parser.base_parser import BaseParser
from yelp.parser.review_status_parser import ReviewStatusParser
from yelp.parser.reviews_page_parser import ReviewsPageParser
from yelp.parser.user_metadata_parser import UserMetadataParser
from yelp.persistence.page_bucket import KeyUtils, download_page


class YelpParserError(Exception):
    pass


class UnrecognizedUrlError(YelpParserError):
    pass


def get_parser(url) -> BaseParser:
    if "user_details?userid" in url:
        return UserMetadataParser
    if "user_details_reviews_self" in url:
        return ReviewsPageParser
    if "/biz/" in url:
        return ReviewStatusParser
    raise UnrecognizedUrlError(url)


def parse_key(record):
    return unquote_plus(record["s3"]["object"]["key"])  # Received S3 message is encoded once


def process_record(record):
    key = parse_key(record)
    url = KeyUtils.from_key(key)
    print(f"Processing record. [{key=}, {url=}]")
    page = download_page(url)
    if parser_cls := get_parser(url):
        parser_cls(YELP_USER_ID).process(url, page)


def handle(event, context=None):
    print(f"Triggered for event: {event}")

    errors = []
    for record in event["Records"]:
        try:
            process_record(record)
        except Exception as e:
            print(f"Error occurred while processing record: {record}")
            traceback.print_exc()
            errors.append(e)

    if errors:
        raise YelpParserError(
            f"Encountered {len(errors)} total error(s) during processing. See execution log for errors."
        )
