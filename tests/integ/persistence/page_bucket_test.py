import os

import boto3
from tests.util import random_string

from persistence import page_bucket
from persistence.page_bucket import KeyUtils

PAGE_BUCKET_NAME = os.environ["PAGE_BUCKET_NAME"]
S3 = boto3.resource("s3")


def test_upload_and_download_page():
    # Given
    url = f"https:{random_string}.com"
    html = random_string(100)

    try:
        # When
        page_bucket.upload_page(url, html)
        result = page_bucket.download_page(url)

        # Then
        assert result == html

    finally:
        # Cleanup
        _delete_object(PAGE_BUCKET_NAME, KeyUtils.to_key(url))


def _delete_object(bucket_name, key):
    S3.Object(bucket_name, key).delete()
