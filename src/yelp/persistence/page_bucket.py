from urllib.parse import quote_plus, unquote_plus

import boto3
from yelp.config import PAGE_BUCKET_NAME

S3 = boto3.resource("s3")


class KeyUtils:
    @staticmethod
    def to_key(url):
        return quote_plus(url)

    @staticmethod
    def from_key(key):
        return unquote_plus(key)


def upload_page(url, html: bytes):
    key = KeyUtils.to_key(url)
    obj = S3.Object(PAGE_BUCKET_NAME, key)
    obj.put(Body=html)
    print(f"Uploaded page. [url={url}, length={len(html)}]")


def download_page(url):
    key = KeyUtils.to_key(url)
    obj = S3.Object(PAGE_BUCKET_NAME, key)
    html = obj.get()["Body"].read().decode("utf-8")
    print(f"Downloaded page. [url={url}, length={len(html)}]")
    return html
