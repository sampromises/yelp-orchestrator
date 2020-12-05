import unittest
from unittest.mock import Mock, patch

from tests.util import random_string

from persistence import page_bucket
from persistence.page_bucket import KeyUtils, download_page, upload_page


class TestKeyUtils(unittest.TestCase):
    def test_from_key(self):
        assert (
            KeyUtils.from_key("https%3A%2F%2Ffoo.com%3Farg1%3Dval1%26arg2%3Dval2")
            == "https://foo.com?arg1=val1&arg2=val2"
        )

    def test_to_key(self):
        assert (
            KeyUtils.to_key("https://foo.com?arg1=val1&arg2=val2")
            == "https%3A%2F%2Ffoo.com%3Farg1%3Dval1%26arg2%3Dval2"
        )

    def test_to_and_from_key(self):
        assert (
            KeyUtils.from_key(KeyUtils.to_key("https://foo.com?arg1=val1&arg2=val2"))
            == "https://foo.com?arg1=val1&arg2=val2"
        )


@patch("persistence.page_bucket.KeyUtils")
def test_upload_page(mock_key_utils):
    # Given
    page_bucket_name = "test-bucket-name"
    page_bucket.PAGE_BUCKET_NAME = page_bucket_name

    url = "test-url"
    key = "test-key"
    html_bytes = bytes(random_string(100), encoding="utf8")

    mock_s3, mock_obj = Mock(), Mock()
    mock_s3.Object.return_value = mock_obj
    page_bucket.S3 = mock_s3

    mock_key_utils.to_key.return_value = key

    # When
    upload_page(url, html_bytes)

    # Then
    mock_s3.Object.assert_called_once_with(page_bucket_name, key)
    mock_obj.put.assert_called_once_with(Body=html_bytes)


@patch("persistence.page_bucket.KeyUtils")
def test_download_page(mock_key_utils):
    # Given
    page_bucket_name = "test-bucket-name"
    page_bucket.PAGE_BUCKET_NAME = page_bucket_name

    url = "test-url"
    key = "test-key"
    html = random_string(100)
    html_bytes = bytes(html, encoding="utf8")

    mock_s3, mock_obj, mock_streaming_body = Mock(), Mock(), Mock()
    mock_streaming_body.read.return_value = html_bytes
    mock_obj.get.return_value = {"Body": mock_streaming_body}
    mock_s3.Object.return_value = mock_obj
    page_bucket.S3 = mock_s3

    mock_key_utils.to_key.return_value = key

    # When
    result = download_page(url)

    # Then
    mock_s3.Object.assert_called_once_with(page_bucket_name, key)
    mock_obj.get.assert_called_once_with()
    assert result == html
