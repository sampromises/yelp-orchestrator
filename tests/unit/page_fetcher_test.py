from unittest.mock import Mock, call, patch

from freezegun import freeze_time
from tests.util import random_string
from yelp import page_fetcher
from yelp.page_fetcher import BatchProcessor, gather_batch


@patch("yelp.page_fetcher.get_all_url_items")
def test_gather_batch(mock_get_all_url_items):
    # Given
    mock_get_all_url_items.return_value = [
        {"PageUrl": "0"},
        {"PageUrl": "1", "LastFetched": 1},
        {"PageUrl": "2", "LastFetched": 2},
        {"PageUrl": "3", "LastFetched": 3},
        {"PageUrl": "4", "LastFetched": 4},
        {"PageUrl": "5", "LastFetched": 5, "ErrorMessage": "Error!"},
    ]
    page_fetcher.FETCH_BATCH_SIZE = 3

    # When
    batch = gather_batch()

    # Then
    assert batch == [
        {"PageUrl": "0"},
        {"PageUrl": "1", "LastFetched": 1},
        {"PageUrl": "2", "LastFetched": 2},
    ]


@freeze_time("2020-08-23")
@patch("yelp.page_fetcher.upload_page")
@patch("yelp.page_fetcher.update_fetched_url")
@patch("yelp.page_fetcher.requests")
def test_batch_process_single_success(mock_requests, mock_update_fetched_url, mock_upload_page):
    # Given
    url = "https://foo.com"
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.content = "content"
    mock_response.text = "text"
    mock_requests.get.return_value = mock_response

    # When
    batch = BatchProcessor()
    batch.process([{"PageUrl": url}])

    # Then
    mock_requests.get.assert_called_once_with(url)
    mock_upload_page.assert_called_once_with(url, "content")
    mock_update_fetched_url.assert_called_once_with(url, 200)
    assert batch.errors == []


@freeze_time("2020-08-23")
@patch("yelp.page_fetcher.upload_page")
@patch("yelp.page_fetcher.update_fetched_url")
@patch("yelp.page_fetcher.requests")
def test_batch_process_single_error(mock_requests, mock_update_fetched_url, mock_upload_page):
    # Given
    url = "https://foo.com"
    mock_response = Mock()
    mock_response.status_code = 404
    mock_response.content = "content"
    mock_response.text = "text"
    mock_requests.get.return_value = mock_response

    # When
    batch = BatchProcessor()
    batch.process([{"PageUrl": url}])

    # Then
    mock_requests.get.assert_called_once_with(url)
    mock_upload_page.assert_not_called()
    mock_update_fetched_url.assert_called_once_with(url, 404)
    assert len(batch.errors) == 1


@freeze_time("2020-08-23")
@patch("yelp.page_fetcher.upload_page")
@patch("yelp.page_fetcher.update_fetched_url")
@patch("yelp.page_fetcher.requests")
def test_batch_process_multiple_success(mock_requests, mock_update_fetched_url, mock_upload_page):
    # Given
    count = 10

    urls = [random_string() for _ in range(count)]

    mock_responses = []
    for _ in range(count):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = "content"
        mock_response.text = random_string()
        mock_responses.append(mock_response)
    mock_requests.get.side_effect = mock_responses

    # When
    batch = BatchProcessor()
    batch.process([{"PageUrl": url} for url in urls])

    # Then
    mock_requests.get.assert_has_calls([call(url) for url in urls], any_order=True)
    mock_update_fetched_url.assert_has_calls([call(url, 200) for url in urls], any_order=True)
    mock_upload_page.assert_has_calls([call(url, "content") for url in urls], any_order=True)
    assert batch.errors == []


@freeze_time("2020-08-23")
@patch("yelp.page_fetcher.upload_page")
@patch("yelp.page_fetcher.update_fetched_url")
@patch("yelp.page_fetcher.requests")
def test_batch_process_multiple_success_single_error(
    mock_requests, mock_update_fetched_url, mock_upload_page
):
    # Given
    count = 10
    failed_index = 4

    urls = [random_string() for _ in range(count)]
    failed_url = urls[failed_index]

    mock_responses = []
    for i in range(count):
        mock_response = Mock()
        mock_response.status_code = 200
        if i == failed_index:
            mock_response.status_code = 400
        mock_response.content = "content"
        mock_response.text = random_string()
        mock_responses.append(mock_response)
    mock_requests.get.side_effect = mock_responses

    # When
    batch = BatchProcessor()
    batch.process([{"PageUrl": url} for url in urls])

    # Assert successful URLs processed
    success_urls = urls[:failed_index] + urls[failed_index + 1 :]
    mock_requests.get.assert_has_calls([call(url) for url in success_urls], any_order=True)
    mock_update_fetched_url.assert_has_calls(
        [call(url, 200) for url in success_urls], any_order=True
    )
    mock_upload_page.assert_has_calls(
        [call(url, "content") for url in success_urls], any_order=True
    )

    # Assert failed URL processed
    mock_update_fetched_url.assert_has_calls([call(failed_url, 400)])
    assert len(batch.errors) == 1
