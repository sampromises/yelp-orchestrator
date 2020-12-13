from unittest.mock import Mock, patch

from freezegun import freeze_time
from yelp import page_fetcher
from yelp.page_fetcher import gather_batch, process_item


@patch("yelp.page_fetcher.get_all_url_items")
def test_gather_batch(mock_get_all_url_items):
    # Given
    mock_get_all_url_items.return_value = [
        {"Url": "SKIPPED", "ErrorMessage": "Error!"},
        {"Url": "0"},
        {"Url": "1", "Date": 1},
        {"Url": "2", "Date": 2},
        {"Url": "3", "Date": 3},
        {"Url": "4", "Date": 4},
        {"Url": "5", "Date": 5},
    ]
    page_fetcher.FETCH_BATCH_SIZE = 3

    # When
    batch = gather_batch()

    # Then
    assert batch == [
        {"Url": "0"},
        {"Url": "1", "Date": 1},
        {"Url": "2", "Date": 2},
    ]


@freeze_time("2020-08-23")
@patch("yelp.page_fetcher.upload_page")
@patch("yelp.page_fetcher.update_fetched_url")
@patch("yelp.page_fetcher.requests")
def test_process_item_success(mock_requests, mock_update_fetched_url, mock_upload_page):
    # Given
    url = "https://foo.com"
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.content = "content"
    mock_response.text = "text"
    mock_requests.get.return_value = mock_response

    # When
    process_item({"Url": url})

    # Then
    mock_requests.get.assert_called_once_with(url)
    mock_upload_page.assert_called_once_with(url, "content")
    mock_update_fetched_url.assert_called_once_with(url)


@freeze_time("2020-08-23")
@patch("yelp.page_fetcher.upload_page")
@patch("yelp.page_fetcher.update_fetched_url")
@patch("yelp.page_fetcher.requests")
def test_process_item_error(mock_requests, mock_update_fetched_url, mock_upload_page):
    # Given
    url = "https://foo.com"
    mock_response = Mock()
    mock_response.status_code = 404
    mock_response.content = "content"
    mock_response.text = "text"
    mock_requests.get.return_value = mock_response

    # When
    process_item({"Url": url})

    # Then
    mock_requests.get.assert_called_once_with(url)
    mock_upload_page.assert_not_called()
    mock_update_fetched_url.assert_called_once_with(
        url, "Fetch error. [status_code=404, text=text]"
    )
