from unittest.mock import Mock, call, patch
from urllib.parse import quote_plus

import pytest
from yelp.yelp_parser import UnrecognizedUrlError, YelpParserError, handle, process_record


@patch("yelp.yelp_parser.UserMetadataParser")
@patch("yelp.yelp_parser.download_page")
def test_process_record(mock_download_page, mock_parser_cls):
    # Given
    url = "https://user_details?userid"
    record = {"s3": {"object": {"key": quote_plus(url)}}}

    mock_page = Mock()
    mock_download_page.return_value = mock_page

    mock_parser = Mock()
    mock_parser_cls.return_value = mock_parser

    # When
    process_record(record)

    # Then
    mock_parser.process.assert_called_once_with(url, mock_page)


@patch("yelp.yelp_parser.download_page")
def test_process_record_unrecognized_url(_):
    # Given
    url = "foo"
    record = {"s3": {"object": {"key": url}}}

    # When
    with pytest.raises(UnrecognizedUrlError) as error:
        process_record(record)

    # Then
    assert url in str(error.value)


@patch("yelp.yelp_parser.process_record")
def test_handle(mock_process_record):
    # Given
    record_1, record_2, record_3 = Mock(), Mock(), Mock()
    test_event = {"Records": [record_1, record_2, record_3]}
    mock_process_record.side_effect = [
        None,
        Exception(),
        None,
    ]

    # When
    with pytest.raises(YelpParserError) as error:
        handle(test_event)

    # Then
    mock_process_record.assert_has_calls([call(record_1), call(record_2), call(record_3)])
    assert "Encountered 1 total error(s)" in str(error.value)
