from unittest.mock import patch

from tests.util import get_file
from yelp.parser.user_metadata_parser import ParsedUserMetadata, UserMetadataParser
from yelp.parser.util import to_soup
from yelp.persistence.yelp_table import UserMetadata


def test_parse():
    # Given
    soup = to_soup(get_file("unit/resources/user_details/5prk8CtPPBHNpa6BOja2ug.html"))

    # When
    result = UserMetadataParser().parse("", soup)

    # Then
    assert result == ParsedUserMetadata("Samuelze K.", "Palos Verdes Estates, CA", 148)


@patch("yelp.parser.user_metadata_parser.upsert_metadata")
def test_write_result(mock_upsert_metadata):
    # Given
    user_id = "test-user-id"
    url = f"https://www.yelp.com/user_details?userid={user_id}"
    result = ParsedUserMetadata("test-name", "test-city", 24)

    # When
    parser = UserMetadataParser()
    parser.write_result(url, result)

    # Then
    mock_upsert_metadata.assert_called_once_with(
        user_id=user_id,
        user_metadata=UserMetadata(name="test-name", city="test-city", review_count=24),
    )
