from unittest.mock import patch

from tests.util import get_file
from yelp.parser.review_status_parser import ParsedReviewStatus, ReviewStatusParser
from yelp.parser.util import to_soup
from yelp.persistence.yelp_table import ReviewId


def test_parse():
    # Given
    soup = to_soup(
        get_file("unit/resources/biz/las-galas-los-angeles?hrid=q2pionpcY_-WZPwSWelTFw_dead.html")
    )

    # When
    result = ReviewStatusParser().parse(
        "https://yelp.com/biz/las-galas-los-angeles?hrid=q2pionpcY_-WZPwSWelTFw", soup
    )

    # Then
    assert result == ParsedReviewStatus(
        review_id_tuple=ReviewId(
            biz_id="las-galas-los-angeles", review_id="q2pionpcY_-WZPwSWelTFw"
        ),
        is_alive=False,
    )


def test_parse_dead():
    # Given
    soup = to_soup(
        get_file(
            "unit/resources/biz/thanh-son-tofu-garden-grove-3?hrid=OcEneH8BXu1z8-fpFFyrAg_alive.html"
        )
    )

    # When
    result = ReviewStatusParser().parse(
        "https://yelp.com/biz/thanh-son-tofu-garden-grove-3?hrid=OcEneH8BXu1z8-fpFFyrAg",
        soup,
    )

    # Then
    assert result == ParsedReviewStatus(
        review_id_tuple=ReviewId(
            biz_id="thanh-son-tofu-garden-grove-3", review_id="OcEneH8BXu1z8-fpFFyrAg"
        ),
        is_alive=True,
    )


@patch("yelp.parser.review_status_parser.get_user_id_from_review_id")
@patch("yelp.parser.review_status_parser.update_review_status")
def test_write_result(mock_upsert_review, mock_get_user_id_from_review_id):
    # Given
    review_id_tuple = ReviewId(biz_id="test-biz-id", review_id="test-review-id")
    is_alive = True
    result = ParsedReviewStatus(
        review_id_tuple=review_id_tuple,
        is_alive=is_alive,
    )

    user_id = "test-user-id"
    mock_get_user_id_from_review_id.return_value = user_id

    # When
    parser = ReviewStatusParser()
    parser.write_result("", result)

    # Then
    mock_upsert_review.assert_called_once_with(
        user_id=user_id, review_id=review_id_tuple, status=is_alive
    )
