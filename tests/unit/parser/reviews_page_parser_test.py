from unittest.mock import call, patch

from tests.util import get_file
from yelp.parser.reviews_page_parser import (
    ParsedReviewMetadata,
    ParsedReviewsPage,
    ReviewsPageParser,
)
from yelp.parser.util import to_soup
from yelp.persistence.yelp_table import ReviewId, ReviewMetadata


def test_parse():
    # Given
    soup = to_soup(
        get_file("unit/resources/user_details_reviews_self/5prk8CtPPBHNpa6BOja2ug_page_last.html")
    )

    # When
    result = ReviewsPageParser("").parse("", soup)

    # Then
    assert result.reviews == [
        ParsedReviewMetadata(
            biz_id="aloha-family-billiards-buena-park",
            biz_name="Aloha Family Billiards",
            biz_address="7311 Orangethorpe Ave Buena Park, CA 90621",
            review_id="U_yiTX51HWo78jOaV0pIxQ",
            review_date="3/13/2020",
        ),
        ParsedReviewMetadata(
            biz_id="chipotle-mexican-grill-cerritos-9",
            biz_name="Chipotle Mexican Grill",
            biz_address="10826 Alondra Blvd Cerritos, CA 90703",
            review_id="ofWhnV6m26kMyCpUjT56XA",
            review_date="1/4/2020",
        ),
        ParsedReviewMetadata(
            biz_id="yoko-buena-park",
            biz_name="Yoko",
            biz_address="4566 Beach Blvd Buena Park, CA 90621",
            review_id="MS1f3LaFfHeh5kcF2qt-1A",
            review_date="1/3/2020",
        ),
        ParsedReviewMetadata(
            biz_id="mdk-noodles-anaheim",
            biz_name="Mdk Noodles",
            biz_address="1000 N Euclid St Anaheim, CA 92801",
            review_id="X0_-zEk0_CKj24wEEoau9A",
            review_date="1/1/2020",
        ),
        ParsedReviewMetadata(
            biz_id="thai-addict-cuisine-buena-park-2",
            biz_name="Thai Addict Cuisine",
            biz_address="6098 Orangethorpe Ave Buena Park, CA 90620",
            review_id="ZfHJEeZFKTPFwAJGMYiXrA",
            review_date="12/31/2019",
        ),
        ParsedReviewMetadata(
            biz_id="panda-express-cerritos",
            biz_name="Panda Express",
            biz_address="11449 S St Cerritos, CA 90703",
            review_id="-eb-SRIXvQ_tyJKFldJMyw",
            review_date="12/29/2019",
        ),
        ParsedReviewMetadata(
            biz_id="in-n-out-burger-la-mirada",
            biz_name="In-N-Out Burger",
            biz_address="14341 Firestone Blvd La Mirada, CA 90638",
            review_id="MD1zzz0eP0GgRLfDqzS7og",
            review_date="12/26/2019",
        ),
        ParsedReviewMetadata(
            biz_id="las-galas-los-angeles",
            biz_name="Las Galas",
            biz_address="103 Japanese Village Plaza Mall Los Angeles, CA 90012",
            review_id="q2pionpcY_-WZPwSWelTFw",
            review_date="12/25/2019",
        ),
    ]


@patch("yelp.parser.reviews_page_parser.upsert_review")
def test_write_result(mock_upsert_review):
    # Given
    user_id = "test-user-id"
    result = ParsedReviewsPage(
        [
            ParsedReviewMetadata(
                "test-biz-id-1",
                "test-biz-name-1",
                "test-biz-address-1",
                "test-review-id-1",
                "test-review-date-1",
            ),
            ParsedReviewMetadata(
                "test-biz-id-2",
                "test-biz-name-2",
                "test-biz-address-2",
                "test-review-id-2",
                "test-review-date-2",
            ),
        ]
    )

    # When
    parser = ReviewsPageParser(user_id)
    parser.write_result(result)

    # Then
    mock_upsert_review.assert_has_calls(
        [
            call(
                user_id=user_id,
                review_id=ReviewId("test-biz-id-1", "test-review-id-1"),
                review_metadata=ReviewMetadata(
                    "test-biz-name-1", "test-biz-address-1", "test-review-date-1"
                ),
            ),
            call(
                user_id=user_id,
                review_id=ReviewId("test-biz-id-2", "test-review-id-2"),
                review_metadata=ReviewMetadata(
                    "test-biz-name-2", "test-biz-address-2", "test-review-date-2"
                ),
            ),
        ]
    )
