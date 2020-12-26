import re
from dataclasses import dataclass
from typing import List

from bs4 import BeautifulSoup
from yelp.parser.base_parser import BaseParser, ParsedResult
from yelp.parser.util import get_elements_by_classname
from yelp.persistence.yelp_table import ReviewId, ReviewMetadata, upsert_review


@dataclass
class ParsedReviewMetadata:
    biz_id: str
    biz_name: str
    biz_address: str
    review_id: str
    review_date: str


@dataclass
class ParsedReviewsPage(ParsedResult):
    reviews: List[ParsedReviewMetadata]


class ReviewsPageParser(BaseParser):
    def parse(self, _, soup: BeautifulSoup) -> ParsedResult:
        return ParsedReviewsPage(reviews=ReviewsPageParser.get_user_biz_reviews(soup))

    def write_result(self, url, result: ParsedResult):
        for scraped in result.reviews:
            upsert_review(
                user_id=ReviewsPageParser.get_user_id_from_url(url),
                review_id=ReviewId(biz_id=scraped.biz_id, review_id=scraped.review_id),
                review_metadata=ReviewMetadata(
                    biz_name=scraped.biz_name,
                    biz_address=scraped.biz_address,
                    review_date=scraped.review_date,
                ),
            )

    DATE_REGEX = re.compile(r"[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}")

    @staticmethod
    def sanitize_address_elem(elem) -> str:
        return elem.decode_contents().strip().replace("<br/>", " ")

    @staticmethod
    def get_user_biz_reviews(soup) -> List[ParsedReviewMetadata]:
        biz_elems = get_elements_by_classname(soup, "biz-name")
        biz_ids = list(map(lambda elem: elem["href"].split("/")[-1], biz_elems))
        biz_names = list(map(lambda elem: elem.get_text(), biz_elems))
        biz_addresses = list(map(ReviewsPageParser.sanitize_address_elem, soup.find_all("address")))

        review_ids = list(
            map(lambda r: r["data-review-id"], get_elements_by_classname(soup, "review"))
        )

        review_date_elems = get_elements_by_classname(soup, "rating-qualifier")

        # Exclude dates from "Previous review"
        review_date_elems = filter(
            lambda e: "Previous review" not in e.get_text(), review_date_elems
        )

        review_dates = list(
            map(
                lambda e: re.search(ReviewsPageParser.DATE_REGEX, e.get_text()).group(),
                review_date_elems,
            )
        )

        tups = zip(biz_ids, biz_names, biz_addresses, review_ids, review_dates)
        scraped_reviews = list(map(lambda tup: ParsedReviewMetadata(*tup), tups))
        return scraped_reviews

    @staticmethod
    def get_user_id_from_url(url: str) -> str:
        return re.search(r"userid=([A-Za-z0-9-_]+)[\?&]?", url).group(1)
