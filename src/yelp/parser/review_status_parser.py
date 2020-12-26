import re
from dataclasses import dataclass

from bs4 import BeautifulSoup
from yelp.parser.base_parser import BaseParser, ParsedResult
from yelp.persistence.yelp_table import ReviewId, get_user_id_from_review_id, update_review_status


@dataclass
class ParsedReviewStatus(ParsedResult):
    review_id_tuple: ReviewId
    is_alive: bool


class ReviewStatusParser(BaseParser):
    def parse(self, url, soup: BeautifulSoup) -> ParsedReviewStatus:
        html = str(soup)
        review_id_tuple: ReviewId = ReviewStatusParser.get_review_id_from_url(url)
        is_alive = ReviewStatusParser.review_id_on_page(html, review_id_tuple.review_id)
        return ParsedReviewStatus(
            review_id_tuple=review_id_tuple,
            is_alive=is_alive,
        )

    def write_result(self, _, result: ParsedReviewStatus):
        update_review_status(
            user_id=get_user_id_from_review_id(result.review_id_tuple.review_id),
            review_id=result.review_id_tuple,
            status=result.is_alive,
        )

    @staticmethod
    def get_review_id_from_url(url) -> ReviewId:
        biz_id = re.search(r"/biz/([a-z0-9-]+)[\?&]?", url).group(1)
        review_id = re.search(r"hrid=(\S+)&?", url).group(1)
        return ReviewId(biz_id, review_id)

    @staticmethod
    def review_id_on_page(page_text, review_id):
        return bool(re.search(review_id, page_text))
