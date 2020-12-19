import re
from dataclasses import dataclass

from bs4 import BeautifulSoup
from yelp.parser.base_parser import BaseParser, ParsedResult
from yelp.parser.util import get_element_by_classname
from yelp.persistence.yelp_table import UserMetadata, upsert_metadata


@dataclass
class ParsedUserMetadata(ParsedResult):
    name: str
    city: str
    review_count: int


class UserMetadataParser(BaseParser):
    def parse(self, url, soup: BeautifulSoup) -> ParsedUserMetadata:
        return ParsedUserMetadata(
            name=UserMetadataParser.get_name(soup),
            city=UserMetadataParser.get_city(soup),
            review_count=UserMetadataParser.get_review_count(soup),
        )

    def write_result(self, result: ParsedUserMetadata):
        upsert_metadata(
            user_id=self.user_id,
            user_metadata=UserMetadata(
                name=result.name, city=result.city, review_count=result.review_count
            ),
        )

    @staticmethod
    def get_name(soup: BeautifulSoup) -> str:
        return get_element_by_classname(soup, "user-profile_info").h1.text

    @staticmethod
    def get_city(soup: BeautifulSoup) -> str:
        return get_element_by_classname(soup, "user-location").text

    @staticmethod
    def get_review_count(soup: BeautifulSoup) -> int:
        text = get_element_by_classname(soup, "review-count").text
        return int(re.search(r"([0-9]+)", text).group())
