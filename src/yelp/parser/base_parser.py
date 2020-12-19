from abc import ABC, abstractmethod

from bs4 import BeautifulSoup
from yelp.parser.util import to_soup


class ParsedResult:
    pass


class BaseParser(ABC):
    def __init__(self, user_id):
        self.user_id = user_id

    @abstractmethod
    def parse(self, url, soup: BeautifulSoup) -> ParsedResult:
        pass

    @abstractmethod
    def write_result(self, result: ParsedResult):
        pass

    def process(self, url: str, page: str):
        soup = to_soup(page)
        result = self.parse(url, soup)
        print(f"Parsed result: {result}")
        self.write_result(result)
        print("Wrote result to YelpTable.")
