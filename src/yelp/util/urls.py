from enum import Enum

UrlType = Enum("UrlType", "USER_METADATA REVIEWS_PAGE REVIEW_STATUS")


class UnrecognizedUrlTypeError(Exception):
    pass


def get_url_type(url) -> UrlType:
    if "user_details?userid" in url:
        return UrlType.USER_METADATA
    if "user_details_reviews_self" in url:
        return UrlType.REVIEWS_PAGE
    if "/biz/" in url:
        return UrlType.REVIEW_STATUS
    raise UnrecognizedUrlTypeError(url)


# UrlTable -> YelpTable
# url -> Keys
