from bs4 import BeautifulSoup


def to_soup(text):
    return BeautifulSoup(text, "html.parser")


def get_elements_by_classname(soup: BeautifulSoup, classname):
    """Use this method when there are anywhere from 0 to N elements with the class."""
    return soup.find_all(class_=classname)


def get_element_by_classname(soup: BeautifulSoup, classname):
    """Use this method when there is exactly 1 element with the class."""
    results = soup.find_all(class_=classname)
    if len(results) == 0:
        return None
    return results[0]
