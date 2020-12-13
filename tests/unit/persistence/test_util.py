from datetime import datetime

from freezegun import freeze_time
from yelp.persistence._util import calculate_ttl


@freeze_time("2020-08-23")
def test_calculate_ttl():
    ttl = 24
    assert calculate_ttl(ttl) == int(datetime(2020, 8, 23).timestamp()) + ttl
