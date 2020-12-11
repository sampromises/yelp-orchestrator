import time


def calculate_ttl(ttl) -> int:
    return int(time.time()) + int(ttl)
