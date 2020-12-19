import os
import random
import string
import time


def random_int(limit=100):
    return random.choice(range(limit))


def random_string(length=20):
    letters = string.ascii_letters
    return "".join(random.choice(letters) for i in range(length))


def get_test_dir():
    abs_path = os.path.join(os.path.abspath(__file__))
    return os.path.dirname(abs_path)


def get_file(filepath) -> str:
    with open(os.path.join(get_test_dir(), filepath), "r") as fp:
        return fp.read().replace("\n", "")


class WaitUntilTimeoutError(Exception):
    pass


def wait_until(fn, fn_args, predicate, timeout=15, poll_delay=1):
    end_time = time.time() + timeout
    while time.time() <= end_time:
        result = fn(*fn_args)
        if predicate(result):
            return result
        time.sleep(poll_delay)
    raise WaitUntilTimeoutError()
