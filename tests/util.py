import random
import string
import time


def random_int(limit=100):
    return random.choice(range(limit))


def random_string(length=20):
    letters = string.ascii_letters
    return "".join(random.choice(letters) for i in range(length))
