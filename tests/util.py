import random
import string


def random_string(length=20):
    letters = string.ascii_letters
    return "".join(random.choice(letters) for i in range(length))
