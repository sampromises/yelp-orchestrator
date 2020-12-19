import json
import os
import sys
import traceback

import requests


def usage():
    print(f"Usage: {sys.argv[0]} [--unit|--integ|-u|-i]")
    exit(1)


def load_urls(filepath):
    with open(filepath, "r") as fp:
        return json.loads(fp.read())


def get_test_path(arg):
    if arg in {"--unit", "-u"}:
        return "unit/resources"
    if arg in {"--integ", "-i"}:
        return "integ/resources"
    usage()


def fetch(url):
    resp = requests.get(url)
    if resp.status_code != 200:
        print(f"Error while updating URL: {url}.")
        traceback.print_exc()
    return resp.text


def write_html(filepath, html):
    parent_dirs = os.path.dirname(filepath)
    if not os.path.exists(parent_dirs):
        os.makedirs(parent_dirs)
    with open(filepath, "w+") as fp:
        fp.write(html)


def main(arg):
    root_dir = os.path.dirname(os.path.realpath(__file__))
    dir_path = os.path.join(root_dir, get_test_path(arg))
    for url, rel_path in load_urls(os.path.join(root_dir, "urls.json")).items():
        if html := fetch(url):
            filepath = os.path.join(dir_path, rel_path)
            write_html(filepath, html)
            print(f"SUCCESS: {url} -> {filepath}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        usage()
    main(sys.argv[1])
