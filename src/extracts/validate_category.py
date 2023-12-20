# Validates if any category exists in file which does not appear in category.csv
# 1. place category.csv and file to be validated in root folder
# 2. run python validate_category.py -f <file name> -t <ynab/txn>

import logging
import argparse

from typing import Final
from pathlib import Path

from utilities import get_extracts_path, setup_logging, validate_category


def setup_args():
    _parser.add_argument("-f", "--file_name",  type=str, help="file to be validated")
    _parser.add_argument("-t", "--type", type=str, help="file to be validated")


def main(args_in: dict):
    extracts_dir = get_extracts_path()
    txn_file_path = Path.joinpath(extracts_dir, str(args_in.get("file_name", "")))
    _status, _result = validate_category(
        file_path=txn_file_path,
        file_type=str(args_in.get("type", ""))
    )

    if _status:
        _logger.info("all categories are present")
    else:
        _logger.info("some categories are missing")
        print(_result)


_logger = logging.getLogger(__name__)
setup_logging()
_parser: Final = argparse.ArgumentParser(
    description="Python utility to process tabula generate csv files from hsbc statements")
setup_args()
args = vars(_parser.parse_args())


if __name__ == '__main__':
    main(args)
