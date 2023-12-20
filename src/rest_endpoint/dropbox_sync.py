import logging
import json
import argparse

from logging.config import dictConfig
from typing import Final

from utilities import LOG_CONF, LOG_FILENAME, DATA_FILES
from dropbox_api import DropboxLocal

_parser: Final = argparse.ArgumentParser(
    description="Python utility to process tabula generate csv files from hsbc statements"
)
_logger = logging.getLogger(__name__)


def setup_logging():
    try:
        with open(LOG_CONF, "r") as conf_file:
            config = json.load(conf_file)["logging"]
            config["handlers"]["file"]["filename"] = LOG_FILENAME
            dictConfig(config)
    except FileNotFoundError:
        _logger.exception("File/path does not exist")


def setup_args():
    """add command line input arguments"""
    _parser.add_argument("--action",
                         type=str,
                         help="action to be performed (sync/download/upload)",
                         required=True)


setup_logging()
setup_args()
args = vars(_parser.parse_args())


def sync_files():
    dbx = DropboxLocal()
    _ = dbx.get_file_metadata()
    dbx.sync_files()


if __name__ == '__main__':
    if args["action"] == "sync":
        sync_files()
