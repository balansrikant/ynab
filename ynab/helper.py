import logging
import json

from logging.config import dictConfig

from utilities import LOG_CONF, LOG_FILENAME
from dropbox_api import DropboxLocal


_logger = logging.getLogger(__name__)


def setup_logging():
    try:
        with open(LOG_CONF, "r") as conf_file:
            config = json.load(conf_file)["logging"]
            config["handlers"]["file"]["filename"] = LOG_FILENAME
            dictConfig(config)
    except FileNotFoundError:
        _logger.exception("File/path does not exist")


setup_logging()


def main():
    dbx = DropboxLocal()
    files = dbx.get_files()
    for file in files:
        print(file['name'])


if __name__ == '__main__':
    main()
