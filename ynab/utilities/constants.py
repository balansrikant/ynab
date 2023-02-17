from datetime import datetime

from pathlib import Path

DATE_FORMAT = "%Y%m%d"
DATE = datetime.now().strftime("%Y%m%d_%H%M%S")

ROOT_DIR = Path().absolute().parent
DATA_DIR = Path.joinpath(ROOT_DIR, 'data')
APP_DIR = Path.joinpath(ROOT_DIR, 'ynab')

# dropbox-specific
DROPBOX_CREDS = Path.joinpath(APP_DIR, 'dropbox_api', 'creds.json')
DROPBOX_TOKEN = Path.joinpath(APP_DIR, 'dropbox_api', 'token.json')
DROPBOX_ENDPOINTS = Path.joinpath(APP_DIR, 'dropbox_api', 'endpoints.json')

DATA_FILES = {
    "account": Path.joinpath(DATA_DIR, 'account.csv'),
    "category": Path.joinpath(DATA_DIR, 'category.csv'),
    "balance": Path.joinpath(DATA_DIR, 'balance.csv')
}

# logging specific
LOG_CONF = Path.joinpath(APP_DIR, "config", "logging_config.json")
LOG_FILENAME = Path.joinpath(ROOT_DIR, "logs", "ynab.log")
