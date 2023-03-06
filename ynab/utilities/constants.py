from datetime import datetime

from pathlib import Path

DATE_FORMAT = "%Y%m%d"
DATE = datetime.now().strftime("%Y%m%d_%H%M%S")

ROOT_DIR = Path().absolute().parent
DATA_DIR = Path.joinpath(ROOT_DIR, "data")
APP_DIR = Path.joinpath(ROOT_DIR, "ynab")
CONFIG_DIR = Path.joinpath(APP_DIR, "config")

# dropbox-specific
DBX_CREDS_FILE = Path.joinpath(APP_DIR, 'dropbox_api', 'creds.json')
DBX_TOKEN_FILE = Path.joinpath(APP_DIR, 'dropbox_api', 'token.json')
DBX_ENDPOINTS_FILE = Path.joinpath(APP_DIR, 'dropbox_api', 'endpoints.json')

DATA_FILES = ["account", "balance", "budget", "category", "other_balances",
              "transaction_cash", "transaction_hsbc_cc", "transaction_hsbc_dc",
              "transaction_hsbc_savings", "transaction_isa",
              "transaction_other", "transaction_sipp", "transaction_trading",
              "year_month",]
{
    "account": {
        "local_path": str(Path.joinpath(DATA_DIR, 'account.csv')),
        "server_path": "/account.csv"
        },
    "balance": {
        "local_path": str(Path.joinpath(DATA_DIR, 'balance.csv')),
        "server_path": "/balance.csv"
        },
    "budget": {
        "local_path": str(Path.joinpath(DATA_DIR, 'budget.csv')),
        "server_path": "/budget.csv"
        },
    "category": {
        "local_path": str(Path.joinpath(DATA_DIR, 'category.csv')),
        "server_path": "/category.csv"
        },
    "other_balances": {
        "local_path": str(Path.joinpath(DATA_DIR, 'other_balances.csv')),
        "server_path": "/other_balances.csv"
        },
    "transaction_cash": {
        "local_path": str(Path.joinpath(DATA_DIR, 'transaction_cash.csv')),
        "server_path": "/transaction_cash.csv"
        },
    "transaction_hsbc_cc": {
        "local_path": str(Path.joinpath(DATA_DIR, 'transaction_hsbc_cc.csv')),
        "server_path": "/transaction_hsbc_cc.csv"
        },
}

# logging specific
LOG_CONF = Path.joinpath(APP_DIR, "config", "logging_config.json")
LOG_FILENAME = Path.joinpath(ROOT_DIR, "logs", "ynab.log")
LOG_DIR = Path.joinpath(ROOT_DIR, "logs")
