from datetime import datetime

from pathlib import Path

DATE_FORMAT = "%Y%m%d"
DATE = datetime.now().strftime("%Y%m%d_%H%M%S")

ROOT_DIR = Path().absolute().parent.parent
APP_DIR = Path.joinpath(ROOT_DIR)

# dropbox-specific
DBX_CREDS_FILE = Path.joinpath(APP_DIR, 'dropbox_api', 'creds.json')
DBX_TOKEN_FILE = Path.joinpath(APP_DIR, 'dropbox_api', 'token.json')
DBX_ENDPOINTS_FILE = Path.joinpath(APP_DIR, 'dropbox_api', 'endpoints.json')

DATA_FILES = ["account", "balance", "budget", "category", "other_balances",
              "transaction_cash", "transaction_hsbc_cc", "transaction_hsbc_dc",
              "transaction_hsbc_savings", "transaction_isa",
              "transaction_other", "transaction_sipp", "transaction_trading",
              "year_month",]

