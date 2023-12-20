from datetime import datetime

from pathlib import Path

"""
Environment specific folders
"""

DATE_FORMAT = "%Y%m%d"
DATE = datetime.now().strftime("%Y%m%d_%H%M%S")
EXTRACTS_PATH = Path("C:/MyDocuments/ynab-files/extracts")

"""
Application configuration folders
"""

APP_DIR = Path().absolute().parent.parent
# D:\MyDevelopment\Projects\ynab

CONFIG_DIR = Path.joinpath(APP_DIR, "config")
LOG_DIR = Path.joinpath(APP_DIR, "logs")

LOG_FILENAME = Path.joinpath(LOG_DIR, "ynab.log")
LOG_CONF_FILENAME = Path.joinpath(CONFIG_DIR, "logging_config.json")
