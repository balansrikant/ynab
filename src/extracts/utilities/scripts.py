import logging
import json
import pandas as pd

from pathlib import Path
from dateutil.parser import parse
from logging.config import dictConfig
from csv import DictReader

from .constants import CONFIG_DIR, LOG_FILENAME, LOG_CONF_FILENAME, EXTRACTS_PATH

_logger = logging.getLogger(__name__)


def setup_logging(logging_config=LOG_CONF_FILENAME, log_filename=LOG_FILENAME):
    try:
        with open(logging_config, "r") as conf_file:
            config = json.load(conf_file)["logging"]
            config["handlers"]["file"]["filename"] = log_filename
            dictConfig(config)
    except FileNotFoundError:
        _logger.exception("File/path does not exist")


def get_files_list(file_path: Path, suffix: list = None, starts_with: str = "", extension: str = "csv") -> list:
    """
    - If suffix is supplied as a list, such as ['txn'],
        then files matching this pattern are returned: yyyy-mm-dd_<suffix>.<extension>
    - If starts_with parameter is supplied,
        then all files matching this pattern are <starts_with>,,,.<extension>
    - If neither parameter is specified,
        then all files matching this pattern are ,,,.<extension>

    Args:
        file_path: (Path) Pathlib Path
        suffix: (list) type of file e.g. [ynab], [transaction]
        starts_with: str
        extension: (str) extension to search

    Returns:
        list: list of files -> pathlib path
    """
    if not suffix:
        suffix = []
    try:
        _files = file_path.glob(f"*.{extension}")
        _files_list = []
        for _file in _files:
            try:
                if suffix:
                    parse(str(_file.stem[0:10]))
                    if str(_file.stem[11:]).lower() in suffix:
                        _files_list.append(_file)
                elif starts_with:
                    if _file.stem.startswith(starts_with):
                        _files_list.append(_file)
                else:
                    _files_list.append(_file)
            except ValueError:
                _logger.debug(f"file {_file.stem} is not a valid file")
        return _files_list
    except FileNotFoundError:
        _logger.exception(f"File/path {file_path} does not exist")


def get_json(json_path: Path) -> dict:
    """
    Return json file contents as dict

    Args:
        json_path: (Path): file path

    Returns:
        dict: contents of file
    """
    try:
        with open(json_path, "r") as conf_file:
            config = json.load(conf_file)
            return config
    except FileNotFoundError:
        _logger.exception("File/path does not exist")


def get_extracts_path() -> Path:
    return EXTRACTS_PATH


def get_balance() -> pd.DataFrame:
    """Return balance df

    Returns:
        DataFrame: balance dataframe
    """
    balance_file_path = Path.joinpath(get_extracts_path(), "balance.csv")
    _df_balance = pd.read_csv(str(balance_file_path), sep=",")
    _df_balance = _df_balance.astype({
        "opening_balance": 'float64',
        "closing_balance": 'float64',
        "statement_date": 'datetime64',
    })
    return _df_balance


def get_category() -> pd.DataFrame:
    """Return category df

    Returns:
        DataFrame: category dataframe
    """
    extracts_path = get_extracts_path()
    category_file_path = Path.joinpath(extracts_path, "dimensions", "category.csv")
    _df_category = pd.read_csv(str(category_file_path), sep=",")
    return _df_category


def get_budget() -> pd.DataFrame:
    """Return budget df

    Returns:
        DataFrame: budget dataframe
    """
    budget_file_path = Path.joinpath(get_extracts_path(), "powerbi_dir", "budget_values.csv")
    _df_budget = pd.read_csv(str(budget_file_path), sep=",")
    return _df_budget


def validate_category(file_path, file_type) -> (bool, pd.DataFrame):
    """Validate all categories in file exist in category dimension

    Returns:
        tuple: (bool - status of comparison, dataframe if missing categories)
    """
    df_txn = pd.read_csv(file_path)
    if file_type == "ynab":
        df_txn['master_category'] = df_txn["Category"].str.split(':').str[0]
        df_txn['subcategory'] = df_txn["Category"].str.split(':').str[1].str.strip()
    df_cat = get_category()
    result = pd.merge(left=df_txn, right=df_cat, how="left", on=["master_category", "subcategory"])
    result = result.loc[result["enabled"].isnull(), ["master_category", "subcategory"]]
    if result.empty:
        return True, result
    else:
        return False, result


def write_df(df_in: pd.DataFrame, path: str, sep: str = ",") -> bool:
    """Write dataframe to disk

    Args:
        df_in (pd.DataFrame): dataframe to write
        path (str): file path
        sep (str): separator

    Returns:
        bool: confirmation if operation is successful
    """
    try:
        df_in.to_csv(path, index=False, sep=sep)
        return True
    except FileNotFoundError as ex:
        _logger.error(f"...folder path not found", ex)
        return False


def get_payee_mapping() -> list:
    payee_mapping_csv = Path.joinpath(CONFIG_DIR, "payee_mapping.csv")
    with open(payee_mapping_csv, 'r') as f:
        dict_reader = DictReader(f)
        payee_mappings = list(dict_reader)

    return payee_mappings


def get_category_mapping() -> list:
    category_mapping_csv = Path.joinpath(CONFIG_DIR, "category_mapping.csv")
    with open(category_mapping_csv, 'r') as f:
        dict_reader = DictReader(f)
        category_mappings = list(dict_reader)

    return category_mappings
