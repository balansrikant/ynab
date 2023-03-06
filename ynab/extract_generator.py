import logging
import json
import os
import pandas as pd
import argparse
import copy

from logging.config import dictConfig
from pathlib import Path
from typing import Final
from csv import DictReader

# noinspection PyUnresolvedReferences
from utilities import LOG_CONF, LOG_FILENAME, CONFIG_DIR

_parser: Final = argparse.ArgumentParser(
    description="Python utility to process tabula generate csv files from hsbc statements"
)
_logger = logging.getLogger(__name__)
os.environ['NUMEXPR_MAX_THREADS'] = '4'
os.environ['NUMEXPR_NUM_THREADS'] = '2'


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

    _parser.add_argument("--root-path",
                         type=str,
                         help="Root directory e.g. D:/MyDocuments/Bank-Statements/HSBC/",
                         required=True)
    _parser.add_argument("--action",
                         type=str,
                         help="action to be performed (process/ynab/transaction)",
                         required=True)


setup_logging()
setup_args()
args = vars(_parser.parse_args())


def get_data_folders(root_path: str) -> dict:
    """Return sub data folders.

    Args:
        root_path (str): Root directory

    Returns:
        dict: dictionary containing unprocessed, tabula, processed, ynab, transaction, dir paths
    """

    _folders = dict()
    _folders["root_path"] = Path(root_path)
    _folders["unprocessed_dir"] = Path.joinpath(_folders["root_path"], "unprocessed")

    _folders["tabula_dir"] = Path.joinpath(_folders["unprocessed_dir"], "1-tabula-output")
    _folders["processed_dir"] = Path.joinpath(_folders["unprocessed_dir"], "2-processed")
    _folders["ynab_dir"] = Path.joinpath(_folders["unprocessed_dir"], "3-ynab")
    _folders["transaction_dir"] = Path.joinpath(_folders["unprocessed_dir"], "4-transaction")

    return _folders


def get_files_list(file_dir: Path, extension: str, file_type: str) -> list:
    """Return csv statement files in a folder

    Args:
        file_dir (Path): Pathlib Path
        extension (str): extension to search for
        file_type (str): type of file e.g. tabula, processed, ynab, transaction

    Returns:
        list: list of files (dict) -> statement_date, pathlib path
    """
    p = Path(file_dir).glob('**/*')
    files = [x for x in p if x.is_file()]
    result = [{"statement_date": x.stem[0:10], f"{file_type}_path": x}
              for x in files if x.suffix == f".{extension}"]
    return result


def get_balances(balances_file: Path) -> pd.DataFrame:
    """Load balances from csv into dataframe.

    Args:
        balances_file (Path): path of balances csv file

    Returns:
        dataframe: dataframe containing balances
    """
    df_balances = pd.read_csv(str(balances_file))
    cols = ['statement_date', 'opening_balance', 'closing_balance']
    df_balances.columns = cols
    df_balances = df_balances.astype({"opening_balance": 'float64',
                                      "closing_balance": 'float64',
                                      })
    df_balances["statement_date"] = pd.to_datetime(df_balances["statement_date"], dayfirst=True)
    return df_balances


# noinspection PyTypeChecker
def clean_df(df_param: pd.DataFrame) -> pd.DataFrame:
    """clean tabula csv"""
    df_in = copy.deepcopy(df_param)
    _cols = ['date', 'transaction_type', 'payee', 'outflow', 'inflow', 'balance']

    # set columns
    df_in.columns = _cols

    # transform data types, reset index
    df_in[['payee', 'outflow', 'inflow', 'balance']] = df_in[['payee', 'outflow', 'inflow', 'balance']].astype(str)
    df_in['date'] = pd.to_datetime(
        df_in['date'].str[:2]
        + '-'
        + df_in['date'].str[3:6]
        + '-'
        + '20' + df_in['date'].str[-2:]
    )
    df_in.reset_index(drop=True, inplace=True)

    # # remove unneeded columns
    # _cols = ['date', 'payee', 'outflow', 'inflow', 'balance']
    # df_in = df_in[_cols]

    # remove thousands separator, change data types
    df_in.loc[:, 'balance'] = df_in['balance'].str.replace(',', '')
    df_in.loc[:, 'outflow'] = df_in['outflow'].str.replace(',', '')
    df_in.loc[:, 'inflow'] = df_in['inflow'].str.replace(',', '')
    df_in.loc[df_in['outflow'] == '.', 'outflow'] = 0.0
    df_in = df_in.astype({'outflow': float, 'inflow': float, 'balance': float})

    # fill blanks
    df_in['outflow'].fillna(0, inplace=True)
    df_in['inflow'].fillna(0, inplace=True)
    df_in['balance'].fillna(0, inplace=True)

    # remove balance carried forward rows within dataset
    # (?i) -> regex case insensitive mode
    # https://www.regular-expressions.info/modifiers.html
    df_temp = df_in.iloc[1:-1, :]
    idx = df_temp.loc[df_temp['payee'].str.contains('(?i)balance'), :].index
    df_in.drop(idx, inplace=True)

    # combine multi-line payees
    # iterrows -> iterate over rows as pairs
    # https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.iterrows.html
    for idx, row in df_in.iterrows():
        if ((df_in.loc[idx, 'outflow'] == 0.0)
                and (df_in.loc[idx, 'inflow'] == 0.0) and (df_in.loc[idx, 'balance'] == 0.0)):
            df_in.loc[idx + 1, 'payee'] = df_in.loc[idx, 'payee'] + ' ' + df_in.loc[idx + 1, 'payee']
            df_in.loc[idx + 1, 'date'] = df_in.loc[idx, 'date']

    df_in.drop(df_in[(df_in['outflow'] == 0.0) & (df_in['inflow'] == 0.0)
                     & (df_in['balance'] == 0.0)].index, inplace=True)
    df_in['date'].fillna(method='ffill', inplace=True)

    # combine outflow and inflow
    df_in.loc[:, 'outflow'] = (df_in['outflow'] * -1) + df_in['inflow']
    df_in = df_in.rename(columns={'outflow': 'amount'})
    df_in.drop(df_in[(df_in['amount'] == 0.0) & (df_in['balance'] == 0.0)].index, inplace=True)
    df_in['date'].fillna(method='ffill', inplace=True)

    cols = ['date', 'payee', 'amount', 'balance']
    df_in = df_in[cols]

    return df_in


def validate_df(df_in: pd.DataFrame, statement_date: str, df_balances: pd.DataFrame) -> bool:
    """Validate dataframe if opening and closing balances are correct

    Args:
        df_in (pd.DataFrame): dataframe to be evaluated
        statement_date (str): file statement date
        df_balances (pd.DataFrame): dataframe containing opening and closing balances

    Returns:
        bool: confirmation if file opening and closing balances reconcile
    """
    # get opening balance
    calculated_opening = df_in.loc[df_in['payee'].str.upper() == 'BALANCE BROUGHT FORWARD', 'balance'].values[0]
    calculated_opening = round(calculated_opening, 2)

    # get transaction rows
    df_tran = df_in.loc[(df_in['payee'].str.upper() != 'BALANCE BROUGHT FORWARD')
                        & (df_in['payee'].str.upper() != 'BALANCE CARRIED FORWARD'), :]

    # get closing balance
    total_amount = df_tran['amount'].sum()
    calculated_closing = calculated_opening + total_amount
    calculated_closing = round(calculated_closing, 2)

    # get actual values from balances file
    actual_opening = df_balances.loc[
        df_balances['statement_date'] == statement_date, 'opening_balance'].values[0]
    actual_closing = df_balances.loc[
        df_balances['statement_date'] == statement_date, 'closing_balance'].values[0]
    actual_opening = round(actual_opening, 2)
    actual_closing = round(actual_closing, 2)

    _logger.info(f"...actual opening: {actual_opening}, calculated_opening: {calculated_opening}")
    _logger.info(f"...actual closing: {actual_closing}, calculated_closing: {calculated_closing}")
    if actual_opening != calculated_opening or actual_closing != calculated_closing:
        valid = False
    else:
        valid = True
    return valid


def get_processed_dfs(tabula_files_list: list) -> list:
    """Load list of tabula csv files, clean, validate and return list of processed dataframes

    Args:
        tabula_files_list (list): list of file (Path) objects

    Returns:
        list: list of processed dataframes along with metadata
    """
    _logger.info("get balances...")
    df_balances = get_balances(Path.joinpath(Path(args["root_path"]), "balances.csv"))

    _logger.info(f"cleaning up and validating files...")
    _processed_df_list = []
    _invalid_file_count = 0
    for file in tabula_files_list:
        _logger.info(f"...cleaning up file: {file['tabula_path']}")
        pathlib_path = file['tabula_path']
        _df_in = pd.read_csv(pathlib_path, header=None)
        _df_clean = clean_df(_df_in)
        _logger.info(f"...cleaning up complete")
        _logger.info(f"...validating file: {file['tabula_path']}")
        if validate_df(df_in=_df_clean, statement_date=file['statement_date'], df_balances=df_balances):
            _logger.info(f"...file is valid")
            _processed_df_list.append(
                {
                    'statement_date': file['statement_date'],
                    'tabula_path': file['tabula_path'],
                    'processed_df': _df_clean
                }
            )
        else:
            _logger.error(f"...{file['pathlib_path']} is invalid.")
            _invalid_file_count += 1

    return _processed_df_list


def generate_processed_files(folders_in: dict) -> bool:
    """Process tabula csv files, output to processed csv files

    Args:
        folders_in (dict): folder paths

    Returns:
        bool: confirmation if operation is successful
    """
    tabula_files_list = get_files_list(file_dir=folders_in["tabula_dir"], extension='csv', file_type='tabula')
    processed_dfs = get_processed_dfs(tabula_files_list=tabula_files_list)

    _logger.info(f"writing dfs to disk...")
    status = True
    for file in processed_dfs:
        filename = f"{file['statement_date']}_processed.csv"
        path = Path.joinpath(folders_in["processed_dir"], filename)
        _logger.info(f"...writing file {path}")
        status = write_df(df_in=file["processed_df"], path=path)
        if status:
            _logger.info(f"...file written successfully")
        else:
            _logger.error(f"...error while writing file")
            status = False

    return status


def transform_payee(original_payee: str) -> (str, str):
    """Helper function, transform payee into friendly name, category

    Args:
        original_payee (str): payee

    Returns:
        tuple: contains friendly payee name, category
    """
    payee_mapping_csv = Path.joinpath(CONFIG_DIR, "payee_mapping.csv")
    category_mapping_csv = Path.joinpath(CONFIG_DIR, "category_mapping.csv")

    with open(payee_mapping_csv, 'r') as f:
        dict_reader = DictReader(f)
        payee_mappings = list(dict_reader)

    with open(category_mapping_csv, 'r') as f:
        dict_reader = DictReader(f)
        category_mappings = list(dict_reader)

    friendly_name = original_payee
    category = ""

    for payee_mapping in payee_mappings:
        if str(payee_mapping['original_payee']).lower() in original_payee.lower():
            friendly_name = payee_mapping['friendly_name']
            for category_mapping in category_mappings:
                if friendly_name == category_mapping["payee"]:
                    category = category_mapping["category"]

    return friendly_name, category


def ynab_df(df_param: pd.DataFrame) -> pd.DataFrame:
    """generate ynab df"""
    df_ynab = copy.deepcopy(df_param)

    # rename columns
    df_ynab.rename(columns={'date': 'Date', 'payee': 'Payee'}, inplace=True)
    df_ynab['Date'] = pd.to_datetime(df_ynab['Date'])

    # remove balance rows
    idx = df_ynab.loc[df_ynab['Payee'].str.contains('(?i)balance'), :].index
    df_ynab.drop(idx, inplace=True)

    # create additional ynab columns, format values
    df_ynab['Category'] = ''
    df_ynab['Memo'] = ''
    df_ynab.loc[df_ynab['amount'] < 0, 'Outflow'] = df_ynab['amount'] * -1
    df_ynab.loc[df_ynab['amount'] > 0, 'Inflow'] = df_ynab['amount']
    df_ynab['Outflow'].fillna('', inplace=True)
    df_ynab['Inflow'].fillna('', inplace=True)
    cols = ['Date', 'Payee', 'Category', 'Memo', 'Outflow', 'Inflow']
    df_ynab = df_ynab[cols]
    df_ynab['Date'] = df_ynab['Date'].apply(lambda x: x.strftime('%Y-%m-%d'))

    # process payees
    df_ynab[['Payee', 'Category']] = df_ynab['Payee'].apply(transform_payee).apply(pd.Series)
    return df_ynab


def get_ynab_dfs(processed_files_list: list) -> list:
    """Load list of processed files, return list of ynab dataframes

    Args:
        processed_files_list (list): list of file (Path) objects

    Returns:
        list: list of processed dataframes along with metadata
    """
    _logger.info(f"generating ynab dfs files...")
    _ynab_df_list = []
    _invalid_file_count = 0
    for file in processed_files_list:
        _logger.info(f"...generating ynab file using processed file: {file['processed_path']}")
        pathlib_path = file['processed_path']
        _df_in = pd.read_csv(pathlib_path)
        _df_ynab = ynab_df(_df_in)
        _logger.info(f"...ynab generation complete")
        _ynab_df_list.append(
            {
                'statement_date': file['statement_date'],
                'processed_path': file['processed_path'],
                'ynab_df': _df_ynab
            }
        )
    return _ynab_df_list


def generate_ynab_files(folders_in: dict):
    """Process processed csv files, output to ynab csv files

    Args:
        folders_in (dict): folder paths

    Returns:
        bool: confirmation if operation is successful
    """
    processed_files_list = get_files_list(file_dir=folders_in["processed_dir"], extension='csv', file_type='processed')
    ynab_dfs = get_ynab_dfs(processed_files_list=processed_files_list)
    _logger.info(f"writing dfs to disk...")
    status = True
    for file in ynab_dfs:
        filename = f"{file['statement_date']}_ynab.csv"
        path = Path.joinpath(folders_in["ynab_dir"], filename)
        _logger.info(f"...writing file {path}")
        status = write_df(df_in=file["ynab_df"], path=path)
        if status:
            _logger.info(f"...file written successfully")
        else:
            _logger.error(f"...error while writing file")
            status = False

    return status


def transaction_df(df_param: pd.DataFrame, statement_date: str) -> list:
    """generate transaction dfs"""
    df_ynab = copy.deepcopy(df_param)

    # combine outflow and inflow
    df_ynab = df_ynab.astype({'Outflow': float, 'Inflow': float})
    df_ynab['Outflow'].fillna(0, inplace=True)
    df_ynab['Inflow'].fillna(0, inplace=True)
    df_ynab['amount'] = df_ynab['Inflow'] - df_ynab['Outflow']

    # construct columns
    df_ynab['account'] = 'HSBC DC'
    df_ynab.rename(columns={'Date': 'transaction_date', 'Payee': 'payee', 'Memo': 'memo'}, inplace=True)
    df_ynab.loc[df_ynab['payee'] == 'Transfer : Cash', 'Category'] = 'Transfer-out: Cash'
    df_ynab['master_category'] = df_ynab['Category'].str.split(':').str[0]
    df_ynab['subcategory'] = df_ynab['Category'].str.split(':').str[1].str.strip()
    df_ynab['transaction_date'] = pd.to_datetime(df_ynab['transaction_date'], dayfirst=True)
    df_ynab = df_ynab.astype({'amount': float})

    # construct df_cash
    df_cash = copy.deepcopy(df_ynab.loc[df_ynab['payee'] == 'Transfer : Cash', :])
    if not df_cash.empty:
        df_cash['account'] = 'Cash'
        df_cash['amount'] = df_cash['amount'] * -1
        df_cash['payee'] = 'Transfer : HSBC DC'
        df_cash['master_category'] = 'Transfer-in'
        df_cash['subcategory'] = 'HSBC DC'

        df_cash_debit = copy.deepcopy(df_cash)
        df_cash_debit['amount'] = df_cash_debit['amount'] * -1
        df_cash_debit['payee'] = 'Misc'
        df_cash_debit['master_category'] = 'Monthly'
        df_cash_debit['subcategory'] = 'Discretionary'

        df_cash = pd.concat([df_cash, df_cash_debit])
        df_cash.sort_values(by=['transaction_date', 'amount'], inplace=True)

    cols = ['account', 'transaction_date', 'payee', 'master_category', 'subcategory', 'memo', 'amount']
    df_ynab = df_ynab[cols]
    transaction_files = []
    file_transaction_dc = {
        "statement_date": statement_date,
        "file_type": "hsbc_dc",
        "transaction_df": df_ynab
    }
    transaction_files.append(file_transaction_dc)
    if not df_cash.empty:
        df_cash = df_cash[cols]
        file_transaction_cash = {
            "statement_date": statement_date,
            "file_type": "cash",
            "transaction_df": df_cash
        }
        transaction_files.append(file_transaction_cash)

    return transaction_files


def get_transaction_dfs(ynab_files_list: list) -> list:
    """Load list of ynab files, return list of transaction dataframes

    Args:
        ynab_files_list (list): list of file (Path) objects

    Returns:
        list: list of processed dataframes along with metadata
    """
    _logger.info(f"generating transaction dfs...")
    _transaction_df_list = []
    _invalid_file_count = 0
    for file in ynab_files_list:
        _logger.info(f"...generating transaction dfs using ynab file: {file['ynab_path']}")
        pathlib_path = file['ynab_path']
        _df_in = pd.read_csv(pathlib_path)
        _dfs = transaction_df(df_param=_df_in, statement_date=file['statement_date'])
        _logger.info(f"...transaction dfs generation complete")
        for _df in _dfs:
            _transaction_df_list.append(_df)
    return _transaction_df_list


def generate_transaction_files(folders_in: dict):
    """Process ynab csv files, output to transaction csv files

    Args:
        folders_in (dict): folder paths

    Returns:
        bool: confirmation if operation is successful
    """
    ynab_files_list = get_files_list(file_dir=folders_in["ynab_dir"], extension='csv', file_type='ynab')
    transaction_dfs = get_transaction_dfs(ynab_files_list=ynab_files_list)
    _logger.info(f"writing dfs to disk...")
    status = True
    for file in transaction_dfs:
        filename = f"{file['statement_date']}_transaction_{file['file_type']}.csv"
        path = Path.joinpath(folders_in["transaction_dir"], filename)
        _logger.info(f"...writing file {path}")
        status = write_df(df_in=file["transaction_df"], path=path, sep='|')
        if status:
            _logger.info(f"...file written successfully")
        else:
            _logger.error(f"...error while writing file")
            status = False

    return status


def write_df(df_in: pd.DataFrame, path: str, sep: str=",") -> bool:
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


if __name__ == '__main__':
    folders = get_data_folders(args["root_path"])
    if args["action"] == "process":
        generate_processed_files(folders_in=folders)
    elif args["action"] == "ynab":
        generate_ynab_files(folders_in=folders)
    elif args["action"] == "transaction":
        generate_transaction_files(folders_in=folders)
