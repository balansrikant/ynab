# Generates ynab file from transaction history
# 1. place transaction history in this format 'yyyy-mm-dd_txn.csc' in the root folder
# 2. run python generate_budget.py

import logging
import pandas as pd
import copy

from csv import DictReader
from pathlib import Path

from utilities import write_df, setup_logging, get_files_list, get_json
from utilities import get_payee_mapping, get_category_mapping, get_balance, get_extracts_path


def transform_payee(original_payee: str) -> (str, str):
    """Helper function, transform payee into friendly name, category

    Args:
        original_payee (str): payee

    Returns:
        tuple: contains friendly payee name, category
    """
    payee_mappings = get_payee_mapping()
    category_mappings = get_category_mapping()

    friendly_name = original_payee
    category = ""

    for payee_mapping in payee_mappings:
        if str(payee_mapping['original_payee']).lower() in original_payee.lower():
            friendly_name = payee_mapping['friendly_name']
            for category_mapping in category_mappings:
                if friendly_name == category_mapping["payee"]:
                    category = category_mapping["category"]

    return friendly_name, category


def clean_df_ynab(df_param: pd.DataFrame) -> pd.DataFrame:
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


def generate_processed_files(extracts_dir: Path):
    """Process txn csv files, output to processed csv files

    Args:
        extracts_dir: (Path): working directory
    """
    _logger.info("**********************************")
    _logger.info("*** generating processed files ***")
    _logger.info("**********************************")

    _logger.info("...get transaction files...")
    _transaction_files_list = []
    try:
        _files = extracts_dir.glob(f"*.csv")
        for _file in _files:
            if str(_file.stem[11:]).lower() == "txn":
                _transaction_files_list.append(_file)
    except FileNotFoundError:
        _logger.exception(f"File/path {extracts_dir} does not exist")

    _logger.info(f"...{len(_transaction_files_list)} files found\n")
    if not _transaction_files_list:
        _logger.info(f"...nothing to process. terminating \n")
        exit()

    _logger.info(f"...cleaning up file into df")
    _clean_dfs = []
    for _file in _transaction_files_list:
        _statement_date = str(_file.stem[0:10])
        _processed_filename = f"{_statement_date}_processed.csv"
        _processed_path = Path.joinpath(extracts_dir, _processed_filename)
        if _processed_path.is_file():
            _logger.info(f"...processed file exists: {_processed_path.stem}... continuing to next file")
            continue

        _df_in = pd.read_csv(str(_file), header=None)
        # set columns
        _cols = ['date', 'payee', 'amount']
        _df_in.columns = _cols

        # transform date
        _df_in[['payee', 'amount']] = _df_in[['payee', 'amount']].astype(str)
        _df_in['date'] = pd.to_datetime(_df_in['date'], dayfirst=True)

        # transform amount
        _df_in.loc[:, 'amount'] = _df_in['amount'].str.replace(',', '')
        _df_in.loc[:, 'amount'] = _df_in['amount'].str.replace('"', '')
        _df_in[['amount']] = _df_in[['amount']].astype(float)

        # reset index
        _df_in.sort_values(by=["date"])
        _df_in.reset_index(drop=True)

        _logger.info("...cleaning up complete")

        _logger.info("...validating df")
        _df_balance = get_balance()

        _opening = _df_balance.loc[_df_balance["statement_date"] == _statement_date, "opening_balance"].values[0]
        _closing = _df_balance.loc[_df_balance["statement_date"] == _statement_date, "closing_balance"].values[0]
        _expected_total = round(_closing - _opening, 2)
        _actual_total = round(_df_in["amount"].sum(), 2)
        _logger.debug(f"...actual opening balance: {_opening}")
        _logger.debug(f"...actual closing balance: {_closing}")
        _logger.debug(f"...expected total: {_expected_total}, actual total: {_actual_total}")
        if _expected_total != _actual_total:
            _logger.info(f"...some transactions are missing\n")
            exit()
        else:
            _logger.info(f"...file is valid\n")
            _clean_df = {"statement_date": _statement_date, "df": _df_in}
            _clean_dfs.append(_clean_df)

    if _clean_dfs:
        _logger.info(f"writing processed dfs to disk...")
        for _clean_df in _clean_dfs:
            _statement_date = _clean_df["statement_date"]
            _processed_filename = f"{_statement_date}_processed.csv"
            _processed_path = str(Path.joinpath(extracts_dir, _processed_filename))
            _logger.info(f"...writing file {_processed_filename}")
            write_df(df_in=_clean_df["df"], path=_processed_path)
        _logger.info("...writing to disk complete\n")
    else:
        _logger.info(f"\nnothing to process\n")


def generate_ynab_files(extracts_dir: Path, suffix: list):
    """Process processed csv files, output to ynab csv files

    Args:
        extracts_dir (str): working directory
        suffix (list): list of files with suffix in [suffix] to process
    """
    _logger.info("**********************************")
    _logger.info("*** generating ynab files ***")
    _logger.info("**********************************")

    _logger.info("get processed files...")
    _processed_files_list = get_files_list(file_path=extracts_dir, suffix=suffix)
    _logger.info(f"...{len(_processed_files_list)} files found\n")
    if not _processed_files_list:
        _logger.info(f"...nothing to process. terminating \n")
        exit()

    _logger.info(f"generating ynab dfs...")
    _ynab_dfs = []
    for _file in _processed_files_list:
        _statement_date = str(_file.stem[0:10])
        _ynab_filename = f"{_statement_date}_ynab.csv"
        _ynab_path = Path.joinpath(extracts_dir, _ynab_filename)
        if _ynab_path.is_file():
            _logger.info(f"...ynab file exists: {_ynab_path.stem}")
            continue

        _logger.info(f"generating ynab file using processed file: {_file.stem}...")
        _statement_date = str(_file.stem[0:10])
        _df_in = pd.read_csv(str(_file), sep=",")
        _df_ynab = clean_df_ynab(_df_in)
        _logger.info(f"...ynab df generation complete\n")
        _ynab_dfs.append({'statement_date': _statement_date, 'df': _df_ynab})

    if _ynab_dfs:
        _logger.info(f"writing ynab dfs to disk...")
        for _ynab_df in _ynab_dfs:
            _statement_date = _ynab_df["statement_date"]
            _ynab_filename = f"{_statement_date}_ynab.csv"
            _ynab_path = str(Path.joinpath(extracts_dir, _ynab_filename))
            _logger.info(f"...writing file {_ynab_filename}")
            write_df(df_in=_ynab_df["df"], path=_ynab_path, sep=",")
        _logger.info("...writing to disk complete\n")
    else:
        _logger.info(f"\nnothing to write\n")


def main():
    extracts_dir = get_extracts_path()

    generate_processed_files(extracts_dir=extracts_dir)
    generate_ynab_files(extracts_dir=extracts_dir, suffix=["processed"])


_logger = logging.getLogger(__name__)
setup_logging()


if __name__ == '__main__':
    main()
