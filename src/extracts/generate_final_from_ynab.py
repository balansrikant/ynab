import logging
import os
import pandas as pd
import copy

from pathlib import Path
from datetime import datetime

from utilities import setup_logging, write_df, get_balance, get_files_list, validate_category, get_extracts_path

os.environ['NUMEXPR_MAX_THREADS'] = '4'
os.environ['NUMEXPR_NUM_THREADS'] = '2'


def validate_monthly_amount_bank(df_in: pd.DataFrame, statement_date: str, df_balance: pd.DataFrame) -> bool:
    """Validate monthly bank df to check if reconcile with opening and closing balances

    Args:
        df_in: (Dataframe) monthly bank df
        statement_date: (str) statement date in yyyy-mm-dd format
        df_balance: (DataFrame) dataframe containing balance

    Returns:
        bool: status if df amounts reconcile
    """
    _opening_balance = df_balance.loc[df_balance["statement_date"] == statement_date, "opening_balance"].values[0]
    _closing_balance = df_balance.loc[df_balance["statement_date"] == statement_date, "closing_balance"].values[0]
    _expected_total = round(_closing_balance - _opening_balance, 2)
    _actual_total = round(df_in["amount"].sum(), 2)
    _logger.debug(f"...actual opening balance: {_opening_balance}")
    _logger.debug(f"...actual closing balance: {_closing_balance}")
    _logger.debug(f"...expected total: {_expected_total}, actual total: {_actual_total}")
    if _expected_total != _actual_total:
        return False
    else:
        return True


def validate_monthly_amount_cash(df_in: pd.DataFrame) -> bool:
    """Validate monthly cash df to check if reconcile with opening and closing balances

    Args:
        df_in: (Dataframe) monthly bank df

    Returns:
        bool: status if df amounts reconcile
    """
    _actual_total = round(df_in["amount"].sum(), 2)
    _logger.debug(f"...actual net amount: {_actual_total}")
    if _actual_total != 0:
        return False
    else:
        return True


def validate_full_amount_bank(df_in: pd.DataFrame, df_balance: pd.DataFrame) -> tuple:
    """Validate full bank df to check if reconcile with opening and closing balances

    Dataframe operations used:
    cumsum
    https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.cumsum.html?highlight=cumsum#pandas.DataFrame.cumsum

    groupby transform last
    https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.core.groupby.GroupBy.last.html

    Args:
        df_in: (Dataframe) monthly bank df
        df_balance: (Dataframe) balances df

    Returns:
        tuple: (bool, mismatch df) status if df amounts reconcile
    """
    _logger.info("...sort df, get max txn date")
    _df = copy.deepcopy(df_in)
    _df = _df.sort_values(by=["transaction_date"])
    _max_txn_date = _df.tail(1)["transaction_date"].values[0]

    _logger.info("...get available dates in balance file")
    _statement_dates = list(df_balance["statement_date"])

    _logger.info("...get subset of balance df")
    _max_date = _max_txn_date
    for _statement_date in _statement_dates:
        if _statement_date.date() < datetime.strptime(_max_txn_date, "%Y-%m-%d").date():
            continue
        else:
            _max_date = _statement_date
            break

    _df_balance = df_balance.loc[df_balance["statement_date"] <= _max_date, :].copy()
    _logger.info(f"...max date: {_max_date}")

    _logger.info("...calculate end of day balance\n")
    _df['balance'] = _df['amount'].cumsum()
    _df['eod_balance'] = _df.groupby('transaction_date')['balance'].transform('last')
    _df.loc[_df.groupby('transaction_date').tail(1).index, 'end_of_day'] = 'True'
    _df['end_of_day'] = _df['end_of_day'].fillna('False')

    _logger.info("locate additional end of day dates...")
    _df["transaction_date"] = pd.to_datetime(_df["transaction_date"]).copy()
    _df_add = pd.merge(_df_balance, _df, left_on=["statement_date"], right_on=["transaction_date"], how="left")
    _df_add = _df_add.loc[_df_add["account"].isna()]
    _df_add["transaction_date"] = _df_add["statement_date"]
    cols = ["account", "transaction_date", "payee", "master_category", "subcategory", "memo", "amount"]
    _df_add = _df_add[cols]

    _logger.info("...add additional end of day dates")
    _df = pd.concat([_df, _df_add], ignore_index=True).copy()
    _df.sort_values(by=['transaction_date'], inplace=True)
    _df.reset_index(drop=True, inplace=True)
    _df["eod_balance"].fillna(method="ffill", inplace=True)
    _df_balance["statement_date"] = pd.to_datetime(_df_balance["statement_date"])

    _logger.info("...reconcile end of transaction period balance")
    _df_eom = pd.merge(_df_balance, _df,
                       left_on=["statement_date"],
                       right_on=["transaction_date"],
                       how="inner")
    _df_mismatch = _df_eom.loc[round(_df_eom["closing_balance"], 2) != round(_df_eom["eod_balance"], 2)]
    if not _df_mismatch.empty:
        _df_combined = pd.merge(_df, _df_balance,
                                left_on=["transaction_date"],
                                right_on=["statement_date"],
                                how="left")
        cols = ["transaction_date", "payee", "amount", "balance", "eod_balance", "closing_balance"]
        _df_combined = _df_combined[cols]
        _df_combined["closing_balance"].fillna(0, inplace=True)
        _df_combined.loc[(_df_combined["closing_balance"] == 0),
                         (round(_df_combined["closing_balance"], 2) == round(_df_combined["eod_balance"], 2)),
                         "match"] = "True"
        _df_combined["match"].fillna("False", inplace=True)
        return False, _df_combined
    else:
        _df_balance_final = _df_balance.tail(1)
        _expected_final_balance = _df_balance_final["closing_balance"].values[0]
        _actual_balance = round(_df["amount"].sum(), 2)
        _logger.info(f"...expected final balance: {_expected_final_balance}, actual final balance: {_actual_balance}")
        return True, _df_mismatch


# noinspection PyTypeChecker
def clean_df_txn(df_param: pd.DataFrame, statement_date: str) -> list:
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
    file_txn_dc = {
        "statement_date": statement_date,
        "file_type": "hsbc_dc",
        "df": df_ynab
    }
    transaction_files.append(file_txn_dc)
    if not df_cash.empty:
        df_cash = df_cash[cols]
        file_txn_cash = {
            "statement_date": statement_date,
            "file_type": "cash",
            "df": df_cash
        }
        transaction_files.append(file_txn_cash)

    return transaction_files


def generate_sep_txn_files(extracts_path: Path):
    """Process ynab csv files, output to transaction csv files

    Args:
        extracts_path (str): working directory
    """
    _logger.info("**********************************")
    _logger.info("*** generating transaction files ***")
    _logger.info("**********************************")

    _logger.info("get ynab files...")
    _ynab_files_list = get_files_list(file_path=extracts_path, suffix=["ynab"])
    _logger.info(f"...{len(_ynab_files_list)} files found\n")
    if not _ynab_files_list:
        _logger.info(f"...nothing to process. terminating \n")
        exit()

    _logger.info(f"generating transaction dfs...")
    _txn_dfs = []
    for _file in _ynab_files_list:
        _logger.info(f"generating transaction dfs using ynab file: {_file.stem}...")
        _statement_date = str(_file.stem[0:10])
        _df_in = pd.read_csv(str(_file))
        _dfs = clean_df_txn(df_param=_df_in, statement_date=_statement_date)
        _logger.info(f"...transaction dfs generation complete")
        for _df in _dfs:
            _txn_dfs.append(_df)

    _logger.info(f"writing transaction dfs to disk...")
    for _txn_df in _txn_dfs:
        _statement_date = _txn_df["statement_date"]
        _file_type = _txn_df["file_type"]
        _filename = f"{_statement_date}_transaction_{_file_type}.csv"
        _path = str(Path.joinpath(extracts_path, _filename))
        _logger.info(f"...writing file {_filename}")
        write_df(df_in=_txn_df["df"], path=_path)
    _logger.info("...writing to disk complete\n")


def validate_sep_txn_files(extracts_path: Path):
    """Validate separate transaction files to check amounts reconcile with opening and closing balances

    Args:
        extracts_path: (Path) working directory
    """
    _logger.info("**********************************")
    _logger.info("*** running validation ***")
    _logger.info("**********************************")
    _logger.info("get separate transaction hsbc dc dfs...")
    _txn_files_list = get_files_list(file_path=extracts_path, suffix=["transaction_hsbc_dc"])

    _logger.info(f"...{len(_txn_files_list)} files found\n")
    if not _txn_files_list:
        _logger.info(f"...nothing to validate\n")
    else:
        _logger.info(f"validating hsbc dc files...")
        _df_balance = get_balance()
        for _file in _txn_files_list:
            _logger.info(f"validating {_file.stem}...")
            _df_in = pd.read_csv(str(_file), sep=",")
            _statement_date = str(_file.stem[0:10])
            _status_amount = validate_monthly_amount_bank(df_in=_df_in,
                                                          statement_date=_statement_date,
                                                          df_balance=_df_balance)

            _status_category, _result = validate_category(file_path=str(_file), file_type="txn")
            if _status_amount and _status_category:
                _logger.info(f"...file is valid\n")
            elif not _status_amount:
                _logger.error(f"...some amounts do not reconcile. please investigate {_file}")
                exit()
            else:
                _logger.error(f"...some category values are not populated, please check {_file}")

    _logger.info("get separate transaction cash dfs...")
    _txn_files_list = get_files_list(file_path=extracts_path, suffix=["transaction_cash"])
    _logger.info(f"...{len(_txn_files_list)} files found\n")
    if not _txn_files_list:
        _logger.info(f"...nothing to validate\n")
    else:
        _logger.info(f"validating cash files...")
        for _file in _txn_files_list:
            _logger.info(f"validating {_file.stem}...")
            _df_in = pd.read_csv(str(_file), sep=",")
            _status_amount = validate_monthly_amount_cash(df_in=_df_in)
            _status_category = validate_category(file_path=str(_file), file_type="txn")
            if _status_amount and _status_category:
                _logger.info(f"...file is valid\n")
            elif not _status_amount:
                _logger.error(f"...some amounts do not reconcile. please investigate {_file}")
                exit()
            else:
                _logger.error(f"...some category values are not populated, please check {_file}")


def validate_master_txn_file(extracts_path: Path):
    """Validate separate transaction files to check amounts reconcile with opening and closing balances

    Args:
        extracts_path: (Path) root directory
    """
    _logger.info("**********************************")
    _logger.info("*** validating master hsbc dc file ***")
    _logger.info("**********************************")

    _logger.info("get hsbc dc transaction df...")
    _path = Path.joinpath(extracts_path, "facts", "transaction_hsbc_dc.csv")
    _df = pd.read_csv(_path, sep=",")

    _logger.info("get dimensions df...")
    _df_balance = get_balance()

    _status_amount, _df_mismatch = validate_full_amount_bank(df_in=_df, df_balance=_df_balance)
    _status_category, _result = validate_category(file_path=_path, file_type="txn")

    if _status_amount and _status_category:
        _logger.info(f"...all balances match. file is valid\n")
    elif not _status_amount:
        _file_name = "transaction_error.csv"
        _file_path = Path.joinpath(extracts_path, _file_name)
        write_df(df_in=_df_mismatch, path=str(_file_path), sep=",")
        _logger.error(f"...some amounts do not reconcile. please investigate {_file_path}")
        exit()
    else:
        _logger.error(f"...some category values are not populated, please check {_path}")
        exit()
    return


def combine_txn_files(extracts_path: Path):
    """Combine transaction files into single file

    Args:
        extracts_path: (Path) extracts directory
    """
    _logger.info("**********************************")
    _logger.info("*** combine separate txn files with master ***")
    _logger.info("**********************************")

    _logger.info("get master transaction files...")
    _txn_file_types = ["transaction_hsbc_dc", "transaction_hsbc_cc", "transaction_cash"]
    _master_dfs = []
    for _file_type in _txn_file_types:
        _master_txn_file_path = Path.joinpath(extracts_path, "facts", f"{_file_type}.csv")
        _logger.info(f"...creating df from {_master_txn_file_path}")
        _df = pd.read_csv(_master_txn_file_path, sep=",")
        _entry = {"file_type": _file_type, "file_path": _master_txn_file_path, "df": _df, "sep_dfs": []}
        _master_dfs.append(_entry)
        _logger.info(f"...df created\n")

    _logger.info("get separate transaction files...")
    suffixes = ["transaction_hsbc_dc", "transaction_hsbc_cc", "transaction_cash"]
    _sep_txn_files = get_files_list(file_path=extracts_path, suffix=suffixes)
    _logger.info(f"...{len(_sep_txn_files)} files found\n")

    if not _sep_txn_files:
        _logger.info("...exiting\n")
        exit()

    _logger.info("associating separate txn files with master...")
    for _sep_txn_file in _sep_txn_files:
        _logger.info(f"finding master file for {_sep_txn_file}...")
        _sep_df = pd.read_csv(_sep_txn_file, sep=",")
        _statement_date = str(_sep_txn_file.stem[0:10])
        _file_type = str(_sep_txn_file.stem[11:])
        for _master_df in _master_dfs:
            if _master_df["file_type"] == _file_type:
                _entry = {"file_path": _sep_txn_file, "df": _sep_df}
                _master_df["sep_dfs"].append(_entry)
                _logger.info(f"...associated with master file {_master_df['file_path']}\n")
                break

    _logger.info("combine separate txn files with master...")
    _df_balance = get_balance()
    for _master_df in _master_dfs:
        _new_rows = 0
        if _master_df["sep_dfs"]:
            _logger.info(f"locating separate files for {_master_df['file_path']}...")
            _df = _master_df["df"]
            _df_combined = copy.deepcopy(_df)
            cols = _df_combined.columns
            for _sep_txn_file in _master_df["sep_dfs"]:
                _logger.info(f"...combining {_sep_txn_file['file_path']}")
                _sep_df = _sep_txn_file["df"]
                _df_new = pd.merge(_sep_df, _df,
                                   on=["transaction_date", "amount", "payee"],
                                   how="left",
                                   suffixes=(None, "_y"))
                _df_new = _df_new.loc[_df_new["master_category_y"].isna()]
                _logger.info(f"...additional rows found: {len(list(_df_new.index))}")
                if len(list(_df_new.index)) > 0:
                    _new_rows += len(list(_df_new.index))
                    _df_new = _df_new[cols]
                    _df_combined = pd.concat([_df_combined, _df_new], ignore_index=True)
                    if _master_df["file_type"] == "transaction_hsbc_dc":
                        _status_amount, _df_mismatch = validate_full_amount_bank(
                            df_in=_df_combined,
                            df_balance=_df_balance)
                        if _status_amount:
                            _logger.info(f"...amounts reconcile. combination valid")
                        else:
                            _file_name = "transaction_error.csv"
                            _file_path = Path.joinpath(extracts_path, _file_name)
                            write_df(df_in=_df_mismatch, path=str(_file_path), sep=",")
                            _logger.error(f"...some amounts do not reconcile. please investigate {_file_path}")
                            exit()
                    else:
                        _logger.info("...cash file. validation not needed\n")
                else:
                    _logger.info(f"...no rows to be added\n")
            if _new_rows > 0:
                _filename = f"{_master_df['file_type']}.csv"
                _path = str(Path.joinpath(extracts_path, _filename))
                _logger.info(f"...writing df to folder: {_path}\n")
                write_df(df_in=_df_combined, path=_path, sep=",")


def main():
    """Main entrypoint"""
    extracts_path = get_extracts_path()

    generate_sep_txn_files(extracts_path=extracts_path)
    validate_sep_txn_files(extracts_path=extracts_path)
    validate_master_txn_file(extracts_path=extracts_path)
    combine_txn_files(extracts_path=extracts_path)


_logger = logging.getLogger(__name__)
setup_logging()


if __name__ == '__main__':
    main()
