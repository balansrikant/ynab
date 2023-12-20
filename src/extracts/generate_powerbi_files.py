import copy
import logging
import os
import pandas as pd
import calendar
import csv

from pathlib import Path

from utilities import setup_logging, write_df, get_files_list
from utilities import get_category, get_extracts_path

os.environ['NUMEXPR_MAX_THREADS'] = '4'
os.environ['NUMEXPR_NUM_THREADS'] = '2'


def generate_txn_file() -> (int, int):
    """Generate transaction fact file by combining transaction files

    Returns:
        (int, int): min year, max year
    """

    extracts_path = get_extracts_path()
    facts_dir = Path.joinpath(extracts_path, "facts")
    powerbi_dir = Path.joinpath(extracts_path, "powerbi")

    _logger.info("**********************************")
    _logger.info("*** combine separate txn files ***")
    _logger.info("**********************************")
    _logger.info("get transaction files...")

    _files = get_files_list(file_path=Path(facts_dir), starts_with="transaction")
    _transaction_files = []
    columns = ["account", "transaction_date", "payee", "master_category", "subcategory", "memo", "amount"]
    _df_combined = pd.DataFrame(columns=columns)
    for _file in _files:
        _logger.info(f"file {_file.stem} added to combined file...")
        _transaction_files.append(_file)
        _df_new = pd.read_csv(str(_file))
        _df_combined = pd.concat([_df_combined, _df_new], ignore_index=True)
    _df_combined['transaction_date'] = pd.to_datetime(_df_combined['transaction_date'])
    _df_combined['memo'].fillna('', inplace=True)
    _df_combined['amount'] = _df_combined['amount'].astype(float)
    _df_combined['master_category'] = _df_combined['master_category'].astype(str)
    _df_combined['subcategory'] = _df_combined['subcategory'].astype(str)

    _df_combined = _df_combined.loc[_df_combined['amount'] != 0]
    _df_combined["category"] = _df_combined["master_category"] + ": " + _df_combined['subcategory']
    _df_combined["year_month"] = _df_combined["transaction_date"].dt.year * 100 \
        + _df_combined["transaction_date"].dt.month
    _df_combined["category_year_month"] = _df_combined["category"] + "-" + _df_combined["year_month"].astype(str)

    _filename = "transaction.csv"
    _path = str(Path.joinpath(powerbi_dir, _filename))
    _logger.info(f"...writing file {_path}")
    write_df(df_in=_df_combined, path=_path)

    min_year = int(str(min(_df_combined["year_month"]))[:4])
    max_year = int(str(max(_df_combined["year_month"]))[:4])
    _logger.debug(f"min year: {min_year}, max year: {max_year}")
    return min_year, max_year


def generate_txn_monthly_total_file():
    """Generate transaction monthly totals by category"""

    extracts_path = get_extracts_path()
    powerbi_dir = Path.joinpath(extracts_path, "powerbi")

    _filepath = Path.joinpath(powerbi_dir, "transaction.csv")
    _df_combined = pd.read_csv(str(_filepath))

    _df_group = pd.DataFrame(_df_combined.groupby(['category', 'year_month', 'category_year_month'])['amount'].sum())
    _df_group.reset_index(inplace=True)
    _filename = "transaction_monthly_total.csv"
    _path = str(Path.joinpath(powerbi_dir, _filename))
    _logger.info(f"...writing file {_path}")
    write_df(df_in=_df_group, path=_path)
    return _df_group


def generate_txn_overall_total_file():
    """Generate transaction overall totals by category"""

    extracts_path = get_extracts_path()
    powerbi_dir = Path.joinpath(extracts_path, "powerbi")

    _filepath = Path.joinpath(powerbi_dir, "transaction.csv")
    _df_combined = pd.read_csv(str(_filepath))

    _df_group = pd.DataFrame(_df_combined.groupby(['category'])['amount'].sum())
    _df_group.reset_index(inplace=True)
    _filename = "transaction_overall_total.csv"
    _path = str(Path.joinpath(powerbi_dir, _filename))
    _logger.info(f"...writing file {_path}")
    write_df(df_in=_df_group, path=_path)


def generate_date_file(min_year: int, max_year: int):
    """Generate date dimension file

    Args:
        min_year: min year of transactions
        max_year: min year of transactions
    """
    powerbi_dir = Path.joinpath(get_extracts_path(), "powerbi")
    current_year = min_year
    _rows = []
    while current_year <= max_year:
        for month_num in range(1, 13, 1):
            # _logger.info(calendar.month_name[month_num])
            _row = {
                "date_key": current_year * 10000 + (month_num * 100) + 1,
                "date_value": str(current_year) + "-" + ("0" + str(month_num))[-2:] + "-01",
                "date_year": current_year,
                "month_name": calendar.month_name[month_num][:3],
                "year_month_str": str(current_year) + "-" + calendar.month_name[month_num][:3],
                "year_month": current_year * 100 + month_num
            }
            _rows.append(_row)

        current_year += 1
    _df_date = pd.DataFrame(_rows)
    _df_date['date_key'] = _df_date['date_key'].astype(int)
    _df_date['date_year'] = _df_date['date_year'].astype(int)
    _df_date['year_month'] = _df_date['year_month'].astype(int)
    _df_date['date_value'] = pd.to_datetime(_df_date['date_value'])

    _filename = "dim_date.csv"
    _path = str(Path.joinpath(powerbi_dir, _filename))
    _logger.info(f"...writing file {_path}")
    write_df(df_in=_df_date, path=_path)


def generate_budget_values(min_year: int, max_year: int):
    """Generate master file

    Args:
        min_year: min year of transactions
        max_year: max year of transactions
    """

    extracts_path = get_extracts_path()
    dimensions_dir = Path.joinpath(extracts_path, "dimensions")
    powerbi_dir = Path.joinpath(extracts_path, "powerbi")

    # collect budget values
    _budget_file_path = Path.joinpath(dimensions_dir, "budget.csv")
    _budget_file = open(str(_budget_file_path), 'r')
    reader = csv.DictReader(_budget_file)
    _budget_values = []
    for dictionary in reader:
        if float(dictionary.get("amount")) != 0:
            start_year_month = int(dictionary.get("start"))
            end_year_month = int(dictionary.get("end"))
            if end_year_month == 999912:
                end_year_month = int(str(max_year) + "12")

            if start_year_month == 190001:
                start_year_month = int(str(min_year) + "01")

            current_year_month = start_year_month
            while current_year_month <= end_year_month:
                category_year_month = dictionary.get("category") + "-" + str(current_year_month)
                budget_amount = float(dictionary.get("amount"))
                new_row = {
                    "category": dictionary.get("category"),
                    "year_month": current_year_month,
                    "category_year_month": category_year_month,
                    "budget_amount": budget_amount
                }
                _budget_values.append(new_row)

                if str(current_year_month)[-2:] == "12":
                    current_year_month = (int(str(current_year_month)[:4]) + 1) * 100 + 1
                else:
                    current_year_month += 1
    _df_budget = pd.DataFrame(_budget_values)

    _filename = "budget_values.csv"
    _path = str(Path.joinpath(powerbi_dir, _filename))
    _logger.info(f"...writing file {_path}")
    write_df(df_in=_df_budget, path=_path)


def generate_combined_budget():
    """Generate combined budget"""

    powerbi_dir = Path.joinpath(get_extracts_path(), "powerbi")

    # collect budget values
    _budget_file_path = Path.joinpath(powerbi_dir, "budget_values.csv")
    _df_budget = pd.read_csv(str(_budget_file_path), sep=",")

    # collect actual transaction amounts
    _txn_monthly_total_file_path = Path.joinpath(powerbi_dir, "transaction_monthly_total.csv")
    _df_txn_monthly_total = pd.read_csv(str(_txn_monthly_total_file_path), sep=",")
    _df_txn_monthly_total.loc[_df_txn_monthly_total["amount"] < 0, "actual_amount"] = \
        _df_txn_monthly_total["amount"] * -1
    _df_txn_monthly_total = _df_txn_monthly_total.loc[_df_txn_monthly_total["actual_amount"].notnull(), :]
    columns = ["category_year_month", "actual_amount"]
    _df_txn_monthly_total = _df_txn_monthly_total[columns]

    # update budget with actual values
    _df_budget = pd.merge(_df_budget, _df_txn_monthly_total,
                          how="left", on=["category_year_month"], suffixes=("", "_x"))

    # fill blanks
    _df_budget['budget_amount'].fillna(0, inplace=True)
    columns = ["category", "year_month", "category_year_month", "budget_amount"]
    _df_budget = _df_budget[columns]

    # write df
    _filename = "budget_combined.csv"
    _path = str(Path.joinpath(powerbi_dir, _filename))
    _logger.info(f"...writing file {_path}")
    write_df(df_in=_df_budget, path=_path)


def generate_master_file():
    """Generate master file"""

    powerbi_dir = Path.joinpath(get_extracts_path(), "powerbi")

    # collect budget values
    _budget_file_path = Path.joinpath(powerbi_dir, "budget_combined.csv")
    _df_budget = pd.read_csv(str(_budget_file_path), sep=",")

    # collect actual transaction amounts
    _txn_monthly_total_file_path = Path.joinpath(powerbi_dir, "transaction_monthly_total.csv")
    _df_txn_monthly_total = pd.read_csv(str(_txn_monthly_total_file_path), sep=",")
    _df_txn_monthly_total.loc[_df_txn_monthly_total["amount"] < 0, "actual_amount"] = \
        _df_txn_monthly_total["amount"] * -1
    _df_txn_monthly_total = _df_txn_monthly_total.loc[_df_txn_monthly_total["actual_amount"].notnull(), :]
    columns = ["category_year_month", "actual_amount"]
    _df_txn_monthly_total = _df_txn_monthly_total[columns]

    # construct master dataframe
    _df_category = get_category()
    columns = ["master_category", "subcategory", "category"]
    _df_category = _df_category[columns]

    columns = ["master_category", "subcategory", "category", "budget", "category_date", "year_month",
               "category_year_month"]
    _df_master = pd.DataFrame(columns=columns)
    _date_file_path = Path.joinpath(powerbi_dir, "dim_date.csv")
    _date_file = open(str(_date_file_path), 'r')
    reader = csv.DictReader(_date_file)
    _date_list = list()
    for dictionary in reader:
        _df_row = copy.deepcopy(_df_category)
        _df_row["category_date"] = dictionary.get("date_value")
        _df_row["year_month"] = dictionary.get("year_month")
        _df_row["category_year_month"] = _df_row["category"] + "-" + str(dictionary.get("year_month"))
        _df_row["budget"] = 0
        _df_master = pd.concat([_df_master, _df_row], ignore_index=True)

    # update with budget values
    _df_master = pd.merge(_df_master, _df_budget, how="left", on=["category_year_month"], suffixes=("", "_x"))

    def _get_amount(amount):
        if amount:
            return amount
        else:
            return 0
    _df_master['budget'] = _df_master['budget_amount'].apply(_get_amount)
    _df_master['budget'].fillna(0, inplace=True)
    columns = ["master_category", "subcategory", "category", "budget", "category_date", "year_month",
               "category_year_month"]
    _df_master = _df_master[columns]

    # combine with actual file
    _df_master = pd.merge(_df_master, _df_txn_monthly_total,
                          how="left", on=["category_year_month"], suffixes=("", "_y"))

    def _get_amount(amount):
        if amount:
            return amount
        else:
            return 0

    _df_master.loc[_df_master['budget'] == -1, "budget"] = _df_master['actual_amount'].apply(_get_amount)
    _df_master.loc[:, "actual"] = _df_master['actual_amount'].apply(_get_amount)

    _df_master['budget'].fillna(0, inplace=True)
    _df_master['actual'].fillna(0, inplace=True)

    columns = ["master_category", "subcategory", "category", "budget", "actual", "category_date", "year_month",
               "category_year_month"]
    _df_master = _df_master[columns]

    # generate running total
    _df_master["budget_running_total"] = _df_master.groupby(["category"])["budget"].cumsum()
    _df_master["actual_running_total"] = _df_master.groupby(["category"])["actual"].cumsum()
    _df_master["balance"] = _df_master["budget_running_total"] - _df_master["actual_running_total"]

    _filename = "master.csv"
    _path = str(Path.joinpath(powerbi_dir, _filename))
    _logger.info(f"...writing file {_path}")
    write_df(df_in=_df_master, path=_path)


def main():
    """Main entrypoint"""

    min_year, max_year = generate_txn_file()
    max_year = 2024
    generate_txn_monthly_total_file()
    generate_date_file(min_year=min_year, max_year=max_year)
    generate_budget_values(min_year=min_year, max_year=max_year)
    generate_combined_budget()
    generate_master_file()


_logger = logging.getLogger(__name__)
setup_logging()


if __name__ == '__main__':
    main()
