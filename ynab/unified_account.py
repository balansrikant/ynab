import pandas as pd
import os
import logging

from pathlib import Path

from utilities import DATA_FILES
from dropbox_api import DropboxLocal

_logger = logging.getLogger(__name__)


class UnifiedAccount:
    def __init__(self, data_dir: str) -> None:
        self._local_cache = False
        self.files_dict = dict()
        self._populate_file_path(data_dir)
        self.download_files(self.files_dict)
        self._local_cache = True

    def _populate_file_paths(self, data_dir):
        for file in DATA_FILES:
            filename = file + ".csv"
            local_path = str(Path.joinpath(data_dir, filename))
            server_path = "/" + filename
            self.files_dict[filename] = {
                "local_path": local_path,
                "server_path": server_path
                }

    def download_files(self, files_dict):
        dbx = DropboxLocal()
        _ = dbx.get_file_metadata()
        dbx.download_files(files_dict)

    def get_account(self):
        filepath = self.files_dict["account"]["local_path"]
        self.account = pd.read_csv(filepath, sep='|')
        return self.account

    def get_category(self):
        filepath = self.files_dict["category"]["local_path"]
        self.category = pd.read_csv(filepath, sep='|')
        return self.category

    def populate_facts(self):
        self.df_transaction = pd.DataFrame()
        self.df_transaction.concat()

    @classmethod
    def read_csv_into_df(cls, filename: str, sep: str) -> pd.DataFrame:
        df_in = pd.read_csv(filename, sep=sep)

        return df_in

    # noinspection PyShadowingNames
    def read_dimensions(self):
        data_folder = os.path.join(os.path.dirname(__file__), 'data\\')

        # dimensions
        df_balance = self.read_csv_into_df(data_folder, 'balance.csv', ',')
        df_balance['statement_date'] = pd.to_datetime(
            df_balance['statement_date'])
        df_category = self.read_csv_into_df(data_folder, 'category.csv', '|')
        df_year_month = self.read_csv_into_df(
            data_folder, 'year_month.csv', ',')

        return df_balance, df_category, df_year_month

    # noinspection PyShadowingNames
    def read_facts(self):
        data_folder = os.path.join(os.path.dirname(__file__), 'data\\')

        # facts
        df_transaction = self.read_csv_into_df(
            data_folder, 'transaction\\transaction_hsbc_dc.csv', '|')
        df_transaction = self.read_csv_into_df(
            data_folder, 'transaction\\transaction_hsbc_cc.csv', '|')
        df_transaction['transaction_date'] = pd.to_datetime(
            df_transaction['transaction_date'])

        transactions = {}
        accounts = df_transaction['account'].unique().tolist()
        for account in accounts:
            df = df_transaction[df_transaction['account'] == account]
            df = df.sort_values(by=['account', 'transaction_date'])
            df.reset_index(drop=True, inplace=True)
            df['balance'] = df['amount'].cumsum()
            df['eod_balance'] = df.groupby(
                'transaction_date')['balance'].transform('last')
            df.loc[df.groupby(
                'transaction_date').tail(1).index, 'end_of_day'] = 'True'
            df['end_of_day'] = df['end_of_day'].fillna('False')
            df.loc[df['end_of_day'] == 'False', 'eod_balance'] = 0
            transactions[account] = df

        df_budget = self.read_csv_into_df(
            data_folder, 'budget\\budget_combined.csv', '|')

        return accounts, transactions, df_budget

    def reconcile_balance(
        df_trn: pd.DataFrame,
        df_bal: pd.DataFrame
    ) -> pd.DataFrame:
        df_merged = pd.merge(
            df_trn,
            df_bal, left_on=['transaction_date'], right_on=["statement_date"])
        df_merged = df_merged[df_merged['end_of_day'] == 'True']
        cols = [
            'transaction_date', 'payee', 'amount', 'balance', 'closing_balance'
            ]
        df_merged = df_merged[cols]
        return df_merged
