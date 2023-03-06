import os
import csv


def read_csv_into_list(root_folder: str, path: str, sep: str) -> list:
    filename = os.path.join(root_folder, path)
    with open(filename, 'r') as f:
        reader = csv.DictReader(f, delimiter=sep)
        a = list(reader)
    return a


def helper():
    # unified_account = UnifiedAccount()
    # pd.set_option('display.max_rows', None)
    # df_balance, df_category, df_year_month = read_dimensions()
    # accounts, transactions, df_budget = unified_account.read_facts()
    # df = reconcile_balance(transactions['HSBC DC'], df_balance)
    # transactions['HSBC DC'].to_csv(os.path.join(os.path.dirname(__file__),
    #   'data\\file_name.csv'), sep='|')  # Use Tab to seperate data
    # df_hsbc = transactions['HSBC DC']
    # df_cash = df_hsbc.loc[(df_hsbc['master_category'] == 'Transfer : Out')
    #   & (df_hsbc['subcategory'] == 'Cash'), :]
    # df_cash['account'] = 'Cash'
    # df_cash['payee'] = 'Transfer : HSBC DC'
    # df_cash['master_category'] = 'Transfer : In'
    # df_cash['subcategory'] = 'HSBC DC'
    # df_cash['memo'] = 'In'
    # df_cash['amount'] = df_cash['amount'] * -1
    # cols = ['account', 'transaction_date', 'payee', 'master_category',
    #   'subcategory', 'memo', 'amount']
    # df_cash = df_cash[cols]
    # df_cash.reset_index(drop=True, inplace=True)
    # df_cash.to_csv(os.path.join(os.path.dirname(__file__),
    #   'data\\transaction\\transactions_cash1.csv'), sep='|', index=False)
    # print(df_cash)
    # print(df)
    # df_cc = transactions['HSBC CC']
    # print(round(df_cc['amount'].sum(), 2))
    # trn_file = Path.joinpath(DATA_DIR, 'transaction_hsbc_dc.csv')
    # df = pd.read_csv(trn_file, sep='|')

    # df = df[cols]
    # df.to_csv(Path.joinpath(DATA_DIR,
    #   'transaction_hsbc_dc.csv'), sep='|', index=False)
    # print(df.loc[(df['master_category'] == 'Transfer : Out')
    #   & (df['subcategory'] == 'ISA')])
    # cash_file = Path.joinpath(DATA_DIR, 'transaction',
    #   'transactions_cash1.csv')
    # df = pd.read_csv(cash_file, sep='|')
    # df2 = copy.deepcopy(df)
    # df2['amount'] = df2['amount'] * -1
    # df2['payee'] = 'Misc'
    # df2['master_category'] = 'Long term'
    # df2['subcategory'] = 'Misc Purchases'
    # df2['memo'] = ''
    # df = pd.concat([df, df2])
    # df.sort_values(by=['transaction_date'], inplace=True)
    # print(df.head())
    # print(df['amount'].sum())
    # df.to_csv(Path.joinpath(DATA_DIR,
    #   'transaction_cash.csv'), sep='|', index=False)
    # pd.set_option('display.max_rows', 5)

    # for account in accounts:
    #     print(account)
    #     print(transactions[account].head())
    #     print('')

    # file_name = os.path.basename(original_full_path)
    # cleaned_df_filename = os.path.splitext(file_name)[0] + '_cleaned.csv'
    # cleaned_df_full_path = Path.joinpath(cleaned_dir, cleaned_df_filename)
    # file['cleaned_full_path'] = cleaned_df_full_path
    # df.to_csv(cleaned_df_full_path, index=False)
    # _logger.info(f"...cleaning up complete: {file['pathlib_path']}")
    pass

def get_processed_dfs_old(cleaned_df_list: list) -> list:
    """Load balances from csv into dataframe.

    Args:
        cleaned_df_list (list): list of dicts containing statement date, pathlib path, cleaned df

    Returns:
        list: list of dataframes containing processed dataframe along with metadata
    """
    def _process_ynab(df_param):
        df_ynab = copy.deepcopy(df_param)

        # rename columns
        df_ynab = df_ynab.rename(columns={'date': 'Date', 'payee': 'Payee'})
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

    _processed_df_list = []
    for file in cleaned_df_list:
        _logger.info(f"processing file: {file['pathlib_path']}")
        df_in = file["clean_df"]
        _df = _process_ynab(df_in)
        # file_name = os.path.basename(processed_full_path)
        # ynab_df_filename = os.path.splitext(file_name)[0] + '_ynab.csv'
        # ynab_df_full_path = Path.joinpath(ynab_dir, ynab_df_filename)
        # df.to_csv(ynab_df_full_path, index=False)
        _processed_df_list.append({'statement_date': file['statement_date'],
                                   'pathlib_path': file['pathlib_path'],
                                   'clean_df': df_in,
                                   'processed_df': _df
                                   }
                                  )

        # file_name = os.path.basename(original_full_path)
        # cleaned_df_filename = os.path.splitext(file_name)[0] + '_cleaned.csv'
        # cleaned_df_full_path = Path.joinpath(cleaned_dir, cleaned_df_filename)
        # file['cleaned_full_path'] = cleaned_df_full_path
        # df.to_csv(cleaned_df_full_path, index=False)
        _logger.info(f"...processing complete: {file['pathlib_path']}")
    return _processed_df_list



