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
    pass
