from pathlib import Path

import pandas as pd


def main():
    ROOT_DIR = Path().absolute().parent
    # print(ROOT_DIR)
    path = Path.joinpath(ROOT_DIR, 'ynab', 'data_copy', 'transaction_hsbc_dc.csv')
    df = pd.read_csv(path, sep='|')
    df['sub'] = df['master_category'] + '|' + df['subcategory']
    print(df['sub'].unique())



if __name__ == '__main__':
    main()