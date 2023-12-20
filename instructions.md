## Workflow

#### Pre-requisites -
- (optional) Install/get subscription for YNAB: https://www.youneedabudget.com/
- Create folder path like so
  - \ynab-files\
  - \ynab-files\extracts\  

**Note: all folders below are in relation to \ynab-files folder**

#### Steps -
- Open \extracts directory, check last date from 'last_date.txt'
- Login to HSBC
  - search for transactions > last date 
  - export as csv file, in \extracts directory
  - format yyyy-mm-dd_txn.csv
- Update last_date.txt
- Open balance.csv, update balance, save date column as yyyy-mm-dd
- Open cmd folder / pycharm terminal
- navigate to \ynab\src\extracts\
- ```conda activate ynab```, if operating from cmd
- ```python generate_ynab_from_txn.py```
- ```python validate_category.py -f yyyy-mm-dd_ynab.csv -t ynab```
- open ynab file (yyyy-mm-dd_ynab.csv) in excel
  - review filled-in categories
  - fill in categories
	- use \extracts\dimensions\category.csv for categories
	- fill in memo separated by ; where appropriate
- ```python validate_category.py -f yyyy-mm-dd_ynab.csv -t ynab```
- ```python generate_final_from_ynab.py```
- rename existing transaction_hsbc_dc.csv in the facts folder with suffix '_bak_yyyy-mm-dd' and move to \extracts\archive folder
- move other temp files to archive folder
- move new transaction_hsbc_dc.csv to the facts folder
- ```python generate_powerbi_files.py```
- ```conda deactivate```
