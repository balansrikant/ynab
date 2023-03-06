# YNAB replicator
This solution and repository contains python objects for processing 
HSBC bank statements and prepare csv files for analysis on YNAB, PowerBI, 
and other software packages. 

## Key objectives
- Create a budget with spending limits for various categories
- Set aside money for a rainy day fund
- Set aside money for one off expenses
- Minimise impulsive spending

## Workflow

#### Pre-requisite steps -
- Install tabula from this path: https://tabula.technology/
- (optional) Install/get subscription for YNAB: https://www.youneedabudget.com/
- In the below steps, replace <root_path> with actual data folder e.e. D:/BankStatements
- Create folder path like so
    - \<root_path>\
    - \<root_path>\unprocessed\
    - \<root_path>\unprocessed\1-tabula-output\
    - \<root_path>\unprocessed\2-processed\
    - \<root_path>\unprocessed\3-ynab\
    - \<root_path>\unprocessed\4-transaction\
- For each calendar year, create folder path like so:
    - \<root_path>\<year>\1-tabula-output\
    - \<root_path>\<year>\2-processed\
    - \<root_path>\<year>\3-ynab\
    - \<root_path>\<year>\4-transaction\

#### Steps - 1, 2: Generate Tabula files
- Download pdf statement and put in \<root_path>\<year>\0-pdf\
- Open tabula, and generate csv, paste in \<root_path>\<year>\1-tabula-output\
- Open file from previous step, and clean up as follows
  - no header needed
  - 6 columns: date, transaction type, payee, outflow, inflow, balance
  - some columns may have shifted... adjust them
  - sometimes strange characters in first row etc... remove them
  - save file
  - copy to unprocessed folder
- Open \<root_path>\Balances.csv
  - Confirm date column is yyyy-mm-dd
  - Add new row for current month
  - Populate opening, closing balance from pdf

#### Step 3: Generate YNAB files

## Technical setup

#### How to setup PyCharm
1. Open Settings > Tools > Terminal
2. Change Shell path from "powershell.exe" to "cmd.exe"

#### How to setup Flask
- Get secret key from command line 
(https://stackoverflow.com/questions/34902378/where-do-i-get-secret-key-for-flask)  
**import os**  
**os.urandom(12)**

#### Additional files to be created:
- \ynab\dropbox_api\creds.json
  {
    "app_key": "",
    "app_secret": "",
    "access_code": ""
  }

- \ynab\dropbox_api\token.json
  {
    "refresh_token": "",
    "access_token": ""
  }

#### Conda commands used
  - create environment
    - conda create --name "env name" python=3.8
  - export packages installed 
    - conda env export --name ynab > environment.yml