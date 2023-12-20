## Workflow

#### Pre-requisite steps -
- Install tabula from this path: https://tabula.technology/
- (optional) Install/get subscription for YNAB: https://www.youneedabudget.com/
- In the below steps, replace <root_path> with actual data folder 
  - Default path: D:/MyDocuments/Bank-Statements/HSBC
- Create folder path like so
  - \<root_path>\
  - \<root_path>\working\
- For each calendar year, create folder path like so:
  - \<root_path>\<year>\1-tabula-output\
  - \<root_path>\<year>\2-processed\
  - \<root_path>\<year>\3-ynab\
  - \<root_path>\<year>\4-transaction\

#### Steps
Download pdf statement and put in \<root_path>\<year>\0-pdf\

##### File explorer
1. Open file explorer, open tabula from tabula folder  
  e.g. ```D:\Program Files (Utilities)\tabula```
2. Generate statement csv as "yyyy-mm-dd_Statement.csv", paste in \<root_path>\<year>\1-tabula-output\
3. Open file from previous step, and clean up as follows
    - no header needed
    - 6 columns: date, transaction type, payee, outflow, inflow, balance
    - some columns may have shifted... adjust them
    - sometimes strange characters in first row etc... remove them
    - save file
4. Copy to \<root_path>\working\ folder  
  e.g. ```D:\MyDocuments\Bank-Statements\HSBC\working```
5. Open \<root_path>\extracts\balance.csv  
  e.g. ```D:\MyDocuments\Bank-Statements\HSBC\extracts```
    - Confirm date column is yyyy-mm-dd
    - Add new row for current month
    - Populate opening, closing balance from pdf

##### Command prompt
1. Open command prompt, change directory to extracts folder  
  ```cd "D:\MyDevelopment\Projects\ynab\ynab\extracts\"```
2. Activate environment  
  ```conda activate ynab```
3. Execute below command to generate ynab files  
  ```python -m extract_generator --action ynab```
4. Populate categories  
  use ```"D:\MyDevelopment\Projects\ynab\ynab\extracts\category_short.csv"```
5. Execute below command to generate transaction files  
  ```python -m extract_generator --action transaction```
6. Execute below command to combine transaction files  
  ```python -m extract_generator --action combine```
7. Copy files to respective locations
8. Open PowerBI for analysis (optional)  
  ```D:\MyDevelopment\Projects\ynab\ynab\extracts\power_bi```
## Technical setup

#### How to set up PyCharm
1. Open Settings > Tools > Terminal
2. Change Shell path from "powershell.exe" to "cmd.exe"

#### How to set up Flask
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