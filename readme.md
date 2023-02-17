How to setup PyCharm
1. Open Settings > Tools > Terminal
2. Change Shell path from "powershell.exe" to "cmd.exe"

How to setup Flask
1. Get secret key from command line 
(https://stackoverflow.com/questions/34902378/where-do-i-get-secret-key-for-flask)  
**import os**  
**os.urandom(12)**

Additional files to be created:
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

Conda commands used
  - create environment
    - conda create --name "env name" python=3.8
  - export packages installed 
    - conda env export --name ynab > environment.yml