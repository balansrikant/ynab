# Application to replicate app 'You Need A Budget (YNAB)' 

## Key features
- Create a budget with spending limits for various categories
- Set aside money for a rainy day fund
- Set aside money for one off expenses
- Minimise impulsive spending


## How to setup PyCharm
1. Open Settings > Tools > Terminal
2. Change Shell path from "powershell.exe" to "cmd.exe"

## How to setup Flask
- Get secret key from command line 
(https://stackoverflow.com/questions/34902378/where-do-i-get-secret-key-for-flask)  
**import os**  
**os.urandom(12)**

## Conda commands used
  - create environment
    - conda create --name "env name" python=3.8
  - export packages installed 
    - conda env export --name ynab > environment.yml
