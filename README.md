## corona_db
Easy Access and Storage of Corona Cases in Germany

**Instructions:**
1. clone repo
2. move to local copy and execute query_corona_db.py:
- `python query_corona_db.py [target_file]`
- enter '' for DB-Name, User Name, Password, Host and Port (all of which are already correctly preset)
- enter specification on regional resolution, the period of time you need information on, sex and age of cases
  as well as whether you need total case numbers or increases per day 
  (possible options for each specification are shown in parenthesis)
- the script automatically queries the database server and writes results as CSV to your local target file

Needed packages: psycopg2, pandas, numpy, pathlib, wget, re, os, csv, datetime, argparse

Additionally, the scripts which are neccessary to build the same postgres-based database locally and 
download the needed data are included (instructions to be added)

You may query the Server directly, readonly credentials for direct queries: 
    
    DataBase:  'corona_db'
    User:      'guest'
    Password:  'pw'
    Host:      '193.196.54.54'
    Port:      '5432'
    
**Relational Schema:**

![rel_schema](/corona_db.png)


**Next steps:**
- Adding recovered cases
- English version of the query_corona_db API
- Adding additional Health Data for German Districts (e.g. hospital beds)

Corona cases are based on data of the Bundesamt für Kartographie und Geodäsie Robert Koch-Institut;

Population number based on Regionalstatistiken/GENESIS(dl-de-by-2.0 License)

zipcodes provided by zauberware: https://github.com/zauberware/postal-codes-json-xml-csv (CC-BY-4.0 License)

Server hosted on bwCloud SCOPE - Science, Operations and Education
