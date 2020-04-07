# corona_db
Easy Access and storage of German Corona Cases via query_corona_db.py (Cases are based on RKI reports)
Instruction for usage:
clone repo
move to local copy and call
> python query_corona_db.py [target_file]
> enter '' for DB-Name, User Name, Password, Host and Port (all of which are already correctly preset)
> enter specification on regional resolution, the period of time you need information on, sex and age of cases
  as well as whether you need total case numbers or increases per day 
  (possible options for each specification are shown in parenthesis)
> the script automatically queries my database server and writes results as CSV to your local target file

Needed packages: psycopg2, pandas, numpy, pathlib, wget, re, os, csv, datetime, argparse

Additionally, the scripts which are neccessary to build the same postgres-based database locally and 
download the needed data are included (instructions to be added)

