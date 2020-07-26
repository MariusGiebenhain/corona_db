## corona_db
**Currently not Working!**
----

Easy Access and Storage of Corona Cases in Germany

**Instructions:**
1. clone repo
2. move to local copy and execute query_corona_db.py:
```
    python query_corona_db.py [target_file]
    
    DB-Name:
    User Name:
    Password:
    Host:
    Port:
```
- Leave DB-Name, User Name, Password, Host and Port blank for Windows or type in `''` for ubuntu (all of them are already preset correctly)
- enter specification on regional resolution, the period of time you need information on, sex and age of cases
  as well as whether you need total case numbers or increases per day (possible options for each specification are shown in parenthesis):
```
    Regionale Auflösung (Bund, Land, Kreis): Land
    Meldedatum vom (JJJJ-MM-TT): 2020-04-01
    Meldedatum bis (JJJJ-MM-TT): 2020-04-07
    älter als (4/14/34/59/79): 14
    jünger als (5/15/35/60/80): 80
    Geschlecht(w/m)
    Gebe Erhöhungen statt absoluter Zahl zurück (W):
```
- Blank/`''` inputs imply no restriction on the given specification (our example would return cases for females and males)
- The script automatically queries the database server and writes results as CSV to your local `target_file`


Needed libraries: *psycopg2, pandas, numpy, wget*

Utilized Standard libraries: *re, datetime, argparse, pathlib, csv*

Additionally, the scripts which are neccessary to build the same postgres-based database locally and 
download the needed data are included. To do so run `update_corona_cases.py` and `add_population.py`. Enter credentials according to your local postgreSQL setup (`DB-Name` defines the name of the local database - default: corona_db). Run `update_corona_cases.py` to download newest data + add it to the database.  

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


This projects was developed based on some of the scripts I wrote for the WirVsVirus coronadb-project - many thanks to everybody involved that weekend.

Corona cases are based on data of the Bundesamt für Kartographie und Geodäsie Robert Koch-Institut; Open Data Datenlizenz Deutschland – Namensnennung – Version 2.0; Robert Koch-Institut (RKI), dl-de/by-2-0

Population number based on Regionalstatistiken/GENESIS(dl-de-by-2.0 License)

zipcodes provided by zauberware: https://github.com/zauberware/postal-codes-json-xml-csv (CC-BY-4.0 License)

Server hosted on bwCloud SCOPE - Science, Operations and Education
