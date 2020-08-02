## corona_db
----

Easy Access and Storage of Corona Cases in Germany

Needed libraries: *psycopg2, wget*

----
**Instructions:**


To access the online data base
1. clone repo
2. navigate to local copy and execute queryDB.py from the command line:
```
    python queryDB.py [target_file] --guest
```
- `target_file` specifies the local file to store query
- With the `--guest` flag, script automatically queries the online-database server
- Enter specification on regional resolution, the period of time you need information on, sex and age of cases
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
- Blank/`''` inputs imply no restriction on the given specification (the input above returns cases for females and males combined)


You may query the Server directly, readonly credentials for direct queries: 
    
    DataBase:  'corona_db'
    User:      'guest'
    Password:  'pw'
    Host:      '193.196.54.54'
    Port:      '5432'

A script which demonstrates the usage of psycopg2 to directly query the server and the according SQL-commands is included in the examples folder (the example requires mathpyplot). 

-----
**Local Installation**

Additionally, all scripts neccessary for creating the postgres-database locally are included. To do so run `createDB.py (--logging)`.

Run `updateDB.py (--keep-covid) (--logging)` to download the newest data and add it to the database.  

Enter credentials according to your local postgreSQL setup for both routines. `DB-Name` defines the name of the local database (default: corona_db). `User Name` defines which user accesses the DB (default: postgres). `Password` sets the users password. `Host`(default: localhost) and `Port`(default: 5432) define the servers location and port.
`keep-covid` stops the script from downloading the newest case data but keep the local file. `logging` writes debug info for corresponding program to the logging-dir. 

    

-----
**Relational Schema:**

![rel_schema](/corona_db.png)


-----
This projects was developed based on some of the scripts I wrote for the WirVsVirus coronadb-project - many thanks to everybody involved that weekend.

Corona cases are based on data of the Bundesamt für Kartographie und Geodäsie Robert Koch-Institut; Open Data Datenlizenz Deutschland – Namensnennung – Version 2.0; Robert Koch-Institut (RKI), dl-de/by-2-0

Population number based on Regionalstatistiken/GENESIS(dl-de-by-2.0 License)

zipcodes provided by zauberware: https://github.com/zauberware/postal-codes-json-xml-csv (CC-BY-4.0 License)

Server hosted on bwCloud SCOPE - Science, Operations and Education
