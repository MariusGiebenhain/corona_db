import psycopg2
import numpy as np
import os
import wget
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)
from pathlib import Path
from pandas import read_csv


def get_credentials():
    dbname = input('DB-Name: ')
    username = input('User Name: ')
    password = input('Password: ')
    host = input('Host: ')
    port = input('Port: ')
    if len(dbname) == 0: dbname = 'corona_db'
    if len(username) == 0: username = 'postgres'
    if len(host) == 0: host = 'localhost'
    if len(port) == 0: port = '5432'
    return {'dbname' : dbname, 'username' : username, 'password' : password, 'host': host, 'port' : port}


def create_corona_db(cred):
    con = psycopg2.connect(
        dbname = 'postgres',
        user = cred['username'], 
        password = cred['password'],
        host = cred['host'],
        port = cred['port'])
    con.autocommit = True
    cur = con.cursor()
    cur.execute('CREATE DATABASE {};'.format('corona_db'))
    con.commit()
    con.close()


def tables_corona_db(con):
    sql_land = 'CREATE TABLE land(\
        land CHAR(2),\
        name VARCHAR(255),\
        PRIMARY KEY(land)\
        );'
    sql_kreis = 'CREATE TABLE kreis(\
        krs INT,\
        land CHAR(2),\
        name VARCHAR(255),\
        PRIMARY KEY(krs),\
        FOREIGN KEY(land) REFERENCES land(land)\
        );'
    sql_gebiet = 'CREATE TABLE gebiet(\
        plz INT,\
        PRIMARY KEY(plz)\
        );'
    sql_meldung = 'CREATE TABLE meldung(\
        ref Int,\
        krs INT,\
        alter_von INT,\
        alter_bis INT,\
        geschlecht CHAR(1),\
        datum DATE,\
        PRIMARY KEY(ref),\
        FOREIGN KEY(krs) REFERENCES kreis(krs) ON DELETE CASCADE\
        );'
    sql_fall = 'CREATE TABLE fall(\
        ref INT,\
        anzahl INT,\
        PRIMARY KEY(ref),\
        FOREIGN KEY(ref) REFERENCES meldung(ref) ON DELETE CASCADE\
        );'
    sql_todesfall = 'CREATE TABLE todesfall(\
        ref INT,\
        anzahl INT,\
        PRIMARY KEY(ref),\
        FOREIGN KEY(ref) REFERENCES meldung(ref) ON DELETE CASCADE\
        );'
    sql_kreis_2_gebiet = 'CREATE TABLE kreis_2_gebiet(\
        krs INT,\
        plz INT,\
        PRIMARY KEY(krs, plz),\
        FOREIGN KEY(krs) REFERENCES kreis(krs) ON DELETE CASCADE,\
        FOREIGN KEY(plz) REFERENCES gebiet(plz) ON DELETE CASCADE\
        );'
    cur = con.cursor()
    cur.execute(sql_land)
    cur.execute(sql_kreis)
    cur.execute(sql_gebiet)
    cur.execute(sql_meldung)
    cur.execute(sql_fall)
    cur.execute(sql_todesfall)
    cur.execute(sql_kreis_2_gebiet)
    cur.close()


def init_corona_db(con, zip_file):
    sql_insert_land = 'INSERT INTO land\
        VALUES (%s, %s);'
    sql_insert_kreis = 'INSERT INTO kreis\
        VALUES (%s, %s, %s);'
    sql_insert_gebiet = 'INSERT INTO gebiet\
        VALUES (%s);'
    sql_insert_kreis_2_gebiet = 'INSERT INTO kreis_2_gebiet\
        VALUES (%s, %s);'
    cur = con.cursor()
    zip = read_csv(zip_file)
    laender = zip.loc[:,['state_code', 'state']].drop_duplicates()
    kreise = zip.loc[:,['community_code', 'state_code', 'community']].drop_duplicates()
    gebiete = zip.loc[:,['zipcode']].drop_duplicates()
    kreis_2_gebiet = zip.loc[:,['community_code', 'zipcode']].drop_duplicates()
    for index, row in laender.iterrows():
        cur.execute(sql_insert_land, (row['state_code'], row['state']))
    for index, row in kreise.iterrows():
        cur.execute(sql_insert_kreis, (row['community_code'], row['state_code'], row['community']))
    for index, row in gebiete.iterrows():
        cur.execute(sql_insert_gebiet, (row['zipcode'],))
    for index, row in kreis_2_gebiet.iterrows():
        cur.execute(sql_insert_kreis_2_gebiet, (row['community_code'], row['zipcode']))
    cur.close()


def main():
    main_dir = Path(__file__).absolute().parent.parent
    zip_file = Path.joinpath(main_dir, 'data', 'zipcodes.de.csv')
    zip_url = 'https://github.com/zauberware/postal-codes-json-xml-csv/blob/master/data/DE/zipcodes.de.csv'
    if not os.path.exists(str(zip_file)):
        wget.download(zip_url, zip_file)
    cred = get_credentials()
    create_corona_db(cred)
    con = psycopg2.connect(
        dbname = cred['dbname'],
        user = cred['username'], 
        password = cred['password'],
        host = cred['host'],
        port = cred['port'])
    tables_corona_db(con)
    init_corona_db(con, zip_file)    
    con.commit()
    con.close()


if __name__ == '__main__':
    main()
