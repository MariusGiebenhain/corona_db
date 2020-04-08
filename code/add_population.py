import psycopg2
import numpy as np
import re
import argparse
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)
from pathlib import Path
from pandas import read_csv


def get_credentials():
    dbname = input('DB-Name: ')
    username = input('User Name: ')
    password = input('Password: ')
    port = input('Port: ')
    if len(dbname) == 0: dbname = 'corona_db'
    if len(username) == 0: username = 'postgres'
    if len(port) == 0: port = '5432'
    return {'dbname' : dbname, 'username' : username, 'password' : password, 'port' : port}


def add_bev_table(con):
    bev_sql = 'CREATE TABLE bevoelkerung (\
        krs INT,\
        alter_von INT,\
        alter_bis INT,\
        geschlecht CHAR(1),\
        anzahl INT,\
        PRIMARY KEY(krs, alter_von, alter_bis, geschlecht),\
        FOREIGN KEY(krs) REFERENCES kreis(krs) ON DELETE CASCADE)'
    with con.cursor() as cur:
        cur.execute(bev_sql)
    return


def add_bev_values(file, con):
    sql_ = 'INSERT INTO bevoelkerung\
        VALUES(%s, %s, %s, %s, %s)'
    data = read_csv(file)
    age_groups = [[0,4],[5,14],[15,34],[35,59],[60,79],[80,125]]
    with con.cursor() as cur:
        for index, row in data.iterrows():
            krs = row['krs']
            for sex in ['m', 'w']:
                for i in range(len(age_groups)):
                    age_from = age_groups[i][0]
                    age_to = age_groups[i][1]
                    n = row[(sex + '.' + str(i))]
                    cur.execute(sql_, (krs, age_from, age_to, sex, n))
    return



def main():
    main_dir = Path(__file__).absolute().parent.parent
    file = Path.joinpath(main_dir, 'data', 'bev.csv')
    cred = get_credentials()
    con = psycopg2.connect(
            dbname = cred['dbname'],
            user= cred['username'], 
            password = cred['password'],
            port = cred['port'])
    add_bev_table(con)
    add_bev_values(file, con)
    con.commit()
    con.close()
    return


if __name__ == '__main__':
    main()
