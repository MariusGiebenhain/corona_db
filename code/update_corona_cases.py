import re
import os
import datetime
import argparse
import psycopg2
import wget
import numpy as np
from pathlib import Path
from pandas import read_csv
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)


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



def update_covid(file, con):
    sql_flush = 'DELETE FROM meldung;'
    sql_meldung = 'INSERT INTO meldung\
        VALUES(%s, %s, %s, %s, %s, %s);'
    sql_fall = 'INSERT INTO fall\
        VALUES(%s, %s);'
    sql_todesfall = 'INSERT INTO todesfall\
        VALUES(%s, %s);'
    data = read_csv(file)
    cur = con.cursor()
    cur.execute(sql_flush)
    for index, row in data.iterrows():
        age_range = re.findall('\d+', row['Altersgruppe'])
        if len(age_range) == 0:
            age_from = age_to = None
        else:
            age_from = age_range[0]
            age_to = 125
            if len(age_range) == 2: age_to = age_range[1]
        sex = 'w'
        if row['Geschlecht'] == 'M': sex = 'm'
        nCase = row['AnzahlFall']
        nDead = row['AnzahlTodesfall']
        date = row['Meldedatum']
        ref = row['ObjectId']
        date = datetime.datetime.strptime(date[0:10], '%Y-%m-%d')
        krs = row['IdLandkreis']
        if not krs == '0-1':
            if 11000 <= int(krs) < 12000: krs = '11000'
            cur.execute(sql_meldung, (ref, krs, age_from, age_to, sex, date))
            if nCase > 0: cur.execute(sql_fall, (ref, nCase))
            if nDead > 0: cur.execute(sql_todesfall, (ref, nDead))
    cur.close()
    return


def main():
    parser = argparse.ArgumentParser(description='Update cases')
    parser.add_argument('--keep-covid', dest='keep', action='store_true',
                    help='do not replace existing RKI_COVID19.csv')
    args = parser.parse_args()
    url = 'https://opendata.arcgis.com/datasets/dd4580c810204019a7b8eb3e0b329dd6_0.csv?session=undefined'
    main_dir = Path(__file__).absolute().parent.parent
    file = Path.joinpath(main_dir, 'data', 'RKI_COVID19.csv')
    if os.path.exists(str(file)) and not args.keep:
        os.remove(file)
    wget.download(url, out=str(file))
    cred = get_credentials()
    con = psycopg2.connect(
            dbname = cred['dbname'],
            user= cred['username'], 
            password = cred['password'],
            host = cred['host'],
            port = cred['port'])
    update_covid(file, con)
    con.commit()
    con.close()
    return


if __name__ == '__main__':
    main()
