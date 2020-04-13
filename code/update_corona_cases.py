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
    """
    Ask for access information for database
    default: local database on 5432-Port with default name(corona_db)
    -> only password needed
    """
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
    """
    rewrite case_table with the new RKI_COVID19 data
    """
    # define some sql routines:
    sql_krs = 'SELECT krs FROM kreis;'
    # reset meldung (and fall/todesfall through cascade delete)
    sql_flush = 'DELETE FROM meldung;'
    # insert meldung/fall/todesfall
    sql_meldung = 'INSERT INTO meldung\
        VALUES(%s, %s, %s, %s);'
    sql_fall = 'INSERT INTO fall\
        VALUES(%s, %s);'
    sql_todesfall = 'INSERT INTO todesfall\
        VALUES(%s, %s);'
    sql_genesen = 'INSERT INTO genesen\
        VALUES(%s, %s);'
    # reset cases in DB
    with con.cursor() as cur:
        cur.execute(sql_krs)
        kreise = set(krs[0] for krs in cur.fetchall())
        cur.execute(sql_flush)
        # read RKI_COVID19.csv
        data = read_csv(file)
        for index, row in data.iterrows():
            # for each row in the RKI_COVID19.csv write meldung + fall/todesfall to DB
            # build bev-ID
            bev = 'w'
            if row['Geschlecht'] == 'M': bev = 'm'
            age_range = re.findall('\d+', row['Altersgruppe'])
            if len(age_range) == 1:
                bev += '80_pl'
            elif len(age_range) == 2:
                bev += (age_range[0] + '_' + age_range[1])
            else:
                bev = ''
            # get cases/date/referenceNumber and krs
            nCase = row['AnzahlFall']
            nDead = row['AnzahlTodesfall']
            nRecov = row['AnzahlGenesen']
            date = row['Meldedatum']
            ref = row['ObjectId']
            date = datetime.datetime.strptime(date[0:10], '%Y-%m-%d')
            krs = row['IdLandkreis']
            # Check whether everything worked Ok and update DB
            if not (krs == '0-1' or len(bev) == 0):
                if 11000 <= int(krs) < 12000: krs = '11000'
                if krs in kreise:
                    cur.execute(sql_meldung, (ref, krs, bev, date))
                    if nCase > 0: cur.execute(sql_fall, (ref, nCase))
                    if nDead > 0: cur.execute(sql_todesfall, (ref, nDead))
                    if nRecov > 0: cur.execute(sql_genesen, (ref, nRecov))
    return


def main():
    """
    downloads newest Version of the RKI_COVID19.csv from opendata.arcgis
    update cases in DB
    """
    # Parse argument --keep-covid: Do not download new version if found
    parser = argparse.ArgumentParser(description='Update cases')
    parser.add_argument('--keep-covid', dest='keep', action='store_true',
                    help='do not replace existing RKI_COVID19.csv')
    args = parser.parse_args()
    # remove existing RKI_COVID19.csv and download newest version
    url = 'https://opendata.arcgis.com/datasets/dd4580c810204019a7b8eb3e0b329dd6_0.csv?session=undefined'
    main_dir = Path(__file__).absolute().parent.parent
    file = Path.joinpath(main_dir, 'data', 'RKI_COVID19.csv')
    if os.path.exists(str(file)) and not args.keep:
        os.remove(str(file))
    if not os.path.exists(str(file)):
        wget.download(url, out=str(file))
    # ask for access information, establish connection to DB and update
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
