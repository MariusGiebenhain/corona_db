import argparse
import logging
import psycopg2
from pathlib import Path
from helpers.credentials import credentials
from helpers.db_writer import db_writer
from helpers.csv_table import csv_table


def create_corona_db(cred, log):
    """
    Create the new DataBase
    """
    try:
        con = psycopg2.connect(
            dbname = 'postgres',
            user = cred.username, 
            password = cred.password,
            host = cred.host,
            port = cred.port)
        con.autocommit = True
        with con.cursor() as cur:
            cur.execute('CREATE DATABASE %s;' % (cred.dbname,))
        con.commit()
        con.close()
        if log: logging.info('Successfully initiated DB')    
    except Exception as e:
        if log: logging.error('Error: ', exc_info=e)
    return


def create_tables(conn, log):
    """
    create all nec. tables
    see rel. schema for details
    """
    db_writer(\
        'CREATE TABLE land(\
        land CHAR(2),\
        name VARCHAR(255),\
        PRIMARY KEY(land)\
        );',
        conn,
        log).write()
    db_writer(
        'CREATE TABLE kreis(\
        krs INT,\
        land CHAR(2),\
        name VARCHAR(255),\
        PRIMARY KEY(krs),\
        FOREIGN KEY(land) REFERENCES land(land)\
        );',
        conn,
        log).write()
    db_writer(
        'CREATE TABLE gebiet(\
        plz INT,\
        PRIMARY KEY(plz)\
        );',
        conn,
        log).write()
    db_writer(
        'CREATE TABLE bev_gruppe(\
        bev CHAR(6),\
        geschlecht CHAR(1),\
        alter_von INT,\
        alter_bis INT,\
        PRIMARY KEY(bev))',
        conn,
        log).write()
    db_writer(
        'CREATE TABLE meldung(\
        ref Int,\
        krs INT,\
        bev CHAR(6),\
        datum DATE,\
        PRIMARY KEY(ref),\
        FOREIGN KEY(krs) REFERENCES kreis(krs) ON DELETE CASCADE,\
        FOREIGN KEY(bev) REFERENCES bev_gruppe(bev)\
        );',
        conn,
        log).write()
    db_writer(
        'CREATE TABLE fall(\
        ref INT,\
        anzahl INT,\
        PRIMARY KEY(ref),\
        FOREIGN KEY(ref) REFERENCES meldung(ref) ON DELETE CASCADE\
        );',
        conn,
        log).write()
    db_writer(
        'CREATE TABLE todesfall(\
        ref INT,\
        anzahl INT,\
        PRIMARY KEY(ref),\
        FOREIGN KEY(ref) REFERENCES meldung(ref) ON DELETE CASCADE\
        );',
        conn,
        log).write()
    db_writer(
        'CREATE TABLE genesen(\
        ref INT,\
        anzahl INT,\
        PRIMARY KEY(ref),\
        FOREIGN KEY(ref) REFERENCES meldung(ref) ON DELETE CASCADE\
        );',
        conn,
        log).write()
    db_writer( 
        'CREATE TABLE kreis_2_gebiet(\
        krs INT,\
        plz INT,\
        PRIMARY KEY(krs, plz),\
        FOREIGN KEY(krs) REFERENCES kreis(krs) ON DELETE CASCADE,\
        FOREIGN KEY(plz) REFERENCES gebiet(plz) ON DELETE CASCADE\
        );',
        conn,
        log).write()
    return


def init_db(conn, zip_file, bev_file, log):
    """
    Build land/kreis/gebiet/bev_gruppe structure
    """
    insert_land = db_writer(
        'INSERT INTO land\
        VALUES (%s, %s);',
        conn,
        log)
    insert_kreis = db_writer(
        'INSERT INTO kreis\
        VALUES (%s, %s, %s);',
        conn,
        log)
    insert_gebiet = db_writer(
        'INSERT INTO gebiet\
        VALUES (%s);',
        conn,
        log)
    insert_kreis2gebiet = db_writer(
        'INSERT INTO kreis_2_gebiet\
        VALUES (%s, %s);',
        conn,
        log)
    insert_bev = db_writer(
        'INSERT INTO bev_gruppe\
        VALUES (%s, %s, %s, %s)',
        conn,
        log)
    spatial = csv_table(zip_file)
    bev = csv_table(bev_file)
    laender = spatial.select(['state_code', 'state']).unique()
    kreise = spatial.select(['community_code', 'state_code', 'community']).unique()
    gebiete = spatial.select(['zipcode']).unique()
    kreis2gebiet = spatial.select(['community_code', 'zipcode']).unique()
    insert_land.write(laender.content)
    insert_kreis.write(kreise.content)
    insert_gebiet.write(gebiete.content)
    insert_kreis2gebiet.write(kreis2gebiet.content)
    insert_bev.write(bev.content)
    return


def main():
    """
    main-method
    build file paths
    ask for database access
    and build basic structure
    """
    parser = argparse.ArgumentParser(description='Build DB')
    parser.add_argument('--logging', dest='log', action='store_true',
                    help='activate logging')
    args = parser.parse_args()
    home = Path(__file__).absolute().parent.parent
    zip_file = Path.joinpath(home, 'data', 'zipcodes.de.csv')
    bev_file = Path.joinpath(home, 'data', 'bev_gruppe.csv')
    log_file = Path.joinpath(home, 'logging', 'build_corona_db.log')
    if args.log: logging.basicConfig(filename=log_file, level=logging.INFO)
    cred = credentials()
    create_corona_db(cred, args.log)
    with psycopg2.connect(
            dbname = cred.dbname,
            user = cred.username, 
            password = cred.password,
            host = cred.host,
            port = cred.port) as conn:
        create_tables(conn, args.log)
        init_db(conn, str(zip_file), str(bev_file), args.log)    
    return


if __name__ == '__main__':
    main()