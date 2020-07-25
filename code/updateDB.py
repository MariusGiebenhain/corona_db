import argparse
import datetime
import logging
import os
import psycopg2
import re
import wget
from pathlib import Path
from helpers.credentials import credentials
from helpers.csv_table import csv_table
from helpers.db_writer import db_writer

def get_reports(data):
    reports = []
    for row in data.content:
        bev = row[data.header.index('Geschlecht')].lower()
        age_range = re.findall('\d+', row[data.header.index('Altersgruppe')])
        if len(age_range) == 1:
            bev += '80_pl'
        elif len(age_range) == 2:
            bev += (age_range[0] + '_' + age_range[1])
        else: 
            bev = '-1'
        date = datetime.datetime.strptime(row[data.header.index('Meldedatum')][0:10], '%Y/%m/%d')
        ref = row[data.header.index('ObjectId')]
        krs = row[data.header.index('IdLandkreis')]
        if 11000 <= int(krs) < 12000: krs = '11000'
        reports.append([ref, krs, bev, date])
    return reports
 
def update_covid19(data, conn, log):
    """
    rewrite case_table with the new RKI_COVID19 data
    """
    # define some sql routines:
    # reset meldung (and fall/todesfall through cascade delete)
    flush = db_writer(
        'DELETE FROM meldung;',
        conn,
        log)
    # insert meldung/fall/todesfall
    insert_report = db_writer(
        'INSERT INTO meldung\
        VALUES(%s, %s, %s, %s);',
        conn,
        log)
    insert_case = db_writer(
        'INSERT INTO fall\
        VALUES(%s, %s);',
        conn,
        log)
    insert_death = db_writer(
        'INSERT INTO todesfall\
        VALUES(%s, %s);',
        conn,
        log)
    insert_recovered = db_writer(
        'INSERT INTO genesen\
        VALUES(%s, %s);',
        conn,
        log)
    reports = get_reports(data)
    cases = [row for row in data.select(['ObjectId', 'AnzahlFall']).content if int(row[1])>0]
    deaths = [row for row in data.select(['ObjectId', 'AnzahlTodesfall']).content if int(row[1])>0]
    recovered = [row for row in data.select(['ObjectId', 'AnzahlGenesen']).content if int(row[1])>0]
    with conn.cursor() as cur:
        flush.write()
        insert_report.write(reports)
        insert_case.write(cases)
        insert_death.write(deaths)
        insert_recovered.write(recovered)
    return


def main():
    """
    downloads newest Version of the RKI_COVID19.csv from opendata.arcgis
    update cases in DB
    """
    # Parse argument --keep-covid: Do not download new version if found
    parser = argparse.ArgumentParser(description='Update cases')
    parser.add_argument('--logging', dest='log', action='store_true',
                    help='activate logging')
    parser.add_argument('--keep-covid', dest='keep', action='store_true',
                    help='do not replace existing RKI_COVID19.csv')
    args = parser.parse_args()
    home = Path(__file__).absolute().parent.parent
    file = Path.joinpath(home, 'data', 'RKI_COVID19.csv')
    log_file = Path.joinpath(home, 'logging', 'update.log')
    if args.log: logging.basicConfig(filename=log_file, level=logging.INFO)
    # remove existing RKI_COVID19.csv and download newest version
    if os.path.exists(str(file)) and not args.keep:
        os.remove(str(file))
    if not os.path.exists(str(file)):
        url = 'https://opendata.arcgis.com/datasets/dd4580c810204019a7b8eb3e0b329dd6_0.csv'
        wget.download(url, out=str(file))
        print('\ndownloaded rki_covid19.csv')
    data = csv_table(str(file))
    data.header[0] = 'ObjectId'
    # ask for access information, establish connection to DB and update
    cred = credentials()
    with psycopg2.connect(
            dbname = cred.dbname,
            user= cred.username, 
            password = cred.password,
            host = cred.host,
            port = cred.port) as conn:
        update_covid19(data, conn, args.log)
    return


if __name__ == '__main__':
    main()