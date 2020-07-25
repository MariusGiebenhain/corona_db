import psycopg2
import re
import argparse
from pathlib import Path
from helpers.credentials import credentials
from helpers.csv_table import csv_table
from helpers.db_writer import db_writer

def add_bev_table(conn, log):
    """
    Creates table bevoelkerung with population numbers
    """
    db_writer(
        """CREATE TABLE bevoelkerung (
            krs INT,
            bev CHAR(6),
            anzahl INT,
           PRIMARY KEY(krs, bev),
           FOREIGN KEY(krs) REFERENCES kreis(krs) ON DELETE CASCADE,
           FOREIGN KEY(bev) REFERENCES bev_gruppe(bev) ON DELETE CASCADE)""",
           conn,
           log).write()
    return


def add_bev_values(conn, data, log):
    """
    insert population numbers in bevoelkerung table
    """
    insert_population = db_writer(
        """INSERT INTO bevoelkerung\
        VALUES(%s, %s, %s)""",
        conn,
        log)
    age_groups = [['00','04'],['05','14'],['15','34'],['35','59'],['60','79'],['80','pl']]
    population = []
    for row in data.content:
        krs = row[0]
        for sex in ['m', 'w']:
            for i in range(len(age_groups)):
                bev = sex + age_groups[i][0] + '_' + age_groups[i][1]
                n = row[i+1]
                population.append([krs, bev, n])
    insert_population.write(population)
    return



def main():
    """
    build and fill bevoelkerung table
    """
    parser = argparse.ArgumentParser(description='Update cases')
    parser.add_argument('--logging', dest='log', action='store_true',
                    help='activate logging')
    args = parser.parse_args()
    main_dir = Path(__file__).absolute().parent.parent
    file = Path.joinpath(main_dir, 'data', 'bev.csv')
    log_file = Path.joinpath(home, 'logging', 'population.log')
    if args.log: logging.basicConfig(filename=log_file, level=logging.INFO)
    data = csv_table(str(file))
    cred = credentials()
    with psycopg2.connect(
            dbname = cred.dbname,
            user= cred.username, 
            password = cred.password,
            host = cred.host,
            port = cred.port) as conn:
        add_bev_table(conn, args.log)
        add_bev_values(conn, data, args.log)
    return


if __name__ == '__main__':
    main()
