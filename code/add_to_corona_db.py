"""
NEEDS UPDATE - Currently not working
"""
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


def add_table(file, id, con):
    sql_add_column = 'ALTER TABLE %s\
        ADD %s %s;'
    sql_update = 'UPDATE %s\
        SET %s = %s\
        WHERE %s = %s'
    cur = con.cursor()
    data = read_csv(file)
    columns = data.columns  
    if id not in ['plz', 'krs', 'land', 'fall']: 
        id = 'NA'
    if id == 'NA':
        if 'plz' in columns: 
            id = 'plz'
        elif 'krs' in columns: 
            id = 'krs'
        elif 'land' in columns: 
            id = 'land'     
    if id == 'plz':
        table = 'gebiet'
    elif id == 'krs': 
        table = 'kreis'
    elif id == 'land':
        table = 'land'
    keys = data[id]
    columns = [col for col in columns if col not in ['Unnamed: 0', 'name', 'plz', 'krs', 'land']]
    for col in columns:
        insert_col = input('Insert Column ' + col + '? ')
        if insert_col == 'T':
            col_ = re.sub("[- ]", "_", col)
            type = input('Data Type: ')
            cur.execute(sql_add_column % (table, col_, type))
            for i in range(len(data[col])):
                if not np.isnan(data[col][i]):
                    cur.execute(sql_update % (table, col_, data[col][i], id, keys[i]))
    cur.close()
    return


def main():
    parser = argparse.ArgumentParser(description='Add CSVs to coronaDB.')
    parser.add_argument('files', metavar='csv-files', nargs='+',
                    help='files to add')
    parser.add_argument('--auto-level', dest='auto_detect', metavar='auto-detect', action='store_const',
                    const=True, default=False,
                    help='auto-detect file resolution')
    args = parser.parse_args()
    main_dir = Path(__file__).absolute().parent.parent
    cred = get_credentials()
    con = psycopg2.connect(
            dbname = cred['dbname'],
            user= cred['username'], 
            password = cred['password'],
            port = cred['port'])
    for file in args.files:
        if not file[-4:] == '.csv': file += '.csv'
        file = Path.joinpath(main_dir, 'data', file)
        if not args.auto_detect:
            id = input('Id Variable: ')
        else:
            id = 'NA'
        add_table(file, id, con)
    con.commit()
    con.close()
    return


if __name__ == '__main__':
    main()
