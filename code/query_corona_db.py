import csv
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
    Ask for DB credentials
    Default: Access to remote corona_db
    """
    dbname = input('DB-Name: ')
    username = input('User Name: ')
    password = input('Password: ')
    host = input('Host: ')
    port = input('Port: ')
    if len(dbname) == 0: dbname = 'corona_db'
    if len(username) == 0: username = 'guest'
    if len(password) == 0: password = 'pw'
    if len(host) == 0: host = '193.196.54.54'
    if len(port) == 0: port = '5432'
    return {'dbname' : dbname, 'username' : username, 'password' : password, 'host': host, 'port' : port}



def create_query():
    """
    Interactive creation of SQL-query
    """
    sql_cases = """
        SELECT DISTINCT l.name AS Bundesland, l.land, k.name AS Kreis, k.krs, m.datum AS Datum, 
            SUM(COALESCE(f.anzahl, 0)) OVER (PARTITION BY m.krs ORDER BY m.datum) AS Faelle, 
            SUM(COALESCE(t.anzahl, 0)) OVER (PARTITION BY m.krs ORDER BY m.datum) AS Todesfaelle
            FROM meldung m
            JOIN kreis k ON m.krs = k.krs
            JOIN land l ON k.land = l.land
            FULL JOIN fall f ON m.ref = f.ref
            FULL JOIN todesfall t ON m.ref = t.ref),"""
    sql_base_pop = """
        SELECT DISTINCT l.land AS Bundesland, l.land, k.name AS Kreis, k.krs, SUM(b.anzahl) AS Gesamtpopulation
            FROM kreis k
            JOIN land l ON k.land = l.land
            JOIN bevoelkerung b ON k.krs = b.krs
            GROUP BY l.land, k.krs"""
    sql_combine = """
    SELECT f.Bundesland, f.land, f.Kreis, f.krs, f.Datum, f.Faelle, f.Todesfaelle, p.Gesamtpopulation
        FROM faelle f JOIN population p ON f.krs = p.krs
        ORDER BY f.land, f.krs, f.datum"""
    res = input('Regionale Auflösung (Bund, Land, Kreis): ')
    date_from = str(input('Meldedatum vom (JJJJ-MM-TT): '))
    date_to = str(input('Meldedatum bis (JJJJ-MM-TT): '))
    age_from = input('älter als (4/14/34/59/79): ')
    age_to = input('jünger als (5/15/35/60/80): ')
    sex = input('Geschlecht(w/m)')
    change = input('Gebe Erhöhungen statt absoluter Zahl zurück (W): ')
    if res == 'Land': by = 'partition by l_.land'
    elif res == 'Kreis': by = 'partition by k_.krs'
    else: by = ''
    change = change == 'W'
    and_ = False
    where_ = ['WHERE', 'AND']
    q = 'WITH temp_ AS (SELECT DISTINCT '
    if res == 'Land': q += ' l_.name AS Land, l_.land AS Kuerzel, '
    if res == 'Kreis': q += ' l_.name AS Land, l_.land AS Kuerzel, k_.name AS Kreis, k_.krs AS KRS, '
    q += ' m_.datum AS Datum, '
    if change: 
        q += 'SUM(COALESCE(f_.anzahl, 0)) AS Faelle, SUM(COALESCE(t_.anzahl, 0)) AS TodesFaelle ' 
    else: 
        q += ('SUM(COALESCE(f_.anzahl, 0)) over (' + by +' order by m_.datum) AS Faelle,' + 
        ' SUM(COALESCE(t_.anzahl, 0)) over (' + by + ' order by m_.datum) AS TodesFaelle ') 
    q += (' FROM meldung m_ ' + 
        ' FULL JOIN fall f_ ON m_.ref = f_.ref ' +
        ' FULL JOIN todesfall t_ ON m_.ref = t_.ref ' +
        ' JOIN kreis k_ ON m_.krs = k_.krs ' +
        ' JOIN land l_ ON k_.land = l_.land ')
    if len(age_from) > 0: 
        q += where_[and_] + ' m_.alter_von > ' + age_from + ' '
        and_ = True
    if len(age_to) > 0: 
        q += where_[and_] + ' m_.alter_bis < ' + age_to + ' '
        and_ = True
    if len(sex) > 0:
        q += where_[and_] + ' m_.geschlecht = \'' + sex  + '\' '
    if change:
        q += ' GROUP BY m_.datum '
        if res in ['Land', 'Kreis']: q += ', l_.land '
        if res == 'Kreis': q += ', k_.krs'
    and_ = False
    q += ') SELECT '
    if res in ['Land', 'Kreis']: q += ' Land, Kuerzel, '
    if res == 'Kreis': q += 'Kreis, KRS, '
    q += 'Datum, Faelle, TodesFaelle FROM temp_ '
    if len(date_from) > 0:
        q += (where_[and_] + ' Datum >= \'' + date_from + '\' ')
        and_ = True
    if len(date_to) > 0:
        q += (where_[and_] + ' Datum <= \'' + date_to + '\' ')
        and_ = True
    q += ' ORDER BY '
    if res in ['Land', 'Kreis']: q += ' Kuerzel, '
    if res == 'Kreis': q += 'KRS, '
    q += ' Datum ;'
    return(q)


def query_corona_db(con, query):
    """
    wrapper:
    creates query with create_query(),
    queries stated connection and
    returns results
    """
    column_names = []
    result = []
    with con.cursor() as cur:
      cur.execute(query)
      column_names = [desc[0] for desc in cur.description]
      for row in cur:
        result.append(row)
    return (column_names, result)



def write_result(file, result):
    """
    simple write operation for query results
    """
    with open(file, "w+") as f:
        writer = csv.writer(f)
        writer.writerow(result[0])
        for row in result[1]:
            writer.writerow(row)
    return



def main():
    """
    main-method
    parses target file path,
    establishes connection,
    creates + executes query and
    writes results to target
    """
    parser = argparse.ArgumentParser(description='file to write to')
    parser.add_argument('file', metavar='target-file', nargs=1,
                    help='file that the query-results are written to')
    args = parser.parse_args()
    cred = get_credentials()
    con = psycopg2.connect(
        dbname = cred['dbname'],
        user = cred['username'], 
        password = cred['password'],
        host = cred['host'],
        port = cred['port'])
    query = create_query()
    result = query_corona_db(con, query)
    write_result(args.file[0], result)
    con.close()
    return


if __name__ == '__main__':
    main()
