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


def case_select(age_from, age_to, sex, change):
    """
    Create case-table for WITH statement
    """
    sql_cases = 'cases AS (SELECT t_2.krs, t_2.datum, {}COALESCE(t_1.faelle, 0){} AS faelle, {}COALESCE(t_1.todesfaelle, 0){} AS todesfaelle, {}COALESCE(t_1.genesen, 0){} AS genesen \
        FROM ( \
            SELECT DISTINCT m.krs, m.datum AS datum, \
            SUM(COALESCE(f.anzahl, 0)) AS faelle, \
            SUM(COALESCE(t.anzahl, 0)) AS todesfaelle, \
            SUM(COALESCE(g.anzahl, 0)) AS genesen \
            FROM meldung m \
                FULL JOIN fall f ON m.ref = f.ref \
                FULL JOIN todesfall t ON m.ref = t.ref \
                FULL JOIN genesen g ON m.ref = g.ref \
                {} \
            {} \
            GROUP BY m.krs, m.datum) t_1 \
        RIGHT JOIN (SELECT k.krs, datum.day AS datum FROM \
            (SELECT DISTINCT krs FROM kreis) k, \
            generate_series((SELECT MIN(datum) FROM meldung), (SELECT MAX(datum) FROM meldung), \'1 day\') AS datum(day)) t_2 ON t_1.krs = t_2.krs AND t_1.datum = t_2.datum)'
    if change:
        over_ = ['', '']
    else:
        over_ = ['SUM(', ') OVER (PARTITION BY t_2.krs ORDER BY t_2.datum)']
    where_ = where_pop(age_from, age_to, sex)
    if len(where_):
        join_bev = 'JOIN bev_gruppe b ON m.bev = b.bev'
    else:
        join_bev = ''
    return sql_cases.format(over_[0], over_[1], over_[0], over_[1], over_[0], over_[1], join_bev, where_)



def base_pop_select():
    """
    create base population query for WITH statement
    """
    sql_basepop = ', base_pop AS (SELECT DISTINCT k.krs, SUM(b.anzahl) AS gesamtpopulation \
        FROM kreis k \
        JOIN bevoelkerung b ON k.krs = b.krs \
        GROUP BY k.krs)'
    return sql_basepop



def sub_pop_select(age_from, age_to, sex):
    """
    create subpopulation query for WITH statement
    """
    # if subpopulation is specified create fitting statement
    if len(age_from) | len(age_to) | len(sex):
        sql_subpop = ', sub_pop AS (SELECT DISTINCT k.krs, SUM(b_.anzahl) AS zielpopulation \
                FROM kreis k \
                JOIN bevoelkerung b_ ON k.krs = b_.krs \
                JOIN bev_gruppe b ON b_.bev = b.bev \
                {} \
                GROUP BY k.krs)'
        where_ = where_pop(age_from, age_to, sex)
        return sql_subpop.format(where_)
    # else: return empty string
    else:
        return ''



def where_pop(age_from, age_to, sex):
    """
    prepares where condition for target population
    """
    where_ = ''
    cond_ = 'WHERE '
    if len(age_from):
        where_ += (cond_ + 'b.alter_von > ' + age_from)
        cond_ = ' AND '
    if len(age_to):
        where_ += (cond_ + 'b.alter_bis < ' + age_to)
        cond_ = ' AND '
    if len(sex):
        where_ += (cond_ + 'b.geschlecht = \'' + sex + '\' ')
    return where_


def where_date(date_from, date_to):
    """
    Prepares where condition on date-specification
    """
    where_ = ''
    cond_ = 'WHERE '
    if len(date_from):
        where_ += (cond_ + 'c.datum >= \'' + date_from + '\' ')
        cond_ = ' AND '
    if len(date_to):
        where_ += (cond_ + 'c.datum <= \'' + date_to + '\' ')
    return where_


def create_query():
    """
    Interactive creation of SQL-query
    Base structure:
    1st WITH statement queries cases for target population on Kreis level
    2nd WITH statement queries total population on Kreis level
    3rd WITH statement queries target population on Kreis level
    Main Statement combines the three tables (with possible aggregation on Land/Bund-level) and filters by date
    """
    query = 'WITH {}{}{} \
        SELECT {}, \
            SUM(c.faelle) AS faelle, \
            SUM(c.todesfaelle) AS todesfaelle, \
            sum(c.genesen) AS genesen, \
            SUM(p.gesamtpopulation) AS gesamtpopulation \
            {} \
        FROM cases c \
        JOIN kreis k ON c.krs = k.krs \
        JOIN land l ON k.land = l.land \
        JOIN base_pop p ON c.krs = p.krs \
        {} \
        {} \
        GROUP BY {} \
        ORDER BY {};'
    # get specifications of the query:
    res = input('Regionale Auflösung (Bund, Land, Kreis): ')
    date_from = str(input('Meldedatum vom (JJJJ-MM-TT): '))
    date_to = str(input('Meldedatum bis (JJJJ-MM-TT): '))
    age_from = input('älter als (4/14/34/59/79): ')
    age_to = input('jünger als (5/15/35/60/80): ')
    sex = input('Geschlecht(w/m): ')
    change = input('Gebe Erhöhungen statt absoluter Zahl zurück (W): ') == 'W'
    # Build the three with statements in sub-routines
    sql_cases = case_select(age_from, age_to, sex, change)
    sql_base_pop = base_pop_select()
    sql_sub_pop = sub_pop_select(age_from, age_to, sex)
    # prepare main query
    sub_pop = ['', '']
    if len(sql_sub_pop) > 0:
        sub_pop[0] = ', SUM(s.zielpopulation) AS zielpopulation '
        sub_pop[1] = ' JOIN sub_pop s ON c.krs = s.krs '
    where_ = where_date(date_from, date_to)
    by_ = ''
    if res in ['Land', 'Kreis']: by_ += 'l.land, l.name, '
    if res == 'Kreis': by_ += 'k.krs, k.name, '
    by_ += 'c.datum'
    return query.format(sql_cases, sql_base_pop, sql_sub_pop, by_, sub_pop[0], sub_pop[1], where_, by_, by_)


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
