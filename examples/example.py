########
# Sample Script
# Queries and plots Case Shares and Population Shares by demographic group (sex/age) 
# over the whole period of observation
# Case shares are plotted as lines, population shares as horicontal dotted lines
########

import psycopg2
from matplotlib import pyplot as plt
from matplotlib import colors as mcolors

########
# Create DB-Query
query = """
    WITH
    timeseries AS (
        SELECT date
        FROM generate_series( 
            (SELECT MIN(datum) FROM meldung)::timestamp, 
            (SELECT MAX(datum) FROM meldung)::timestamp, 
            '1 day'::interval) AS date),
    datebev AS (
        SELECT DISTINCT t.date, b.bev
        FROM timeseries t, bev_gruppe b),
    groupcases AS (
        SELECT DISTINCT d.date, d.bev, SUM(COALESCE(f.anzahl, 0)) OVER(PARTITION BY d.bev ORDER BY d.date) AS cases
        FROM meldung m
        NATURAL JOIN fall f
        RIGHT OUTER JOIN datebev d ON m.datum = d.date AND m.bev = d.bev),
    totalcases AS (
        SELECT DISTINCT t.date, SUM(COALESCE(f.anzahl, 0)) OVER(ORDER BY t.date) AS cases
        FROM meldung m
        NATURAL JOIN fall f
        RIGHT OUTER JOIN timeseries t ON m.datum = t.date),
    popshare AS (
        SELECT bev, SUM(anzahl)/CAST((SELECT SUM(anzahl) FROM bevoelkerung) AS FLOAT) AS popshare
        FROM bevoelkerung
        GROUP BY bev)
    SELECT g.date, g.bev, g.cases/CAST(t.cases AS FLOAT) AS caseshare, p.popshare
    FROM groupcases g
    JOIN totalcases t ON g.date = t.date
    JOIN popshare p ON g.bev = p.bev 
    ORDER BY g.bev, g.date;
    """
# establish Connection to DB server
with psycopg2.connect(
        dbname = 'corona_db',
        user = 'guest', 
        password = 'pw',
        host = '193.196.54.54',
        port = '5432') as conn:
    # Create cursor
    with conn.cursor() as cur:
        # execute query and fetch results
        cur.execute(query)
        data = cur.fetchall()
        # extract information from retrieved tuples
        caseshares = {}
        popshares = {}
        ticks = [['January'], [-1]]
        for i, row in enumerate(data):
            if row[1] not in popshares:
                popshares[row[1] ] = row[3]
            if row[1] not in caseshares:
                caseshares[row[1]] = [row[2]]
            else:
                caseshares[row[1]].append(row[2])
            if row[0].strftime("%B") not in ticks[0]:
                ticks[0].append(row[0].strftime("%B"))
                ticks[1].append(i)
        # plot extracted data
        colors = ['b', 'g', 'r', 'c', 'm', 'y'] + [mcolors.TABLEAU_COLORS[color] for color in mcolors.TABLEAU_COLORS]
        for i, demographic in enumerate(caseshares):
            plt.plot(caseshares[demographic], label = demographic, color = colors[i])
            plt.hlines(popshares[demographic], 1, len(caseshares[demographic]), color = colors[i], linestyles =  'dotted')
        plt.ylabel('case/population share')
        plt.xlabel('date/month')
        plt.xticks(ticks[1][1:], ticks[0][1:], rotation=20)
        plt.legend()
        plt.show()