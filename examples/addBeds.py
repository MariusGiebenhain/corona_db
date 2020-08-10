import psycopg2
import csv
from pathlib import Path

def main():
    home = Path(__file__).absolute().parent.parent
    beds_file = Path.joinpath(home, 'data', 'krankenh_betten.csv')
    data = []
    with io.open(beds_file) as f:
        reader = csv.reader(f, delimiter = ';')
        for row in reader:
            data.append([row[1],row[3],row[4]])
    with psycopg2.connect(
        dbname = input('DB-Name: '),
        user = input('Username: '), 
        password = input('Password: '),
        host = input('Host: '),
        port = input('Port: ')) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE krankenhaeuser (
                krs INT,
                anzahl INT,
                betten INT,
                PRIMARY KEY(krs),
                FOREIGN KEY(krs) REFERENCES kreis(krs) ON DELETE CASCADE)
                """)
            for row in data:
                if row[1] != '-':
                    cur.execute("""
                        INSERT INTO krankenhaeuser
                        VALUES (%s,%s,%s)
                        """, row)


main()