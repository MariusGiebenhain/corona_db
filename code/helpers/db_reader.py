import psycopg2
import logging

class db_reader:
    def __init__(self, query, conn, logging = True):
        self.conn = conn
        self.query = query
        self.logging = logging
    
    def read(self):
        with self.conn.cursor() as cur:
            try:
                cur.execute(self.query)
                header = [d[0] for d in cur.description]
                content = []
                row = cur.fetchone()
                while len(row) != 0:
                    content.append([str(e) for e in row])
                    row = cur.fetchone()
                return {'header': header, 'content': content}
            except Exception as e:
                if self.logging: logging.error('Error: ', exc_info=e)
                self.conn.rollback()