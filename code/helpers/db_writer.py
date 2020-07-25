import psycopg2
import logging

class db_writer:
    def __init__(self, query, conn, logging = True):
        self.conn = conn
        self.query = query
        self.queue = []
        self.logging = logging
    
    def write(self, data = [[]], buffer = 50):
        with self.conn.cursor()  as cur: 
            for row in data:
                try:
                    cur.execute(self.query, row)
                    self.queue.append(row)
                except Exception as e:
                    if self.logging: logging.error('Error writing '+self.query+' for input '+str(row)+'\n', exc_info=e)
                    self.conn.rollback()
                    for q in self.queue:
                        cur.execute(self.query, q)
                if len(self.queue) == buffer:
                    self.conn.commit()
                    self.queue = []
            self.conn.commit()