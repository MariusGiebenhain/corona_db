import getpass
class credentials:
    def __init__(self, dbname = '', username = '', password = '', host = '', port = ''):
        self.dbname = dbname if len(dbname) else input('DB-Name: ')
        self.username = username if len(username) else input('User Name: ')
        self.password = password if len(password) else getpass.getpass('Password: ')
        self.host = host if len(host) else input('Host: ')
        self.port = port if len(port) else input('Port: ')
        if len(self.dbname) == 0: self.dbname = 'corona_db'
        if len(self.username) == 0: self.username = 'postgres'
        if len(self.host) == 0: self.host = 'localhost'
        if len(self.port) == 0: self.port = '5432'