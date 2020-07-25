import getpass
class credentials:
    def __init__(self):
        self.dbname = input('DB-Name: ')
        self.username = input('User Name: ')
        self.password = getpass.getpass('Password: ')
        self.host = input('Host: ')
        self.port = input('Port: ')
        if len(self.dbname) == 0: self.dbname = 'corona_db'
        if len(self.username) == 0: self.username = 'postgres'
        if len(self.host) == 0: self.host = 'localhost'
        if len(self.port) == 0: self.port = '5432'