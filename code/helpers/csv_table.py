import io
class csv_table:
    def __init__(self, file = '', sep = ',', encoding = 'utf-8', header = [], content = []):
        self.header = header
        self.content = content
        if len(file):
            self.read(file, sep, encoding)
    
    def read(self, file, sep = ',', encoding = 'utf-8'):
        with io.open(file, 'r', encoding = encoding) as f:
            self.header = f.readline().rstrip('\n').split(sep)
            self.content = []
            line = f.readline()
            while line  != "":
                self.content.append(self.parseline(line)) 
                line = f.readline()
    
    def parseline(self, line, sep = [',', '\n'], quotes = ['"', '\'']):
        data = []
        w = ''
        quoted = False
        for c in line:
            if (c not in sep) or quoted:
                w+=c
                quoted = (quoted and (c not in quotes)) or (not quoted and (c in quotes))
            else:
                data.append(w)
                w = ''
        if len(w) > 0: data.append(w)
        return data
    
    def select(self, selection, reduce = False):
        selector = [self.header.index(s) for s in selection]
        content = sub = [[r[s] for s in selector] for r in self.content]
        return csv_table(header = selection, content = content)
    
    def unique(self, id = 0):
        sub = {}
        for row in self.content:
            key = row[id]
            if key not in sub:
                sub[key] = [row]
            else:
                if row not in sub[key]:
                    sub[key].append(row)
        return csv_table(header = self.header, content = [l for hashlist in sub.values() for l in hashlist])
    
    def write(self, file):
        with open(file, "w") as f:
            f.write(str(self.header)[1:-1]+'\n')
            f.writelines([str(row)[1:-1]+'\n' for row in self.content])
        return