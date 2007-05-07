from oclibpq import *
import pgtype

from_db = {}
to_db = {}

def connect(*args, **kwargs):
    return Connection(*args, **kwargs)

class Cursor:
    def __init__(self, pqconnection):
        self.pqconnection = pqconnection

    def execute(self, cmd, *args, **kwargs):
        self.pqconnection.execute(cmd, *args, **kwargs)

    def close(self):
        self.pqconnection.close()


class Connection(PgConnection):
    def __init__(self, *args, **kwargs):
        conninfo = ','.join(['%s=%s' % i for i in kwargs.items()])
        self.from_db = from_db
        self.to_db = to_db
        PgConnection.__init__(self, conninfo)

    def result_column(self, cell):
        try:
            cvt = self.from_db[cell.type]
        except KeyError:
            raise InterfaceError('No from_db function for type %r (column %r, value %r)'% (cell.type, cell.name, cell.value))
        else:
            return cvt(cell.value)

    def result_row(self, row):
        return [self.result_column(cell) for cell in row]

    def cursor(self):
        return OCcursor(self.pqconnection)

    def execute(self, cmd, *args):
        print cmd, args
        result = PgConnection.execute(self, cmd, args)
        return [self.result_row(row) for row in result]

def connect(*args, **kwargs):
    return Connection(*args, **kwargs)

