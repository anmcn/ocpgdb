from oclibpq import *
import fromdb

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
        self.from_db = dict(fromdb.from_db)
        self.to_db = dict(to_db)
        PgConnection.__init__(self, conninfo)
        # This makes sure we can parse what comes out of the db..
        self.execute('SET datestyle TO ISO')

    def result_column(self, cell):
        if cell.value is None:
            return None
        try:
            cvt = self.from_db[cell.type]
        except KeyError:
            raise InterfaceError('No from_db function for type %r (column %r, value %r)'% (cell.type, cell.name, cell.value))
        else:
            try:
                return cvt(cell.value)
            except Exception, e:
                raise InternalError('failed to convert column value %r (column %r, value %r): %s' % (cell.value, cell.name, cell.type, e))

    def result_row(self, row):
        return [self.result_column(cell) for cell in row]

    def cursor(self):
        return OCcursor(self.pqconnection)

    def value_to_db(self, value):
        if value is None:
            return None
        cvt = self.to_db.get(type(value), str)
        try:
            return cvt(value)
        except Exception, e:
            raise InternalError('column value %r: %s' % (value, e))

    def set_from_db(self, pgtype, fn):
        self.from_db[pgtype] = fn

    def use_python_datetime(self):
        fromdb._set_python_datetime(self.set_from_db)

    def execute(self, cmd, *args):
        args = [self.value_to_db(a) for a in args]
        result = PgConnection.execute(self, cmd, args)
        return [self.result_row(row) for row in result]


def connect(*args, **kwargs):
    return Connection(*args, **kwargs)
