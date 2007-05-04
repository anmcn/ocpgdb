from oclibpq import *

apilevel = '2.0'
threadsafety = 1
paramstyle = 'pyformat'
__version__ = '0.1'

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
        PgConnection.__init__(self, conninfo)

    def cursor(self):
        return OCcursor(self.pqconnection)


def connect(*args, **kwargs):
    return Connection(*args, **kwargs)

