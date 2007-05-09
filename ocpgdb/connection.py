# Standard library
import itertools
import re
# Module
from oclibpq import *
import fromdb

to_db = {}                      # XXX this is a placeholder


class Cursor:
    _re_DQL = re.compile(r'^\s*SELECT\s', re.IGNORECASE)
    _re_4UP = re.compile(r'\sFOR\s+UPDATE', re.IGNORECASE)
    _re_IN2 = re.compile(r'\sINTO\s', re.IGNORECASE)

    def __init__(self, connection, name=None):
        self.__connection = connection
        if name is None:
            name = 'OcPy_%08X' % id(self)
        self.__name = name
        self.__cursor = False
        self.reset()
        self.arraysize = 10

    def reset(self):
        self.__result = None
        if self.__connection is not None and self.__cursor:
            self.__connection._execute('CLOSE "%s"' % self.__name)
        self.description = None
        self.rowcount = -1

    def close(self):
        self.reset()
        self.__connection = None

    def __del__(self):
        if getattr(self, '__connection', None) is not None:
            self.close()

    def _assert_open(self):
        if self.__connection is None:
            raise ProgrammingError("Cursor not open")

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass

    def execute(self, cmd, *args, **kwargs):
        self._assert_open()
        self.reset()
        use_cursor = (self._re_DQL.match(cmd) is not None and
                      self._re_4UP.search(cmd) is None and 
                      self._re_IN2.search(cmd) is None)
        if use_cursor:
            self.__connection.begin()
            cmd = 'DECLARE "%s" CURSOR FOR %s' % (self.__name, cmd)
        result = self.__connection._execute(cmd, *args, **kwargs)
        self.__cursor = use_cursor and result.result_type == 'DDL'
        if result.result_type == 'DQL':
            self.__result = result

    def _fetch(self, count=None):
        self._assert_open()
        if self.__result:
            rows = self.__result
            rr = self.__connection._result_row
            if count is not None:
                rows = itertools.islice(self.__result, count)
            return [rr(row) for row in rows]
        elif self.__cursor:
            if count is None:
                count = 'ALL'
            cmd = 'FETCH %s FROM "%s"' % (count, self.__name)
            result = self.__connection._execute(cmd)
            return self.__connection._result_rows(result)
        else:
            raise ProgrammingError('No results pending')

    def fetchall(self):
        return self._fetch()

    def fetchone(self):
        rows = self._fetch(1)
        if not rows:
            return None
        assert len(rows) == 1
        return rows[0]

    def fetchmany(self, count=None):
        if count is None:
            count = self.arraysize
        return self._fetch(count)


class Connection(PgConnection):
    _re_BEGIN = re.compile(r'^\s*BEGIN', re.IGNORECASE)
    _re_ENDTX = re.compile(r'^\s*(COMMIT|ROLLBACK)', re.IGNORECASE)

    def __init__(self, *args, **kwargs):
        conninfo = ','.join(['%s=%s' % i for i in kwargs.items()])
        self.from_db = dict(fromdb.from_db)
        self.to_db = dict(to_db)
        PgConnection.__init__(self, conninfo)
        # This makes sure we can parse what comes out of the db..
        self._execute('SET datestyle TO ISO')
        self.__intx = False

    def _result_column(self, cell):
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

    def _result_row(self, row):
        return tuple([self._result_column(cell) for cell in row])

    def _result_rows(self, result):
        if result.result_type == 'DQL':
            return [self._result_row(row) for row in result]

    def _value_to_db(self, value):
        if value is None:
            return None
        cvt = self.to_db.get(type(value), str)
        try:
            return cvt(value)
        except Exception, e:
            raise InternalError('column value %r: %s' % (value, e))

    def _args_to_db(self, args):
        return [self._value_to_db(a) for a in args]

    def cursor(self, name=None):
        return Cursor(self, name)

    def set_from_db(self, pgtype, fn):
        self.from_db[pgtype] = fn

    def use_python_datetime(self):
        fromdb._set_python_datetime(self.set_from_db)

    def _execute(self, cmd, *args):
        args = self._args_to_db(args)
        result = PgConnection.execute(self, cmd, args)
        if self._re_BEGIN.match(cmd):
            self.__intx = True
        elif self._re_ENDTX.match(cmd):
            self.__intx = False
        return result

    def execute(self, cmd, *args):
        return self._result_rows(self._execute(cmd, args))

    def begin(self):
        if not self.__intx:
            self._execute('BEGIN WORK')

    def commit(self):
        if self.__intx:
            self._execute('COMMIT WORK')

    def rollback(self):
        if self.__intx:
            self._execute('ROLLBACK WORK')


def connect(*args, **kwargs):
    return Connection(*args, **kwargs)
