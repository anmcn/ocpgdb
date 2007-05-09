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
        cmd, args = self.__connection._normalise_args(cmd, args, kwargs)
        use_cursor = (self._re_DQL.match(cmd) is not None and
                      self._re_4UP.search(cmd) is None and 
                      self._re_IN2.search(cmd) is None)
        if use_cursor:
            self.__connection.begin()
            cmd = 'DECLARE "%s" CURSOR FOR %s' % (self.__name, cmd)
        result = self.__connection._execute(cmd, args)
        self.__cursor = use_cursor and result.result_type == 'DDL'
        if result.result_type == 'DQL':
            self.__result = result
        return self

    def _fetch(self, count=None):
        self._assert_open()
        if count is not None:
            try:
                count = int(count)
            except (TypeError, ValueError):
                raise ProgrammingError('Fetch count must be an integer')
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

    def set_from_db(self, pgtype, fn):
        self.from_db[pgtype] = fn

    def use_python_datetime(self):
        fromdb._set_python_datetime(self.set_from_db)

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

    def _execute(self, cmd, args=()):
        args = self._args_to_db(args)
        result = PgConnection.execute(self, cmd, args)
        if self._re_BEGIN.match(cmd):
            self.__intx = True
        elif self._re_ENDTX.match(cmd):
            self.__intx = False
        return result

    def _normalise_dict_args(self, cmd, dictargs):
        class DictArgs:
            def __init__(self, dictargs):
                self.index = 0
                self.args = []
                self.dictargs = dictargs
                self.fmtdict = {}
            def __getitem__(self, k):
                fmt = self.fmtdict.get(k, None)
                if fmt is None:
                    try:
                        self.args.append(self.dictargs[k])
                    except KeyError:
                        raise ProgrammingError('argument %%(%)s not found in dictionary' % k)
                    self.index += 1
                    self.fmtdict[k] = fmt = '$%d' % self.index
                return fmt
            def __str__(self):
                raise ProgrammingError('command contains %s with dict args')
        dictargs = DictArgs(dictargs)
        cmd = cmd % dictargs
        return cmd, tuple(dictargs.args)

    def _normalise_seq_args(self, cmd, seqargs):
        seqargs = tuple(seqargs)
        cmd = cmd.split('%s')
        expected = len(cmd) - 1
        if expected != len(seqargs):
            raise ProgrammingError('wrong number of arguments for command string (expected %d, got %s)' % (expected, len(seqargs)))
        for i in xrange(expected, 0, -1):
            cmd.insert(i, '$%d' % i)
        return ''.join(cmd), seqargs

    def _normalise_args(self, cmd, args, kwargs):
        if not args and not kwargs:
            return cmd, ()
        if kwargs:
            if args:
                raise ProgrammingError('Cannot mix dict and tuple args')
            return self._normalise_dict_args(cmd, kwargs)
        if len(args) == 1:
            if hasattr(args[0], 'keys'):
                return self._normalise_dict_args(cmd, args[0])
            elif hasattr(args[0], '__iter__'):
                return self._normalise_seq_args(cmd, args[0])
        return self._normalise_seq_args(cmd, args)

    def execute(self, cmd, *args, **kwargs):
        cmd, args = self._normalise_args(cmd, args, kwargs)
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

    def cursor(self, name=None):
        return Cursor(self, name)


def connect(*args, **kwargs):
    return Connection(*args, **kwargs)
