# Standard library
import sys
import itertools
import re
# Module
from oclibpq import *
from . import fromdb
from . import todb

class Cursor:
    _re_DQL = re.compile(r'^\s*SELECT\s', re.IGNORECASE)
    _re_4UP = re.compile(r'\sFOR\s+UPDATE', re.IGNORECASE)
    _re_IN2 = re.compile(r'\sINTO\s', re.IGNORECASE)

    def __init__(self, connection, name=None):
        self.connection = connection
        if name is None:
            name = 'OcPy_%08X' % (id(self) & 0xffffffff)
        self.__name = name
        self.__cursor = False
        self.reset()
        self.arraysize = 1     # Dim-witted DBAPI default rowcount for fetchmany

    def reset(self):
        self.__result = None
        if self.connection is None or self.connection.closed:
            raise ProgrammingError('Cursor is closed')
        if self.__cursor:
            self._execute('CLOSE "%s"' % self.__name)
            self.__cursor = False
        self.description = None
        self.rowcount = -1
        self.oidValue = None

    def close(self):
        self.reset()
        self.connection = None

    def __del__(self):
        if getattr(self, 'connection', None) is not None:
            try:
                self.close()
            except DatabaseError:
                pass

    def _assert_open(self):
        if self.connection is None:
            raise ProgrammingError("Cursor not open")

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass

    def _execute(self, cmd, args=()):
        return self.connection._execute(cmd, args)

    def _make_description(self, result):
        return [(col.name, col.type, None, None, None, None, None)
                for col in result.columns]

    def execute(self, cmd, args=None):
        self._assert_open()
        self.reset()
        cmd, args = self.connection._normalise_args(cmd, args)
        use_cursor = (self._re_DQL.match(cmd) is not None and
                      self._re_4UP.search(cmd) is None and 
                      self._re_IN2.search(cmd) is None)
        if not self.connection.autocommit:
            self.connection.begin()
        if use_cursor:
            cmd = 'DECLARE "%s" CURSOR WITHOUT HOLD FOR %s' % (self.__name, cmd)
        result = self._execute(cmd, args)
        if result.status == PGRES_TUPLES_OK:
            self.result_type = 'DQL'
            self.__result = result
            self.rowcount = result.ntuples
            self.description = self._make_description(result)
        elif result.status == PGRES_EMPTY_QUERY:
            self.result_type = 'EMPTY'
        elif result.status == PGRES_COMMAND_OK:
            if use_cursor:
                self.__cursor = True
                # We need to FETCH anyway to get the column descriptions.
                result = self._execute('FETCH 0 FROM "%s"' % self.__name)
                self.result_type = 'DQL'
                self.description = self._make_description(result)
            else:
                self.rowcount = result.cmdTuples
                self.oidValue = result.oid
                if self.rowcount:
                    self.result_type = 'DML'
                else:
                    self.result_type = 'DDL'
        else:
            raise InternalError('Unexpected result status %s' % result.status)
        return self

    def executemany(self, cmd, arglist):
        # We could be much smarter about this...
        for args in arglist:
            self.execute(cmd, args)

    def _fetch(self, count=None):
        self._assert_open()
        if count is not None:
            try:
                count = int(count)
            except (TypeError, ValueError):
                raise ProgrammingError('Fetch count must be an integer')
        if self.__result:
            rows = self.__result
            rr = self.connection._result_row
            if count is not None:
                rows = itertools.islice(self.__result, count)
            return [rr(row) for row in rows]
        elif self.__cursor:
            if count is None:
                count = 'ALL'
            result = self._execute('FETCH %s FROM "%s"' % (count, self.__name))
            return self.connection._result_rows(result)
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
    def __init__(self, **kwargs):
        """
        Connect to PostgreSQL database

        All arguments are optional, and include:
            host                Hostname
            port                TCP/IP port
            dbname              Database name
            database            Alias for dbname
            user                User name
            password            Password
            connect_timeout     Connect timeout in seconds (optional)
            sslmode
                disable         only unencrypted SSL connection
                allow           first try non-SSL, then SSL connection
                prefer          (the default) first try SSL, then non-SSL
                require         require an SSL connection
            show_notices        write Postgres notice messages to stderr
            use_mx_datetime     accept and return mx.DateTime types
            use_ipaddr          accept and return ipaddr instead of ipaddress
            autocommit          transaction autocommit mode
        """
        # Positional connection arguments are a horrible idea - only support
        # keyword args until convinced otherwise.
        if 'database' in kwargs:
            kwargs['dbname'] = kwargs.pop('database')
        self.show_notices = kwargs.pop('show_notices', False)
        self.autocommit = kwargs.pop('autocommit', False)
        use_mx_datetime = kwargs.pop('use_mx_datetime', False)
        use_ipaddr = kwargs.pop('use_ipaddr', False)
        conninfo = ' '.join(['%s=%s' % i for i in kwargs.items()])
        PgConnection.__init__(self, conninfo)
        # This makes sure we can parse what comes out of the db..
        self._execute('SET datestyle TO ISO')
        self.from_db = dict(fromdb.from_db)
        self.to_db = dict(todb.to_db)
        if use_mx_datetime:
            self.use_mx_datetime()
        else:
            self.use_py_datetime()
        if use_ipaddr:
            self.use_ipaddr()
        else:
            try:
                self.use_ipaddress()
            except ImportError:
                pass
        self._set_encoding(self.client_encoding)

    def set_from_db(self, pgtype, fn):
        self.from_db[pgtype] = fn

    def set_to_db(self, pytype, fn):
        self.to_db[pytype] = fn

    def _set_encoding(self, encoding):
        fromdb._set_encoding(self.set_from_db, encoding)
        todb._set_encoding(self.set_to_db, encoding)

    def use_py_datetime(self):
        fromdb._set_py_datetime(self.set_from_db, self.integer_datetimes)
        todb._set_py_datetime(self.set_to_db, self.integer_datetimes)

    def use_mx_datetime(self):
        fromdb._set_mx_datetime(self.set_from_db, self.integer_datetimes)
        todb._set_mx_datetime(self.set_to_db, self.integer_datetimes)

    def use_ipaddress(self):
        fromdb._set_ipaddress(self.set_from_db)
        todb._set_ipaddress(self.set_to_db)

    def use_ipaddr(self):
        fromdb._set_ipaddr(self.set_from_db)
        todb._set_ipaddr(self.set_to_db)

    def _result_row(self, row):
        return tuple([fromdb.value_from_db(self.from_db, cell) for cell in row])

    def _result_rows(self, result):
        if result.status == PGRES_TUPLES_OK:
            return [self._result_row(row) for row in result]

    def _args_to_db(self, args):
        return [todb.value_to_db(self.to_db, a) for a in args]

    def notice(self, msg):
        sys.stderr.write(msg)

    def _execute(self, cmd, args=()):
        args = self._args_to_db(args)
        result = PgConnection.execute(self, cmd, args)
        if self.show_notices:
            for msg in self.notices:
                self.notice(msg)
            del self.notices[:]
        if result.status == PGRES_NONFATAL_ERROR:
            raise ProgrammingError(result.errorMessage)
        elif result.status == PGRES_FATAL_ERROR:
            msg = result.errorMessage
            if 'referential integrity violation' in msg:
                raise IntegrityError(msg)
            else:
                raise OperationalError(msg)
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
                        raise ProgrammingError('argument %%(%s)s not found in dictionary' % k)
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

    def _normalise_args(self, cmd, args):
        if not args:
            return cmd, ()
        if hasattr(args, 'keys'):
            return self._normalise_dict_args(cmd, args)
        return self._normalise_seq_args(cmd, args)

    def execute(self, cmd, args=None):
        cmd, args = self._normalise_args(cmd, args)
        return self._result_rows(self._execute(cmd, args))

    def begin(self):
        if self.transactionStatus == TRANS_IDLE:
            self._execute('BEGIN WORK')

    def commit(self):
        if self.transactionStatus != TRANS_IDLE:
            self._execute('COMMIT WORK')

    def rollback(self):
        if self.transactionStatus != TRANS_IDLE:
            self._execute('ROLLBACK WORK')

    def cursor(self, name=None):
        return Cursor(self, name)


def connect(*args, **kwargs):
    return Connection(*args, **kwargs)
connect.__doc__ = Connection.__init__.__doc__
