import sys
import unittest
import ocpgdb
import operator
import datetime
try:
    import decimal
    have_decimal = True
except ImportError:
    print 'WARNING: Decimal not available, tests skipped'
    have_decimal = False
try:
    from mx import DateTime
    have_mx = True
except ImportError:
    print 'WARNING: mx.DateTime not available, tests skipped'
    have_mx = False

scratch_db = dict(dbname='ocpgdb_test', port=5432)


class BasicTests(unittest.TestCase):
    def test_module_const(self):
        mandatory_attrs = (
            # General info:
            'apilevel', 'threadsafety', 'paramstyle', '__version__', 
            # Exceptions:
            'Warning', 'Error', 
            'InterfaceError', 'DatabaseError', 
            'DataError', 'OperationalError', 'IntegrityError', 'InternalError',
            'ProgrammingError', 'NotSupportedError',
            # Type support:
            'Binary', 'Date', 'Time', 'Timestamp', 
            'DateFromTicks', 'TimestampFromTicks', 'TimeFromTicks',
            'STRING', 'BINARY', 'NUMBER', 'DATETIME', 'ROWID',
            # Extensions - setErrorVerbosity
            'ERRORS_TERSE', 'ERRORS_DEFAULT', 'ERRORS_VERBOSE',
            # Extensions - transactionStatus
            'TRANS_IDLE', 'TRANS_ACTIVE', 'TRANS_INTRANS',
            'TRANS_INERROR', 'TRANS_INERROR', 'TRANS_UNKNOWN',
            # Extensions - PQresultStatus
            'PGRES_EMPTY_QUERY', 'PGRES_COMMAND_OK', 'PGRES_TUPLES_OK',
            'PGRES_COPY_OUT', 'PGRES_COPY_IN', 'PGRES_BAD_RESPONSE',
            'PGRES_NONFATAL_ERROR', 'PGRES_FATAL_ERROR',
            # Extensions - PQresultErrorField
            "DIAG_SEVERITY", "DIAG_SQLSTATE", "DIAG_MESSAGE_PRIMARY",
            "DIAG_MESSAGE_DETAIL", "DIAG_MESSAGE_HINT",
            "DIAG_STATEMENT_POSITION", "DIAG_INTERNAL_POSITION",
            "DIAG_INTERNAL_QUERY", "DIAG_CONTEXT", "DIAG_SOURCE_FILE",
            "DIAG_SOURCE_LINE",
        )
        for attr in mandatory_attrs:
            self.failUnless(hasattr(ocpgdb, attr), 
                'Module does not export mandatory attribute %r' % attr)
        self.failUnless(issubclass(ocpgdb.Warning, StandardError))
        self.failUnless(issubclass(ocpgdb.Error, StandardError))
        self.failUnless(issubclass(ocpgdb.InterfaceError, ocpgdb.Error))
        self.failUnless(issubclass(ocpgdb.DatabaseError, ocpgdb.Error))
        self.failUnless(issubclass(ocpgdb.DataError, ocpgdb.DatabaseError))
        self.failUnless(issubclass(ocpgdb.OperationalError, ocpgdb.DatabaseError))
        self.failUnless(issubclass(ocpgdb.IntegrityError, ocpgdb.DatabaseError))
        self.failUnless(issubclass(ocpgdb.InternalError, ocpgdb.DatabaseError))
        self.failUnless(issubclass(ocpgdb.ProgrammingError, ocpgdb.DatabaseError))
        self.failUnless(issubclass(ocpgdb.NotSupportedError, ocpgdb.DatabaseError))
        self.assertEqual(ocpgdb.apilevel, '2.0')
        self.assertEqual(ocpgdb.threadsafety, 1)
        self.assertEqual(ocpgdb.paramstyle, 'pyformat')

    def test_connect(self):
        c = ocpgdb.connect(**scratch_db)
        self.failUnless(c.fileno() >= 0)
        self.failUnless(isinstance(c.conninfo, str))
        self.assertEqual(c.notices, [])
        self.failUnless(isinstance(c.host, str))
        self.failUnless(isinstance(c.port, int))
        self.failUnless(isinstance(c.db, str))
        self.failUnless(isinstance(c.user, str))
        self.failUnless(isinstance(c.password, str))
        self.failUnless(isinstance(c.options, str))
        self.failUnless(isinstance(c.protocolVersion, int))
        self.failUnless(c.protocolVersion >= 2)
        self.failUnless(isinstance(c.serverVersion, int))
        self.failUnless(c.serverVersion >= 70000)
        self.failUnless(not c.closed)
        self.assertEqual(c.transactionStatus, ocpgdb.TRANS_IDLE)
        old_verb = c.setErrorVerbosity(ocpgdb.ERRORS_VERBOSE)
        self.assertEqual(old_verb, ocpgdb.ERRORS_DEFAULT)
        old_verb = c.setErrorVerbosity(ocpgdb.ERRORS_DEFAULT)
        self.assertEqual(old_verb, ocpgdb.ERRORS_VERBOSE)
        c.close()
        self.failUnless(c.closed)
        self.assertRaises(ocpgdb.ProgrammingError, getattr, c, 'host')
        self.assertRaises(ocpgdb.ProgrammingError, getattr, c, 'port')
        self.assertRaises(ocpgdb.ProgrammingError, getattr, c, 'db')
        self.assertRaises(ocpgdb.ProgrammingError, getattr, c, 'user')
        self.assertRaises(ocpgdb.ProgrammingError, getattr, c, 'password')
        self.assertRaises(ocpgdb.ProgrammingError, getattr, c, 'options')
        self.assertRaises(ocpgdb.ProgrammingError, c.fileno)
        self.assertRaises(ocpgdb.ProgrammingError, getattr, c, 'protocolVersion')
        self.assertRaises(ocpgdb.ProgrammingError, getattr, c, 'serverVersion')
        self.assertRaises(ocpgdb.ProgrammingError, c.close)

    def test_result(self):
        c = ocpgdb.connect(**scratch_db)
        # Null command
        result = ocpgdb.PgConnection.execute(c, '', ())
        self.assertEqual(result.status, ocpgdb.PGRES_EMPTY_QUERY)
        # Error command
        result = ocpgdb.PgConnection.execute(c, 'nonsense_command', ())
        self.assertEqual(result.status, ocpgdb.PGRES_FATAL_ERROR)
        self.assertEqual(result.errorMessage, 
            'ERROR:  syntax error at or near "nonsense_command"\n'
            'LINE 1: nonsense_command\n'
            '        ^\n')
        self.assertEqual(result.errorField(ocpgdb.DIAG_SEVERITY), 'ERROR')
        self.assertEqual(result.errorField(ocpgdb.DIAG_SQLSTATE), '42601')
        self.assertEqual(result.errorField(ocpgdb.DIAG_MESSAGE_PRIMARY), 
                            'syntax error at or near "nonsense_command"')
        self.assertEqual(result.errorField(ocpgdb.DIAG_MESSAGE_DETAIL), None)
        self.assertEqual(result.errorField(ocpgdb.DIAG_MESSAGE_HINT), None)
        self.assertEqual(result.errorField(ocpgdb.DIAG_STATEMENT_POSITION), '1')
        self.assertEqual(result.errorField(ocpgdb.DIAG_INTERNAL_POSITION), None)
        self.assertEqual(result.errorField(ocpgdb.DIAG_INTERNAL_QUERY), None)
        self.assertEqual(result.errorField(ocpgdb.DIAG_CONTEXT), None)
        # Simple select
        result = ocpgdb.PgConnection.execute(c, 'select null', ())
        self.assertEqual(result.status, ocpgdb.PGRES_TUPLES_OK)
        rows = list(result)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(len(row), 1)
        cell = row[0]
        self.assertEqual(cell.format, 1)
        self.assertEqual(cell.modifier, -1)
        self.assertEqual(cell.name, '?column?')
        self.assertEqual(cell.type, ocpgdb.pgoid.unknown)
        self.assertEqual(cell.value, None)


class BasicSuite(unittest.TestSuite):
    tests = [
        'test_module_const',
        'test_connect',
        'test_result',
    ]
    def __init__(self):
        unittest.TestSuite.__init__(self, map(BasicTests, self.tests))


class ConversionTestCase(unittest.TestCase):
    connect_args = scratch_db
    equal = operator.eq
    pgtype = None
    values = []
    exceptions = []

    def exone(self, db, cmd, *args):
        rows = list(db.execute(cmd, *args))
        self.assertEqual(len(rows), 1)
        self.assertEqual(len(rows[0]), 1)
        return rows[0][0]

    def fromdb(self, pyval, pgstr=None):
        """
        Test conversion from an SQL literal to a python type
        """
        if pgstr is None:
            pgstr = str(pyval)
        got = self.exone(self.db, "select %r::%s" % (pgstr, self.pgtype))
        self.failUnless(self.equal(pyval, got),
                        "from pg val %s::%s, expected %r, got %r" %
                            (pgstr, self.pgtype, pyval, got))

    def _todb(self, pyval):
        return self.exone(self.db, "select %s::" + self.pgtype, pyval)

    def roundtrip(self, value, expect=None):
        """
        Test conversion round trip from py to pg and back to py
        """
        if expect is None:
            expect = value
        got = self._todb(value)
        self.failUnless(self.equal(got, expect),
                        "round trip, %s type, expected %r, got %r" % 
                            (self.pgtype, expect, got))

    def both(self, value, *args):
        """
        Test both conversion from SQL and round trip
        """
        pgstr = None
        expect = value
        if len(args) == 1:
            pgstr = args[0]
        elif len(args) == 2:
            pgstr, expect = args
        self.fromdb(expect, pgstr)
        self.roundtrip(value, expect)

    def errorval(self, exc, pyval):
        """
        Check that a value conversion results in an exception being raised
        """
        self.assertRaises(exc, self._todb, pyval)

    def setUp(self):
        self.db = ocpgdb.connect(**self.connect_args)

    def tearDown(self):
        self.db.close()


class BoolConversion(ConversionTestCase):
    pgtype = 'bool'

    def runTest(self):
        self.fromdb(True, 'true')
        self.fromdb(False, 'false')
        self.roundtrip(None)
        self.roundtrip(True)
        self.roundtrip(False)


class IntConversion(ConversionTestCase):
    pgtype = 'int'

    def runTest(self):
        self.roundtrip(None)
        # int
        self.both(-1)
        self.both(0)
        self.both(1)
        self.both(0x7FFFFFF)
        self.both(-0x8000000)
        # long
        self.both(-1L)
        self.both(0L)
        self.both(1L)
        self.both(0x7FFFFFFL)
        self.both(-0x8000000L)



class Int2Conversion(ConversionTestCase):
    pgtype = 'int2'

    def runTest(self):
        self.roundtrip(None)
        self.both(-1)
        self.both(0)
        self.both(1)
        self.both(0x7fff)
        self.both(-0x8000)
        self.errorval(ocpgdb.OperationalError, 0xffff)
        self.errorval(ocpgdb.OperationalError, -0xffff)


class Int8Conversion(ConversionTestCase):
    pgtype = 'int8'

    def runTest(self):
        self.roundtrip(None)
        self.both(-1)
        self.both(-1L)
        self.both(0)
        self.both(0L)
        self.both(1L)
        self.both(0x7FFFFFFFFFFFFFFFL)
        self.both(-0x8000000000000000L)
        if sys.maxint > 0x7FFFFFFF:
            self.both(0x7FFFFFFFFFFFFFFF)
            self.both(-0x8000000000000000)


class FloatConversion(ConversionTestCase):
    pgtype = 'float'

    def runTest(self):
        self.roundtrip(None)
        self.both(-1.0)
        self.both(0.0)
        self.both(1.0)
        self.both(1e240) 
        self.both(-1e240)
        self.both(1e-240)
        self.both(-1e-240)


class NumericConversion(ConversionTestCase):
    pgtype = 'numeric'

    def equal(self, a, b):
        if isinstance(a, decimal.Decimal) and isinstance(b, decimal.Decimal):
            return a.as_tuple() == b.as_tuple()
        return a == b

    def runTest(self):
        D = decimal.Decimal
        self.roundtrip(None)
        self.both(D('0'))               # 0 words
        self.both(D('0.0000'))          # 0 words, weight 0, dscale 4
        self.both(D('1'))               # 1 word
        self.both(D('-1'))              # 1 word
        self.both(D('1000'))            # 1 word
        self.both(D('-1000'))           # 1 word, weight 0, dscale 0
        self.both(D('-10.00'))          # 1 word, weight 0, dscale 2
        self.both(D('10000'))           # 1 word, weight 1, dscale 0
        self.both(D('-10000'))          # 1 word, weight 1, dscale 0
        self.both(D('.001'))            # 1 word, weight -1, dscale 3
        self.both(D('-.001'))           # 1 word, weight -1, dscale 3
        self.both(D('.0001'))           # 1 word, weight -1, dscale 4
        self.both(D('-.0001'))          # 1 word, weight -1, dscale 4
        self.both(D('10001'))           # 2 words, weight 1, dscale 0
        self.both(D('-10001'))          # 2 words, weight 1, dscale 0
        self.both(D('1.23'))            # 2 words, weight 0, dscale 2
        self.both(D('-1.23'))           # 2 words, weight 0, dscale 2
        self.both(D('10001.001'))       # 3 words, weight 1, dscale 3
        self.both(D('-10001.001'))      # 3 words, weight 1, dscale 3
        self.both(D('10001.0001'))      # 3 words, weight 1, dscale 4
        self.both(D('-10001.0001'))     # 3 words, weight 1, dscale 4
        self.both(D('10001.000001'))    # 3 words, weight 1, dscale 6
        self.both(D('-10001.000001'))   # 3 words, weight 1, dscale 6
        self.both(D('1e1000'))          # 1 word, weight 250, dscale 0
        self.both(D('-1e1000'))         # 1 word, weight 250, dscale 0
        self.both(D('1e-1000'))         # 1 word, weight -250, dscale 1000
        self.both(D('-1e-1000'))        # 1 word, weight -250, dscale 1000
        self.both(D('NaN'))
        self.errorval(ocpgdb.DataError, D('Infinity'))
        self.errorval(ocpgdb.DataError, D('-Infinity'))


class TextConversion(ConversionTestCase):
    pgtype = 'text'

    def runTest(self):
        self.roundtrip(None)
        self.both('')
        self.both('\'\"\x01')
        self.both('A' * 65536)


class VarcharConversion(ConversionTestCase):
    pgtype = 'varchar'

    def runTest(self):
        self.roundtrip(None)
        self.both('')
        self.both('\'\"\x01')
        self.both('A' * 65536)


class Varchar5Conversion(ConversionTestCase):
    pgtype = 'varchar(5)'

    def runTest(self):
        self.roundtrip(None)
        self.both('')
        self.both('\'\"\x01')
        self.roundtrip('A' * 65536, 'A' * 5)


class Char5Conversion(ConversionTestCase):
    pgtype = 'char(5)'

    def runTest(self):
        self.roundtrip(None)
        self.roundtrip('', ' ' * 5)
        self.roundtrip('\'\"\x01', '\'\"\x01  ')
        self.roundtrip('A' * 65536, 'A' * 5)


class ByteaConversion(ConversionTestCase):
    pgtype = 'bytea'

    def runTest(self):
        self.roundtrip(None)
        self.roundtrip(ocpgdb.bytea(''))
        data = ocpgdb.bytea(''.join([chr(i) for i in range(256)]))
        self.roundtrip(data)


class PyTimestampConversion(ConversionTestCase):
    pgtype = 'timestamp'

    def runTest(self):
        self.roundtrip(None)
        self.both(datetime.datetime(2007,5,8,15,9,32,23))
        self.both(datetime.datetime(1900,5,8,15,9,32,23))
        self.both(datetime.datetime(2200,5,8,15,9,32,23))


class PyTimeConversion(ConversionTestCase):
    pgtype = 'time'

    def runTest(self):
        self.roundtrip(None)
        self.both(datetime.time(15,9,32,23))
        self.both(datetime.time(0,0,0,0))
        self.both(datetime.time(23,59,59,999))


class PyDateConversion(ConversionTestCase):
    pgtype = 'date'

    def runTest(self):
        self.roundtrip(None)
        self.both(datetime.date(2007,5,8))
        self.both(datetime.date(1900,5,8))
        self.both(datetime.date(2200,5,8))


# Can't support "interval" semantics with python datetime module
#class PyIntervalConversion(ConversionTestCase):
#    pgtype = 'interval'
#
#    def runTest(self):
#        pass


class MxTestConversion(ConversionTestCase):
    connect_args = dict(use_mx_datetime=True, **scratch_db)

    def equal(self, a, b):
        # mx.DateTime has a precision of 10ms, so we need to account for
        # rounding errors.
        datetime_types = DateTime.DateTimeType, DateTime.DateTimeDeltaType
        if isinstance(a, datetime_types) and isinstance(b, datetime_types):
            return DateTime.cmp(a, b, 0.01) == 0
        return a == b


class MxTimestampConversion(MxTestConversion):
    pgtype = 'timestamp'

    def runTest(self):
        self.roundtrip(None)
        self.both(DateTime.DateTime(2007,5,8,15,9,32.24))
        self.both(DateTime.DateTime(1900,5,8,15,9,32.24))
        self.both(DateTime.DateTime(2200,5,8,15,9,32.24))


class MxTimeConversion(MxTestConversion):
    pgtype = 'time'

    def runTest(self):
        self.roundtrip(None)
        self.both(DateTime.Time(15,9,32.23))
        self.both(DateTime.Time(0,0,0.0))
        self.both(DateTime.Time(23,59,59.99))


class MxDateConversion(MxTestConversion):
    pgtype = 'date'

    def runTest(self):
        self.roundtrip(None)
        self.both(DateTime.Date(2007,5,8))
        self.both(DateTime.Date(1900,5,8))
        self.both(DateTime.Date(2200,5,8))


class MxIntervalConversion(MxTestConversion):
    pgtype = 'interval'

    def runTest(self):
        rd = DateTime.RelativeDateTime
        self.roundtrip(None)
        self.roundtrip(rd())
        self.both(rd(seconds=1), '1 second')
        self.both(rd(seconds=-1), '-1 second')
        self.both(rd(days=1), '1 day')
        self.both(rd(days=-1), '-1 day')
        self.both(rd(months=1), '1 month')
        self.both(rd(months=-1), '-1 month')
        self.both(rd(years=1), '1 year')
        self.both(rd(years=-1), '-1 year')
        self.both(rd(minutes=1, seconds=1), '1 minute 1 second')
        self.both(rd(minutes=-1, seconds=1), '-1 min 1 sec', rd(seconds=-59)) 
        self.both(rd(minutes=1, seconds=-1), '1 min -1 sec', rd(seconds=59))
        self.both(rd(minutes=-1, seconds=-1), '-1 minute -1 seconds')
        self.both(rd(years=1, seconds=1), '1 year, 1 second')
        self.both(rd(years=-1, seconds=1), '-1 year, 1 second')
        self.both(rd(years=1, seconds=-1), '1 year, -1 second')
        self.both(rd(years=-1, seconds=-1), '-1 year, -1 second')


class ConversionSuite(unittest.TestSuite):
    tests = [
        BoolConversion,
        IntConversion,
        Int2Conversion,
        Int8Conversion,
        FloatConversion,
        TextConversion,
        VarcharConversion,
        Varchar5Conversion,
        Char5Conversion,
        ByteaConversion,
        PyTimestampConversion,
        PyTimeConversion,
        PyDateConversion,
    ]
    mx_tests = [
        MxTimestampConversion,
        MxTimeConversion,
        MxDateConversion,
        MxIntervalConversion,
    ]
    def __init__(self):
        unittest.TestSuite.__init__(self)
        for test in self.tests:
            self.addTest(test())
        if have_decimal:
            self.addTest(NumericConversion())
        if have_mx:
            for test in self.mx_tests:
                self.addTest(test())


class OCPGDBSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self)
        self.addTest(BasicSuite())
        self.addTest(ConversionSuite())


suite = OCPGDBSuite

if __name__ == '__main__':
    unittest.main(defaultTest='suite')
