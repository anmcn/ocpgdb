import os
import sys
import unittest
import operator
import datetime

del sys.path[0]
import ocpgdb
print 'WARNING: testing %s' % os.path.dirname(ocpgdb.__file__)

scratch_db = dict(dbname='ocpgdb_test', port=5432)

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
try:
    import ipaddress
    have_ipaddress = True
except ImportError:
    try:
        import ipaddr as ipaddress
        have_ipaddress = True
        scratch_db['use_ipaddr'] = True
        print 'WARNING: using ipaddr rather than ipaddress module'
    except ImportError:
        print 'WARNING: neither ipaddress nor ipaddr modules are available, tests skipped'
        have_ipaddress = False



class TestCase(unittest.TestCase):

    def assertHasAttr(self, o, attr):
        self.assertTrue(hasattr(o, attr), 'no attribute %r' % attr)

    def assertSubClass(self, sb, c):
        self.assertTrue(issubclass(sb, c), 
                        '%r is not a subclass of %r' % (sb, c))


class BasicTests(TestCase):
    def test_module_const(self):
        mandatory_attrs = (
            # General info:
            '__version__', 
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
            self.assertHasAttr(ocpgdb, attr) 

    def test_connect(self):
        c = ocpgdb.connect(**scratch_db)
        self.assertTrue(c.fileno() >= 0)
        self.assertTrue(isinstance(c.conninfo, str))
        self.assertEqual(c.notices, [])
        self.assertTrue(isinstance(c.host, str))
        self.assertTrue(isinstance(c.port, int))
        self.assertTrue(isinstance(c.db, str))
        self.assertTrue(isinstance(c.user, str))
        self.assertTrue(isinstance(c.password, str))
        self.assertTrue(isinstance(c.options, str))
        self.assertTrue(isinstance(c.protocolVersion, int))
        self.assertTrue(c.protocolVersion >= 2)
        self.assertTrue(isinstance(c.serverVersion, int))
        self.assertTrue(c.serverVersion >= 70000)
        self.assertTrue(not c.closed)
        self.assertEqual(c.transactionStatus, ocpgdb.TRANS_IDLE)
        old_verb = c.setErrorVerbosity(ocpgdb.ERRORS_VERBOSE)
        self.assertEqual(old_verb, ocpgdb.ERRORS_DEFAULT)
        old_verb = c.setErrorVerbosity(ocpgdb.ERRORS_DEFAULT)
        self.assertEqual(old_verb, ocpgdb.ERRORS_VERBOSE)
        c.close()
        self.assertTrue(c.closed)
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


class ConversionTestCase(TestCase):
    connect_args = scratch_db
    equal = operator.eq
    pgtype = None
    values = []
    exceptions = []

    def exone(self, db, cmd, *args):
        rows = list(db.execute(cmd, args))
        self.assertEqual(len(rows), 1)
        self.assertEqual(len(rows[0]), 1)
        return rows[0][0]

    def fromdb(self, pyval, pgstr=None):
        """
        Test conversion from an SQL literal to a python type
        """
        if pgstr is None:
            pgstr = str(pyval)
        got = self.exone(self.db, "select '%s'::%s" % (pgstr, self.pgtype))
        self.assertTrue(self.equal(pyval, got),
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
        self.assertTrue(self.equal(got, expect),
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
        self.both('\'"\x01', '\'\'"\x01')
        self.both('A' * 65536)


class VarcharConversion(ConversionTestCase):
    pgtype = 'varchar'

    def runTest(self):
        self.roundtrip(None)
        self.both('')
        self.both('\'\"\x01', '\'\'"\x01')
        self.both('A' * 65536)


class Varchar5Conversion(ConversionTestCase):
    pgtype = 'varchar(5)'

    def runTest(self):
        self.roundtrip(None)
        self.both('')
        self.both('\'\"\x01', '\'\'"\x01')
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


class IpAddressConversion(ConversionTestCase):
    pgtype = 'inet'

    def runTest(self):
        self.roundtrip(None)
        self.both(ipaddress.IPv4Address('0.0.0.0'))
        self.both(ipaddress.IPv4Address('255.255.255.255'))
        self.both(ipaddress.IPv4Address('127.0.0.1'))
        self.both(ipaddress.IPv6Address('::1'))
        self.both(ipaddress.IPv6Address('2001:db8:85a3::8a2e:370:7334'))
        self.both(ipaddress.IPv6Address('::ffff:192.0.2.128'))


class IpNetworkConversion(ConversionTestCase):
    pgtype = 'cidr'

    def runTest(self):
        self.roundtrip(None)
        self.both(ipaddress.IPv4Network('0.0.0.0'))
        self.both(ipaddress.IPv4Network('255.255.255.255'))
        self.both(ipaddress.IPv4Network('127.0.0.1'))
        self.both(ipaddress.IPv6Network('::1'))
        self.both(ipaddress.IPv6Network('2001:db8:85a3::8a2e:370:7334'))
        self.both(ipaddress.IPv6Network('::ffff:192.0.2.128'))
        self.both(ipaddress.IPv4Network('10.0.0.0/8'))
        self.both(ipaddress.IPv4Network('0.0.0.0/0'))
        self.both(ipaddress.IPv4Network('127.0.0.1/32'))
        self.both(ipaddress.IPv6Network('::1/128'))


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
    ipaddress_tests = [
        IpAddressConversion,
        IpNetworkConversion,
    ]
    def __init__(self):
        unittest.TestSuite.__init__(self)
        for test in self.tests:
            self.addTest(test())
        if have_decimal:
            self.addTest(NumericConversion())
        if have_ipaddress:
            for test in self.ipaddress_tests:
                self.addTest(test())
        if have_mx:
            for test in self.mx_tests:
                self.addTest(test())


class DBAPI2Test(TestCase):

    pass


class DBAPI2Module(DBAPI2Test):

    def runTest(self):
        self.assertEqual(ocpgdb.apilevel, '2.0')
        self.assertEqual(ocpgdb.threadsafety, 1)
        self.assertEqual(ocpgdb.paramstyle, 'pyformat')


class DBAPI2Exceptions(DBAPI2Test):

    def runTest(self):
        self.assertSubClass(ocpgdb.Warning, Exception)
        self.assertSubClass(ocpgdb.Error, Exception)
        self.assertSubClass(ocpgdb.InterfaceError, ocpgdb.Error)
        self.assertSubClass(ocpgdb.DatabaseError, ocpgdb.Error)
        self.assertSubClass(ocpgdb.DataError, ocpgdb.DatabaseError)
        self.assertSubClass(ocpgdb.OperationalError, ocpgdb.DatabaseError)
        self.assertSubClass(ocpgdb.IntegrityError, ocpgdb.DatabaseError)
        self.assertSubClass(ocpgdb.InternalError, ocpgdb.DatabaseError)
        self.assertSubClass(ocpgdb.ProgrammingError, ocpgdb.DatabaseError)
        self.assertSubClass(ocpgdb.NotSupportedError, ocpgdb.DatabaseError)


class DBAPI2Connect(DBAPI2Test):

    def runTest(self):
        db = ocpgdb.connect(**scratch_db)
        self.assertHasAttr(db, 'close')
        self.assertHasAttr(db, 'commit')
        self.assertHasAttr(db, 'rollback')
        self.assertHasAttr(db, 'cursor')
        # After a close, all methods should raise an Error subclass
        db.close()
        self.assertRaises(ocpgdb.Error, db.close)
        self.assertRaises(ocpgdb.Error, db.rollback)
        self.assertRaises(ocpgdb.Error, db.commit)
        self.assertRaises(ocpgdb.Error, db.cursor)


class DBAPI2Cursor(DBAPI2Test):

    def runTest(self):
        db = ocpgdb.connect(**scratch_db)
        curs = db.cursor()
        # Make sure all the bits are there
        self.assertHasAttr(curs, 'description')
        self.assertHasAttr(curs, 'rowcount')
        #self.assertHasAttr(curs, 'callproc') # Optional
        self.assertHasAttr(curs, 'close')
        self.assertHasAttr(curs, 'execute')
        self.assertHasAttr(curs, 'executemany')
        self.assertHasAttr(curs, 'fetchone')
        self.assertHasAttr(curs, 'fetchmany')
        self.assertHasAttr(curs, 'fetchall')
        #self.assertHasAttr(curs, 'nextset') # Optional
        self.assertHasAttr(curs, 'arraysize')
        self.assertHasAttr(curs, 'setinputsizes') # Stub
        self.assertHasAttr(curs, 'setoutputsize') # Stub
        # Now test functionality
        self.assertRaises(ocpgdb.Error, curs.fetchone)
        self.assertEqual(curs.description, None)
        self.assertEqual(curs.rowcount, -1)
        # description
        curs.execute('select null')
        self.assertEqual(curs.description, 
            [('?column?', ocpgdb.pgoid.unknown, None, None, None, None, None)])
        self.assertEqual(curs.rowcount, -1)
        # execute
        curs.execute('select null')
        self.assertEqual(curs.fetchall(), [(None,)])
        curs.execute('select null', ())
        self.assertEqual(curs.fetchall(), [(None,)])
        curs.execute('select null', [])
        self.assertEqual(curs.fetchall(), [(None,)])
        curs.execute('select null', {})
        self.assertEqual(curs.fetchall(), [(None,)])
        curs.execute('select null, 1')
        self.assertEqual(curs.fetchall(), [(None, 1)])
        curs.execute('select %s::int, %s::int', (None, 1))
        self.assertEqual(curs.fetchall(), [(None, 1)])
        curs.execute('select %(a)s::int, %(b)s::int', dict(a=None, b=1))
        self.assertEqual(curs.fetchall(), [(None, 1)])
        self.assertRaises(ocpgdb.Error, curs.execute, 
                          'select %s::int, %s::int')
        db.rollback()
        self.assertRaises(ocpgdb.Error, curs.execute, 
                          'select %s::int, %s::int', None)
        db.rollback()
        self.assertRaises(TypeError, curs.execute, 
                          'select %s::int, %s::int', None, 1)
        db.rollback()
        self.assertRaises(ocpgdb.Error, curs.execute, 
                          'select %s::int, %s::int', (None, None, None))
        db.rollback()
        self.assertRaises(ocpgdb.Error, curs.execute, 
                          'select %s::int, %s::int', dict(a=None, b=1))
        db.rollback()
        self.assertRaises(ocpgdb.Error, curs.execute, 
                          'select %(a)s::int, %(b)s::int', (None, 1))
        db.rollback()
        curs.execute('create table x ( y text )')
        self.assertEqual(curs.rowcount, None)
        self.assertRaises(ocpgdb.Error, curs.fetchall)
        db.rollback()
        # executemany
        pass
        # fetchone
        curs.execute('select null')
        self.assertEqual(curs.fetchone(), (None,))
        self.assertEqual(curs.fetchone(), None)
        # fetchmany
        curs.execute('select null')
        self.assertEqual(curs.fetchmany(), [(None,)])
        self.assertEqual(curs.fetchmany(), [])
        curs.execute('select null')
        self.assertEqual(curs.fetchmany(2), [(None,)])
        self.assertEqual(curs.fetchmany(2), [])
        # fetchall
        curs.execute('select null')
        self.assertEqual(curs.fetchall(), [(None,)])
        self.assertEqual(curs.fetchall(), [])
        # multiple rows
        curs.execute('create table x ( y text )')
        curs.execute('insert into x values (%s)', 'a')
        curs.execute('insert into x values (%s)', 'b')
        curs.execute('insert into x values (%s)', 'c')
        curs.execute('select * from x')
        self.assertEqual(curs.fetchall(), [('a',), ('b',), ('c',)])
        curs.execute('select * from x')
        self.assertEqual(curs.fetchmany(), [('a',)])
        self.assertEqual(curs.fetchmany(), [('b',)])
        self.assertEqual(curs.fetchmany(), [('c',)])
        self.assertEqual(curs.fetchmany(), [])
        curs.execute('select * from x')
        self.assertEqual(curs.fetchmany(2), [('a',), ('b',)])
        self.assertEqual(curs.fetchmany(2), [('c',)])
        self.assertEqual(curs.fetchmany(2), [])
        self.assertEqual(curs.arraysize, 1)
        curs.arraysize = 2
        self.assertEqual(curs.arraysize, 2)
        curs.execute('select * from x')
        self.assertEqual(curs.fetchmany(), [('a',), ('b',)])
        self.assertEqual(curs.fetchmany(), [('c',)])
        self.assertEqual(curs.fetchmany(), [])
        db.rollback()
        curs = db.cursor()
        # multiple rows and cols
        curs.execute('create table x ( y text, z int )')
        curs.executemany('insert into x values (%s, %s)', 
                    [('a', 1), ('b', 1), ('c', 1)])
        curs.execute('select * from x')
        self.assertEqual(curs.fetchall(), 
                    [('a', 1), ('b', 1), ('c', 1)])
        db.rollback()
        curs = db.cursor()
        curs.execute('create table x ( y text, z int )')
        curs.executemany('insert into x values (%(a)s, %(b)s)', 
                    [dict(a='a', b=1), dict(a='b', b=1), dict(a='c', b=1)])
        curs.execute('select * from x')
        self.assertEqual(curs.fetchall(), 
                    [('a', 1), ('b', 1), ('c', 1)])
        # close
        curs.close()
        self.assertRaises(ocpgdb.Error, curs.close)
        self.assertRaises(ocpgdb.Error, curs.execute, 'select null')
        self.assertRaises(ocpgdb.Error, curs.fetchone)
        self.assertRaises(ocpgdb.Error, curs.fetchmany)
        self.assertRaises(ocpgdb.Error, curs.fetchall)




class DBAPI2Type(DBAPI2Test):

    def assertExecute(self, expect, cmd, *args):
        curs = self.db.cursor()
        try:
            curs.execute(cmd, args)
            rows = curs.fetchall()
        finally:
            curs.close()
        self.assertEqual(len(rows), 1)
        self.assertEqual(len(rows[0]), 1)
        self.assertEqual(expect, rows[0][0])

    def assertRoundTrip(self, type_obj, data):
        curs = self.db.cursor()
        try:
            curs.execute('select %s', [data])
            rows = curs.fetchall()
            self.assertEqual(len(curs.description), 1)
            self.assertEqual(curs.description[0][1], type_obj)
        finally:
            curs.close()
        self.assertEqual(len(rows), 1)
        self.assertEqual(len(rows[0]), 1)
        self.assertEqual(data, rows[0][0])

    def runTest(self):
        self.assertHasAttr(ocpgdb, 'Binary')
        self.assertHasAttr(ocpgdb, 'Date')
        self.assertHasAttr(ocpgdb, 'Time')
        self.assertHasAttr(ocpgdb, 'Timestamp')
        self.assertHasAttr(ocpgdb, 'DateFromTicks')
        self.assertHasAttr(ocpgdb, 'TimestampFromTicks')
        self.assertHasAttr(ocpgdb, 'TimeFromTicks')
        self.assertHasAttr(ocpgdb, 'STRING')
        self.assertHasAttr(ocpgdb, 'BINARY')
        self.assertHasAttr(ocpgdb, 'NUMBER')
        self.assertHasAttr(ocpgdb, 'DATETIME')
        self.assertHasAttr(ocpgdb, 'ROWID')
        self.db = ocpgdb.connect(**scratch_db)
        self.assertExecute(None, 'select null')
        self.assertRoundTrip(ocpgdb.DATETIME, ocpgdb.Date(2010,5,11))
        self.assertRoundTrip(ocpgdb.DATETIME, ocpgdb.Time(13,14,15))
        self.assertRoundTrip(ocpgdb.DATETIME, 
                             ocpgdb.Timestamp(2010,5,11, 13,14,15))
        self.assertRoundTrip(ocpgdb.DATETIME, 
                             ocpgdb.DateFromTicks(1273547655))
        self.assertRoundTrip(ocpgdb.DATETIME, 
                             ocpgdb.TimeFromTicks(1273547655))
        self.assertRoundTrip(ocpgdb.DATETIME, 
                             ocpgdb.TimestampFromTicks(1273547655))
        self.assertRoundTrip(ocpgdb.BINARY, ocpgdb.Binary('abc\0'))
        self.assertRoundTrip(ocpgdb.STRING, 'abc')
        self.assertRoundTrip(ocpgdb.NUMBER, 1)
        self.assertRoundTrip(ocpgdb.NUMBER, 1.1)
        # No ROWID tests at this time


class DBAPI2Suite(unittest.TestSuite):

    tests = [
        DBAPI2Module,
        DBAPI2Exceptions,
        DBAPI2Connect,
        DBAPI2Cursor,
        DBAPI2Type,
    ]

    def __init__(self):
        unittest.TestSuite.__init__(self)
        for test in self.tests:
            self.addTest(test())


class OCPGDBSuite(unittest.TestSuite):

    def __init__(self):
        unittest.TestSuite.__init__(self)
        self.addTest(BasicSuite())
        self.addTest(ConversionSuite())
        self.addTest(DBAPI2Suite())


suite = OCPGDBSuite

if __name__ == '__main__':
    unittest.main(defaultTest='suite')
