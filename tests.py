import sys
import unittest
import ocpgdb
try:
    from mx import DateTime
    have_mx = True
except ImportError:
    have_mx = False

scratch_db = dict(dbname='ocpgdb_test', port=5433)


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


class BasicSuite(unittest.TestSuite):
    tests = [
        'test_module_const',
        'test_connect',
    ]
    def __init__(self):
        unittest.TestSuite.__init__(self, map(BasicTests, self.tests))


class FromDBTests(unittest.TestCase):
    """
    This checks the "from PG" conversions - the "to PG" tests later
    assume the "from PG" conversions are correct.
    """

    def _test(self, db, input, pgtype, expect):
        rows = list(db.execute("select '%s'::%s" % (input, pgtype)))
        self.assertEqual(len(rows), 1)
        self.assertEqual(len(rows[0]), 1)
        value = rows[0][0]
        self.assertEqual(expect, value)

    def test_bool(self):
        db = ocpgdb.connect(**scratch_db)
        try:
            self._test(db, 'true', 'bool', True)
            self._test(db, 'false', 'bool', False)
        finally:
            db.close()

    def test_int(self):
        db = ocpgdb.connect(**scratch_db)
        try:
            self._test(db, '1', 'int', 1)
            self._test(db, str(sys.maxint), 'int', sys.maxint)
            self._test(db, str(-sys.maxint), 'int', -sys.maxint)

            self._test(db, '1', 'int2', 1)

            self._test(db, '1', 'int8', 1)
            maxsq = sys.maxint * sys.maxint
            self._test(db, str(maxsq), 'int8', maxsq)
            self._test(db, str(-maxsq), 'int8', -maxsq)
        finally:
            db.close()

    def test_float(self):
        db = ocpgdb.connect(**scratch_db)
        try:
            self._test(db, '0.0', 'float', 0.0)
            self._test(db, '1.0', 'float', 1.0)
            self._test(db, '1e240', 'float', 1e240)
            self._test(db, '-1e240', 'float', -1e240)
            self._test(db, '1e-240', 'float', 1e-240)
            self._test(db, '-1e-240', 'float', -1e-240)
        finally:
            db.close()

    def test_str(self):
        db = ocpgdb.connect(**scratch_db)
        try:
            aaa = 'A' * 65536
            self._test(db, '', 'text', '')
            self._test(db, aaa, 'text', aaa)

            self._test(db, '', 'varchar', '')
            self._test(db, aaa, 'varchar', aaa)

            self._test(db, '', 'varchar(5)', '')
            self._test(db, aaa, 'varchar(5)', 'A' * 5)

            self._test(db, '', 'char(5)', ' ' * 5)
            self._test(db, aaa, 'char(5)', 'A' * 5)
        finally:
            db.close()

    def test_bytea(self):
        db = ocpgdb.connect(**scratch_db)
        try:
            self._test(db, '', 'bytea', ocpgdb.bytea(''))
        finally:
            db.close()

    def test_decimal(self):
        import decimal
        d = decimal.Decimal
        db = ocpgdb.connect(**scratch_db)
        values = [
            '0',                # 0 words
            '0.0000',           # 0 words, weight 0, dscale 4
            '1',                # 1 word
            '1000',             # 1 word
            '10000',            # 1 word, weight 1, dscale 0
            '.001',             # 1 word, weight -1, dscale 3
            '.0001',            # 1 word, weight -1, dscale 4
            '10001',            # 2 words, weight 1, dscale 0
            '10001.001',        # 3 words, weight 1, dscale 3
            '10001.0001',       # 3 words, weight 1, dscale 4
            '1e1000',           # 1 word, weight 250, dscale 0
            '1e-1000',          # 1 word, weight -250, dscale 1000
        ]
        try:
            for value in values:
                self._test(db, value, 'numeric', decimal.Decimal(value))
                self._test(db, '-'+value, 'numeric', decimal.Decimal('-'+value))
# equality doesn't work with NaN, so we have to do it explicitly
#            self._test(db, 'NaN', 'numeric', decimal.Decimal('NaN'))
            nan = list(db.execute("select 'NaN'::numeric"))[0][0]
            self.failUnless(isinstance(nan, decimal.Decimal))
            self.assertEqual(str(nan), 'NaN')
        finally:
            db.close()

    def test_py_datetime(self):
        import datetime
        db = ocpgdb.connect(**scratch_db)
        try:
            self._test(db, '2007-5-8 15:9:32.23', 'timestamp', 
                        datetime.datetime(2007,5,8,15,9,32,230000))
            self._test(db, '1900-05-08 15:09:32.23', 'timestamp', 
                        datetime.datetime(1900,5,8,15,9,32,230000))
            self._test(db, '2200-05-08 15:09:32.23', 'timestamp', 
                        datetime.datetime(2200,5,8,15,9,32,230000))
        finally:
            db.close()

    def test_py_time(self):
        import datetime
        db = ocpgdb.connect(**scratch_db)
        try:
            self._test(db, '15:9:32.23', 'time', datetime.time(15,9,32,230000))
            self._test(db, '0:0:0.0', 'time', datetime.time(0,0,0,0))
            self._test(db, '23:59:59.999', 'time', 
                        datetime.time(23,59,59,999000))
        finally:
            db.close()

    def test_py_date(self):
        import datetime
        db = ocpgdb.connect(**scratch_db)
        try:
            self._test(db, '2007-5-8', 'date', datetime.date(2007,5,8))
            self._test(db, '1900-5-8', 'date', datetime.date(1900,5,8))
            self._test(db, '2200-5-8', 'date', datetime.date(2200,5,8))
        finally:
            db.close()

    def _test_mx(self, db, input, pgtype, expect):
        # mx.DateTime has a precision of 10ms, so we need to account for
        # rounding errors.
        rows = list(db.execute("select '%s'::%s" % (input, pgtype)))
        self.assertEqual(len(rows), 1)
        self.assertEqual(len(rows[0]), 1)
        value = rows[0][0]
        self.failIf(DateTime.cmp(expect, value, 0.01), 
                    'expect %s, got %s' % (expect, value))

    def test_mx_datetime(self):
        db = ocpgdb.connect(use_mx_datetime=True, **scratch_db)
        try:
            self._test_mx(db, '2007-5-8 15:9:32.24', 'timestamp', 
                            DateTime.DateTime(2007,5,8,15,9,32.24))
            self._test_mx(db, '1900-5-8 15:9:32.24', 'timestamp', 
                            DateTime.DateTime(1900,5,8,15,9,32.24)) 
            self._test_mx(db, '2200-5-8 15:9:32.24', 'timestamp', 
                            DateTime.DateTime(2200,5,8,15,9,32.24)) 
        finally:
            db.close()

    def test_mx_time(self):
        db = ocpgdb.connect(use_mx_datetime=True, **scratch_db)
        try:
            self._test_mx(db, '15:9:32.24', 'time', DateTime.Time(15,9,32.23))
            self._test_mx(db, '0:0:0.0', 'time', DateTime.Time(0,0,0.0))
            self._test_mx(db, '23:59:59.99', 'time', DateTime.Time(23,59,59.99))
        finally:
            db.close()

    def test_mx_date(self):
        db = ocpgdb.connect(use_mx_datetime=True, **scratch_db)
        try:
            self._test_mx(db, '2007-5-8', 'date',  DateTime.Date(2007,5,8))
            self._test_mx(db, '1900-5-8', 'date', DateTime.Date(1900,5,8))
            self._test_mx(db, '2200-5-8', 'date', DateTime.Date(2200,5,8))
        finally:
            db.close()

    def test_mx_interval(self):
        db = ocpgdb.connect(use_mx_datetime=True, **scratch_db)
        try:
            rd = DateTime.RelativeDateTime
            self._test(db, '1 second', 'interval', rd(seconds=1))
            self._test(db, '-1 second', 'interval', rd(seconds=-1))
            self._test(db, '1 day', 'interval', rd(days=1))
            self._test(db, '-1 day', 'interval', rd(days=-1))
            self._test(db, '1 month', 'interval', rd(months=1))
            self._test(db, '-1 month', 'interval', rd(months=-1))
            self._test(db, '1 year', 'interval', rd(years=1))
            self._test(db, '-1 year', 'interval', rd(years=-1))
            self._test(db, '1 minute 1 second', 'interval', 
                            rd(minutes=1, seconds=1))
            self._test(db, '-1 minute 1 second', 'interval', 
                            rd(seconds=-59))
            self._test(db, '1 minute, -1 second', 'interval', 
                            rd(seconds=59))
            self._test(db, '-1 minute, -1 seconds', 'interval', 
                            rd(minutes=-1, seconds=-1))
            self._test(db, '1 year, 1 second', 'interval', 
                            rd(years=1, seconds=1))
            self._test(db, '-1 year, 1 second', 'interval', 
                            rd(years=-1, seconds=1))
            self._test(db, '1 year, -1 second', 'interval', 
                            rd(years=1, seconds=-1))
            self._test(db, '-1 year, -1 second', 'interval', 
                            rd(years=-1, seconds=-1))
        finally:
            db.close()

class FromDBSuite(unittest.TestSuite):
    tests = [
        'test_bool',
        'test_int',
        'test_float',
        'test_str',
        'test_bytea',
        'test_decimal',
        'test_py_datetime',
        'test_py_time',
        'test_py_date',
#        'test_py_interval',
    ]
    if have_mx:
        tests.extend([
        'test_mx_datetime',
        'test_mx_time',
        'test_mx_date',
        'test_mx_interval',
        ])
    def __init__(self):
        unittest.TestSuite.__init__(self, map(FromDBTests, self.tests))


class ConversionTests(unittest.TestCase):

    def _test(self, db, input, pgtype, expect=None, expect_type=None):
        if expect is None:
            expect = input
        if expect_type is None:
            expect_type = type(expect)
        rows = list(db.execute("select %s::" + pgtype, input))
        self.assertEqual(len(rows), 1)
        self.assertEqual(len(rows[0]), 1)
        value = rows[0][0]
#        self.failUnless(isinstance(value, expect_type),
#                        'Expected type %s, got %s' % (expect_type.__name__,
#                                                      type(value).__name__))
        self.assertEqual(expect, value)

    def test_bool(self):
        db = ocpgdb.connect(**scratch_db)
        try:
            self._test(db, None, 'bool')
            self._test(db, True, 'bool')
            self._test(db, False, 'bool')
        finally:
            db.close()

    def test_int(self):
        db = ocpgdb.connect(**scratch_db)
        try:
            self._test(db, None, 'int')
            self._test(db, 1, 'int')
            self._test(db, sys.maxint, 'int')
            self._test(db, -sys.maxint, 'int')

            self._test(db, None, 'int2')
            self._test(db, 1, 'int2')
            self.assertRaises(ocpgdb.OperationalError, self._test, 
                                db, sys.maxint, 'int2')
            self.assertRaises(ocpgdb.OperationalError, self._test, 
                                db, -sys.maxint, 'int2')

            self._test(db, None, 'int8')
            self._test(db, 1, 'int8')
            self._test(db, sys.maxint * sys.maxint, 'int8')
            self._test(db, -(sys.maxint * sys.maxint), 'int8')
        finally:
            db.close()

    def test_float(self):
        db = ocpgdb.connect(**scratch_db)
        try:
            self._test(db, None, 'float')
            self._test(db, 0.0, 'float')
            self._test(db, 1.0, 'float')
            self._test(db, 1e240, 'float')
            self._test(db, -1e240, 'float')
            self._test(db, 1e-240, 'float')
            self._test(db, -1e-240, 'float')
        finally:
            db.close()

    def test_str(self):
        db = ocpgdb.connect(**scratch_db)
        try:
            self._test(db, None, 'text')
            self._test(db, '', 'text')
            self._test(db, '\'\"\x01', 'text')
            self._test(db, 'A' * 65536, 'text')

            self._test(db, None, 'varchar')
            self._test(db, '', 'varchar')
            self._test(db, '\'\"\x01', 'varchar')
            self._test(db, 'A' * 65536, 'varchar')

            self._test(db, None, 'varchar(5)')
            self._test(db, '', 'varchar(5)')
            self._test(db, '\'\"\x01', 'varchar(5)')
            self._test(db, 'A' * 65536, 'varchar(5)', 'A' * 5)

            self._test(db, None, 'char(5)')
            self._test(db, '', 'char(5)', ' ' * 5)
            self._test(db, '\'\"\x01', 'char(5)', '\'\"\x01  ')
            self._test(db, 'A' * 65536, 'char(5)', 'A' * 5)
        finally:
            db.close()

    def test_bytea(self):
        db = ocpgdb.connect(**scratch_db)
        try:
            self._test(db, None, 'bytea')
            self._test(db, ocpgdb.bytea(''), 'bytea')
            data = ocpgdb.bytea(''.join([chr(i) for i in range(256)]))
            self._test(db, data, 'bytea')
        finally:
            db.close()

# Currently broken
#    def test_decimal(self):
#        import decimal
#        db = ocpgdb.connect(**scratch_db)
#        try:
#            self._test(db, None, 'numeric')
#            self._test(db, decimal.Decimal('0'), 'numeric')
#            self._test(db, decimal.Decimal('0.0000'), 'numeric')
#            self._test(db, decimal.Decimal('0.000000000000000000000000000000000001'), 'numeric')
##            self._test(db, decimal.Decimal('NaN'), 'numeric')
#        finally:
#            db.close()

    def test_py_datetime(self):
        import datetime
        db = ocpgdb.connect(**scratch_db)
        try:
            self._test(db, None, 'timestamp')
            self._test(db, datetime.datetime(2007,5,8,15,9,32,23), 'timestamp')
            self._test(db, datetime.datetime(1900,5,8,15,9,32,23), 'timestamp')
            self._test(db, datetime.datetime(2200,5,8,15,9,32,23), 'timestamp')
        finally:
            db.close()

    def test_py_time(self):
        import datetime
        db = ocpgdb.connect(**scratch_db)
        try:
            self._test(db, None, 'time')
            self._test(db, datetime.time(15,9,32,23), 'time')
            self._test(db, datetime.time(0,0,0,0), 'time')
            self._test(db, datetime.time(23,59,59,999), 'time')
        finally:
            db.close()

    def test_py_date(self):
        import datetime
        db = ocpgdb.connect(**scratch_db)
        try:
            self._test(db, None, 'date')
            self._test(db, datetime.date(2007,5,8), 'date')
            self._test(db, datetime.date(1900,5,8), 'date')
            self._test(db, datetime.date(2200,5,8), 'date')
        finally:
            db.close()


    if 0:
        def test_py_interval(self):
            import datetime
            db = ocpgdb.connect(**scratch_db)
            try:
                self._test(db, None, 'interval')
                self._test(db, datetime.timedelta(0,0,0,0,0), 'interval')
                self._test(db, datetime.timedelta(0,0,0,0,0), 'interval')
                self._test(db, datetime.timedelta(0,59,0,0,59,23), 'interval')
                self._test(db, datetime.timedelta(seconds=-1), 'interval')
                self._test(db, datetime.timedelta(days=-1), 'interval')
                self._test(db, datetime.timedelta(days=-1, seconds=1), 'interval')
                self._test(db, datetime.timedelta(days=-1, seconds=-1), 'interval')
            finally:
                db.close()

    def _test_mx(self, db, input, pgtype, expect=None, expect_type=None):
        # mx.DateTime has a precision of 10ms, so we need to account for
        # rounding errors.
        if expect is None:
            expect = input
        rows = list(db.execute("select %s::" + pgtype, input))
        self.assertEqual(len(rows), 1)
        self.assertEqual(len(rows[0]), 1)
        value = rows[0][0]
        if expect is None and value is None:
            return
        self.failIf(DateTime.cmp(expect, value, 0.01), 
                    'expect %s, got %s' % (expect, value))

    def test_mx_datetime(self):
        db = ocpgdb.connect(use_mx_datetime=True, **scratch_db)
        try:
            self._test_mx(db, None, 'timestamp')
            self._test_mx(db, DateTime.DateTime(2007,5,8,15,9,32.24), 
                            'timestamp')
            self._test_mx(db, DateTime.DateTime(1900,5,8,15,9,32.24), 
                            'timestamp')
            self._test_mx(db, DateTime.DateTime(2200,5,8,15,9,32.24), 
                            'timestamp')
        finally:
            db.close()

    def test_mx_time(self):
        db = ocpgdb.connect(use_mx_datetime=True, **scratch_db)
        try:
            self._test_mx(db, None, 'time')
            self._test_mx(db, DateTime.Time(15,9,32.23), 'time')
            self._test_mx(db, DateTime.Time(0,0,0.0), 'time')
            self._test_mx(db, DateTime.Time(23,59,59.99), 'time')
        finally:
            db.close()

    def test_mx_date(self):
        db = ocpgdb.connect(use_mx_datetime=True, **scratch_db)
        try:
            self._test_mx(db, None, 'date')
            self._test_mx(db, DateTime.Date(2007,5,8), 'date')
            self._test_mx(db, DateTime.Date(1900,5,8), 'date')
            self._test_mx(db, DateTime.Date(2200,5,8), 'date')
        finally:
            db.close()

    def test_mx_interval(self):
        db = ocpgdb.connect(use_mx_datetime=True, **scratch_db)
        try:
            rd = DateTime.RelativeDateTime
            self._test(db, None, 'interval')
            self._test(db, rd(), 'interval')
            self._test(db, rd(seconds=1), 'interval')
            self._test(db, rd(seconds=-1), 'interval')
            self._test(db, rd(days=1), 'interval')
            self._test(db, rd(days=-1), 'interval')
            self._test(db, rd(months=1), 'interval')
            self._test(db, rd(months=-1), 'interval')
            self._test(db, rd(years=1), 'interval')
            self._test(db, rd(years=-1), 'interval')
            self._test(db, rd(minutes=1, seconds=1), 'interval')
            self._test(db, rd(minutes=-1, seconds=1), 'interval',
                           rd(seconds=-59))
            self._test(db, rd(minutes=1, seconds=-1), 'interval',
                           rd(seconds=59))
            self._test(db, rd(minutes=-1, seconds=-1), 'interval')
            self._test(db, rd(years=1, seconds=1), 'interval')
            self._test(db, rd(years=-1, seconds=1), 'interval')
            self._test(db, rd(years=1, seconds=-1), 'interval')
            self._test(db, rd(years=-1, seconds=-1), 'interval')
        finally:
            db.close()

class ConversionSuite(unittest.TestSuite):
    tests = [
        'test_bool',
        'test_int',
        'test_float',
        'test_str',
        'test_bytea',
#        'test_decimal',
        'test_py_datetime',
        'test_py_time',
        'test_py_date',
#        'test_py_interval',
    ]
    if have_mx:
        tests.extend([
        'test_mx_datetime',
        'test_mx_time',
        'test_mx_date',
        'test_mx_interval',
        ])
    def __init__(self):
        unittest.TestSuite.__init__(self, map(ConversionTests, self.tests))


class OCPGDBSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self)
        self.addTest(BasicSuite())
        self.addTest(FromDBSuite())
        self.addTest(ConversionSuite())


suite = OCPGDBSuite

if __name__ == '__main__':
    unittest.main()
