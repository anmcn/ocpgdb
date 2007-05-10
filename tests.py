import sys
import unittest
import ocpgdb

scratch_db = dict(dbname='ocpgdb_test')


class BasicTests(unittest.TestCase):

    def test_module_const(self):
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
        self.failUnless(hasattr(ocpgdb, '__version__'))

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
        c.close()


class BasicSuite(unittest.TestSuite):
    tests = [
        'test_module_const',
        'test_connect',
    ]
    def __init__(self):
        unittest.TestSuite.__init__(self, map(BasicTests, self.tests))


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
        self.failUnless(isinstance(value, expect_type),
                        'Expected type %s, got %s' % (expect_type.__name__,
                                                      type(value).__name__))
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
            self._test(db, '', 'bytea')
            data = ocpgdb.bytea(''.join([chr(i) for i in range(256)]))
            self._test(db, data, 'bytea')
        finally:
            db.close()

    def test_decimal(self):
        import decimal
        db = ocpgdb.connect(**scratch_db)
        try:
            self._test(db, None, 'numeric')
            self._test(db, decimal.Decimal('0'), 'numeric')
            self._test(db, decimal.Decimal('0.0000'), 'numeric')
            self._test(db, decimal.Decimal('0.000000000000000000000000000000000001'), 'numeric')
#            self._test(db, decimal.Decimal('NaN'), 'numeric')
        finally:
            db.close()

    def test_py_dateime(self):
        import datetime
        db = ocpgdb.connect(**scratch_db)
        db.use_python_datetime()
        try:
            self._test(db, None, 'timestamp')
            self._test(db, datetime.datetime(2007,5,8,15,9,32,23), 'timestamp')
            self._test(db, datetime.datetime(1900,5,8,15,9,32,23), 'timestamp')
            self._test(db, datetime.datetime(2200,5,8,15,9,32,23), 'timestamp')

            self._test(db, None, 'time')
            self._test(db, datetime.time(15,9,32,23), 'time')
            self._test(db, datetime.time(0,0,0,0), 'time')
            self._test(db, datetime.time(23,59,59,999), 'time')

            self._test(db, None, 'date')
            self._test(db, datetime.date(2007,5,8), 'date')
            self._test(db, datetime.date(1900,5,8), 'date')
            self._test(db, datetime.date(2200,5,8), 'date')

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


class ConversionSuite(unittest.TestSuite):
    tests = [
        'test_bool',
        'test_int',
        'test_float',
        'test_str',
        'test_bytea',
        'test_decimal',
        'test_py_datetime',
    ]
    def __init__(self):
        unittest.TestSuite.__init__(self, map(ConversionTests, self.tests))


class OCPGDBSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self)
        self.addTest(BasicSuite())
        self.addTest(ConversionSuite())


suite = OCPGDBSuite

if __name__ == '__main__':
    unittest.main()
