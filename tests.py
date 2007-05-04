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
        self.failUnless(isinstance(c.conninfo, str))
        self.assertEqual(c.notices, [])
        self.failUnless(isinstance(c.host, str))
        self.failUnless(isinstance(c.port, int))
        self.failUnless(isinstance(c.db, str))
        self.failUnless(isinstance(c.user, str))
        self.failUnless(isinstance(c.password, str))
        self.failUnless(isinstance(c.options, str))
        self.failUnless(isinstance(c.socket, int))
        self.failUnless(isinstance(c.protocolVersion, int))
        self.failUnless(c.protocolVersion >= 2)
        self.failUnless(isinstance(c.serverVersion, int))
        self.failUnless(c.serverVersion >= 70000)
        c.close()
        self.assertRaises(ocpgdb.ProgrammingError, getattr, c, 'host')
        self.assertRaises(ocpgdb.ProgrammingError, getattr, c, 'port')
        self.assertRaises(ocpgdb.ProgrammingError, getattr, c, 'db')
        self.assertRaises(ocpgdb.ProgrammingError, getattr, c, 'user')
        self.assertRaises(ocpgdb.ProgrammingError, getattr, c, 'password')
        self.assertRaises(ocpgdb.ProgrammingError, getattr, c, 'options')
        self.assertRaises(ocpgdb.ProgrammingError, getattr, c, 'socket')
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


class OCPGDBSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self)
        self.addTest(BasicSuite())


suite = OCPGDBSuite

if __name__ == '__main__':
    unittest.main()
