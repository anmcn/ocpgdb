import sys
import unittest
import oclibpq


class BasicTests(unittest.TestCase):

    def test_module_const(self):
        pass


class BasicSuite(unittest.TestSuite):
    tests = [
        'test_module_const',
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
