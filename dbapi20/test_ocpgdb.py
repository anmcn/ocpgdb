#!/usr/bin/env python
import dbapi20
import unittest
import ocpgdb

class TestPgSQL(dbapi20.DatabaseAPI20Test):
    driver = ocpgdb
    connect_args = ()
    connect_kw_args = dict(database='dbapi20_test', port=5433)
    lower_func = 'lower'

    def test_nextset(self):
        pass

    def test_setoutputsize(self):
        pass
    

if __name__ == '__main__':
    unittest.main()

