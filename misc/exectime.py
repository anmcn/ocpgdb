from time import time

"""
Compare performance of ocpgdb, pyPgSQL and psycopg2 for simple exec's
"""

def time_exec(db, cmd, *args):
    st = time()
    curs = db.cursor()
    try:
        for n in xrange(10000):
            curs.execute(cmd, args)
    finally:
        curs.close()
    return 1 / ((time() - st) / 10000)

import ocpgdb
from pyPgSQL import PgSQL
import psycopg2

print "no args"
print 'ocpgdb', time_exec(ocpgdb.connect(port=5433), 'select 1')
print 'pypgsql', time_exec(PgSQL.connect(port=5433), 'select 1')
print 'psycopg', time_exec(psycopg2.connect(port=5433), 'select 1')

print "one arg"
print 'ocpgdb', time_exec(ocpgdb.connect(port=5433), 'select %s', 1)
print 'pypgsql', time_exec(PgSQL.connect(port=5433), 'select %s', 1)
print 'psycopg', time_exec(psycopg2.connect(port=5433), 'select %s', 1)
