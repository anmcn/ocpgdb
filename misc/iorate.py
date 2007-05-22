from time import time

"""
Compare performance of ocpgdb, pyPgSQL and psycopg2 I/O (bulk select
and insert)
"""

dbname = 'andrewm'
dbport = 5433
table = 'iorate'

def do_type(db, vtype, value):
    curs = db.cursor()
    curs.execute('BEGIN WORK')
    try:
        try:
            curs.execute('drop table %s' % table)
        except Exception:
            db.rollback()
        curs.execute('create table %s (a %s, b %s, c %s, d %s)' % 
            (table, vtype, vtype, vtype, vtype))
    finally:
        curs.close()
    curs = db.cursor()
    count = 2000
    try:
        row = (value, value, value, value)
        ins = 'insert into %s values (%%s, %%s, %%s, %%s)' % table
        st = time()
        for i in xrange(count):
            curs.execute(ins, row)
        insert_el = (time() - st) / count
        st = time()
        curs.execute('select * from %s' % table)
        rows = curs.fetchall()
        select_el = (time() - st) / count
        assert len(rows) == count
        assert [row] * count == [tuple(r) for r in rows]
    finally:
        curs.close()
    db.rollback()
    return insert_el, select_el


def test(module, connect_fn, **connect_args):
    times = []
    st = time()
    for i in xrange(10):
        db = connect_fn(**connect_args)
        db.close()
    times.append((time() - st) / 10)
    db = connect_fn(**connect_args)
    try:
        times.extend(do_type(db, 'int', 10000))
        times.extend(do_type(db, 'varchar', 'A' * 512))
        times.extend(do_type(db, 'float8', 1.35e-4))
    finally:
        db.close()
    times = ['%8.0f' % (1 / el) for el in times]
    print '%-8s %s' % (module, ' '.join(times))

import ocpgdb
from pyPgSQL import PgSQL
import psycopg2

titles = ['', 'int', 'int', 'varchar', 'varchar', 'float8', 'float8']
print '%-8s %s' % ('', ' '.join(['%8s' % t for t in titles]))
titles = ['connect', 'insert', 'select', 'insert', 'select','insert', 'select']
print '%-8s %s' % ('', ' '.join(['%8s' % t for t in titles]))
test('ocpgdb ', ocpgdb.connect, dbname=dbname, port=dbport)
test('pypgsql', PgSQL.connect, database=dbname, port=dbport)
test('psycopg', psycopg2.connect, database=dbname, port=dbport)
