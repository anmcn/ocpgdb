"""
Simple framework for profiling py components of ocpgdb
"""
import hotshot
import hotshot.stats
import ocpgdb

db = ocpgdb.connect(port=5433)

def init():
    curs = db.cursor()
    try:
        curs.execute('drop table iorate')
    except ocpgdb.DatabaseError:
        db.rollback()
    curs.execute('create table iorate (a int, b int, c int, d int)')
    curs.close()

def foo():
    curs = db.cursor()
    for i in xrange(10000):
        curs.execute('insert into iorate values (%s, %s, %s, %s)', 
                     (1000,1000,1000,1000))
    curs.close()
    db.rollback()

init()
if 0:
    foo()
else:
    prof = hotshot.Profile("scratch/ocpgdb.prof", 1, 1)
    prof.runcall(foo)
    prof.close()
    stats = hotshot.stats.load("scratch/ocpgdb.prof")
    stats.strip_dirs()
    stats.sort_stats('time', 'calls')
    stats.print_stats(20)
