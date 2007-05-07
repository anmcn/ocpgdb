import os

op = os.popen('psql template1 -A -t -c "select oid, typname from pg_type where typtype = \'b\' and typelem = 0"', 'r')
for line in op:
    oid, name = line.strip().split('|')
    print '%s = %s' % (name, oid)

