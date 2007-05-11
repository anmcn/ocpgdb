import os

op = os.popen('psql template1 -A -t -c "select oid, typname, typelem from pg_type where typtype = \'b\' order by typname"', 'r')
array_types = []
for line in op:
    oid, name, typelem = line.strip().split('|')
    oid = int(oid)
    typelem = int(typelem)
    if typelem:
        array_types.append((oid, typelem))
    print '%s = %d' % (name, oid)

print
array_types.sort()
print 'array_types = {'
for oid, typelem in array_types:
    print '    %d: %d,' % (oid, typelem)
print '}'
