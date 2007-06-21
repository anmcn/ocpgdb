import os

op = os.popen('psql template1 -A -t -c "select oid, typname, typelem from pg_type where typtype = \'b\' order by oid"', 'r')
name_to_oid = {}
names = []
for line in op:
    oid, name, typelem = line.strip().split('|')
    oid = int(oid)
    name_to_oid[name] = oid
    names.append(name)
    typelem = int(typelem)
    print '%s = %d' % (name, oid)

print
print 'data_to_array = {'
for name in names:
    _name = '_' + name
    if not name.startswith('_') and _name in name_to_oid:
        print '    %d: %d,' % (name_to_oid[name], name_to_oid[_name])
print '}'

print
print 'array_to_data = {'
for name in names:
    _name = '_' + name
    if not name.startswith('_') and _name in name_to_oid:
        print '    %d: %d,' % (name_to_oid[_name], name_to_oid[name])
print '}'
