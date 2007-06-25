from __future__ import division
import sys
# Module specific
import pgoid, pgtype
from oclibpq import bytea, InterfaceError, InternalError

from_db = {}

def set_from_db(pgtype, fn):
    from_db[pgtype] = fn

def array_from_db(from_db, data):
    data_oid, dims, elements = pgtype.unpack_array(data)
    if len(dims) != 1:
        raise InterfaceError('Cannot unpack multi-dimensional arrays - %s' % dims)
    try:
        cvt = from_db[data_oid]
    except KeyError:
        raise InterfaceError('No from_db function for oid %r' % data_oid)
    return [cvt(e) for e in elements]

def value_from_db(from_db, cell):
    if cell.value is None:
        return None
    try:
        cvt = from_db[cell.type]
    except KeyError:
        raise InterfaceError('No from_db function for type %r (column %r, value %r)'% (cell.type, cell.name, cell.value))
    try:
        if cvt is array_from_db:
            return cvt(from_db, cell.value)
        else:
            return cvt(cell.value)
    except Exception, e:
        raise InternalError, 'failed to convert column value %r (column %r, type %r): %s' % (cell.value, cell.name, cell.type, e), sys.exc_info()[2]

for array_oid in pgoid.array_to_data:
    set_from_db(array_oid, array_from_db)

set_from_db(pgoid.bool, pgtype.unpack_bool)
set_from_db(pgoid.float4, pgtype.unpack_float4)
set_from_db(pgoid.float8, pgtype.unpack_float8)
set_from_db(pgoid.int2, pgtype.unpack_int2)
set_from_db(pgoid.int4, pgtype.unpack_int4)
set_from_db(pgoid.int8, pgtype.unpack_int8)
set_from_db(pgoid.oid, pgtype.unpack_oid)
set_from_db(pgoid.text, str)
set_from_db(pgoid.varchar, str)
set_from_db(pgoid.bpchar, str)
set_from_db(pgoid.name, str)
set_from_db(pgoid.bytea, bytea)
try:
    import cvtdecimal
except ImportError:
    pass
else:
    set_from_db(pgoid.numeric, cvtdecimal.unpack_numeric)

def _set_py_datetime(setfn, integer_datetimes):
    import cvtpytime
    cvtpytime.register_from(setfn, integer_datetimes)

def _set_mx_datetime(setfn, integer_datetimes):
    import cvtmxtime
    cvtmxtime.register_from(setfn, integer_datetimes)
