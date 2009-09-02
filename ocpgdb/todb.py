import sys
import types
# Module specific
import pgoid, pgtype
from oclibpq import bytea, DataError

to_db = {}

def set_to_db(pytype, fn):
    to_db[pytype] = fn

def list_to_db(to_db, array):
    """
    Attempt to autodetect list type and coerce to a PG array.

    Fails for zero length lists (what type to use?), and non-homogenous lists
    """
    if not len(array):
        raise DataError('Cannot coerce 0-length tuples/lists to PG array')
    data_type = type(array[0])
    if data_type is types.InstanceType:
        data_type = array[0].__class__
    try:
        cvt = to_db[data_type]
    except KeyError:
        raise DataError('no to_db function for %r' % data_type)
    data_oid = None
    array_data = []
    for v in array:
        t = type(v)
        if t is types.InstanceType:
            t = v.__class__
        if t is not data_type:
            raise DataError('Array contains non-homogenous types '
                            '(%r, %r)' % (data_type, t))
        oid, data = cvt(v)
        if data_oid is None:
            data_oid = oid
        elif data_oid != oid:
            raise InternalError('Inconsistent array data OIDs'
                                '(%r, %r)' % (data_oid, oid))
        array_data.append(data)
    try:
        array_oid = pgoid.data_to_array[data_oid]
    except KeyError:
        raise DataError('No array type corresponding to %r (oid %s)' %
                        (data_type, data_oid))
    return pgtype.pack_array(array_oid, data_oid, [len(array_data)], array_data)

def value_to_db(to_db, value):
    if value is None:
        return None
    vtype = type(value)
    if vtype is types.InstanceType:
        vtype = value.__class__
    elif vtype is list or vtype is tuple:
        return list_to_db(to_db, value)
    try:
        cvt = to_db[vtype]
    except KeyError:
        raise DataError('no to_db function for %r' % vtype)
    try:
        return cvt(value)
    except Exception, e:
        raise DataError, 'column value %r: %s' % (value, e),\
              sys.exc_info()[2]


set_to_db(bool, pgtype.pack_bool)
set_to_db(float, pgtype.pack_float8)
set_to_db(int, pgtype.pack_int)
set_to_db(long, pgtype.pack_int8)
set_to_db(str, pgtype.pack_str)
set_to_db(bytea, pgtype.pack_bytea)
try:
    import decimal, cvtdecimal
except ImportError:
    pass
else:
    set_to_db(decimal.Decimal, cvtdecimal.pack_numeric)


def _set_py_datetime(setfn, integer_datetimes):
    import cvtpytime
    cvtpytime.register_to(setfn, integer_datetimes)

def _set_mx_datetime(setfn, integer_datetimes):
    import cvtmxtime
    cvtmxtime.register_to(setfn, integer_datetimes)
