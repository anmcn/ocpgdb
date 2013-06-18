import sys
import types
# Module specific
from . import pgoid, pgtype
from oclibpq import bytea, DataError

# Note that this is the common to_db map - each connection object also has it's
# own to_db map which overrides this:
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
    try:
        cvt = to_db[data_type]
    except KeyError:
        raise DataError('no to_db function for %r' % data_type)
    data_oid = None
    array_data = []
    for v in array:
        t = type(v)
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
    if vtype is list or vtype is tuple:
        return list_to_db(to_db, value)
    try:
        cvt = to_db[vtype]
    except KeyError:
        try:
            cvt = to_db[value.__class__]
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
set_to_db(bytea, pgtype.pack_bytea)
try:
    import decimal
    from . import cvtdecimal
except ImportError:
    pass
else:
    set_to_db(decimal.Decimal, cvtdecimal.pack_numeric)

def _set_encoding(setfn, encoding):
    if sys.version_info < (3,0):
        setfn(str, pgtype.pack_str)
        setfn(unicode, pgtype.mk_pack_unicode(encoding))
    else:
        setfn(bytes, pgtype.pack_str)
        setfn(str, pgtype.mk_pack_unicode(encoding))

def _set_py_datetime(setfn, integer_datetimes):
    from . import cvtpytime
    cvtpytime.register_to(setfn, integer_datetimes)

def _set_mx_datetime(setfn, integer_datetimes):
    from . import cvtmxtime
    cvtmxtime.register_to(setfn, integer_datetimes)

def _set_ipaddress(setfn):
    import ipaddress
    from . import cvtipaddress
    setfn(ipaddress.IPv4Address, cvtipaddress.pack_ipv4addr)
    setfn(ipaddress.IPv6Address, cvtipaddress.pack_ipv6addr)
    setfn(ipaddress.IPv4Network, cvtipaddress.pack_ipv4net)
    setfn(ipaddress.IPv6Network, cvtipaddress.pack_ipv6net)

def _set_ipaddr(setfn):
    import ipaddr
    from . import cvtipaddr
    setfn(ipaddr.IPv4Address, cvtipaddr.pack_ipv4addr)
    setfn(ipaddr.IPv6Address, cvtipaddr.pack_ipv6addr)
    setfn(ipaddr.IPv4Network, cvtipaddr.pack_ipv4net)
    setfn(ipaddr.IPv6Network, cvtipaddr.pack_ipv6net)
