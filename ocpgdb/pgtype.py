# Standard Python Libs
import struct
# Module libs
from oclibpq import bytea
import pgoid

# We go to some silly lengths to maximise unpacking performance, essentially
# using closures as objects for their better performance characteristics.
try:
    struct.Struct
except AttributeError:
    # struct.Struct was introduced in 2.5, and is quite a bit quicker than this
    # fallback:
    def _make_tuple_fns(oid, fmt):
        __unpack = struct.unpack
        __pack = struct.pack
        __size = struct.calcsize(fmt)
        def _unpack(buf):
            return __unpack(fmt, buf)
        def _pack(*args):
            return oid, __pack(fmt, *args)
        return _unpack, _pack

    def _mk_fns(oid, fmt):
        __unpack = struct.unpack
        __pack = struct.pack
        __size = struct.calcsize(fmt)
        def _pack(*args):
            return oid, __pack(fmt, *args)
        def _unpack(buf):
            return __unpack(fmt, buf)[0]
        return _unpack, _pack
else:
    def _make_tuple_fns(oid, fmt):
        _struct = struct.Struct(fmt)
        __pack = _struct.pack
        __size = _struct.size
        def _pack(*args):
            return oid, __pack(*args)
        return _struct.unpack, _pack

    def _mk_fns(oid, fmt):
        _struct = struct.Struct(fmt)
        __pack = _struct.pack
        __unpack = _struct.unpack
        __size = _struct.size
        def _pack(*args):
            return oid, __pack(*args)
        def _unpack(buf):
            return __unpack(buf)[0]
        return _unpack, _pack

unpack_bool, pack_bool = _mk_fns(pgoid.bool, '!B')
unpack_int2, pack_int2 = _mk_fns(pgoid.int2, '!h')
unpack_int4, pack_int4 = _mk_fns(pgoid.int4, '!l')
unpack_oid, pack_oid = _mk_fns(pgoid.oid, '!L')
unpack_int8, pack_int8 = _mk_fns(pgoid.int8, '!q')
unpack_float4, pack_float4 = _mk_fns(pgoid.float4, '!f')
unpack_float8, pack_float8 = _mk_fns(pgoid.float8, '!d')

unpack_str = str
def pack_str(value):
    return pgoid.text, value
unpack_bytea = bytea
def pack_bytea(value):
    return pgoid.bytea, value

# Depending on build time options, PG may send dates and times as floats or
# ints. The connection parameter integer_datetimes allows us to tell.

# uS into day
unpack_int_time, pack_int_time = _mk_fns(pgoid.time, '!q')
unpack_flt_time, pack_flt_time = _mk_fns(pgoid.time, '!d')
# uS from 2000-01-01
unpack_int_timestamp, pack_int_timestamp = _mk_fns(pgoid.timestamp, '!q')
unpack_flt_timestamp, pack_flt_timestamp = _mk_fns(pgoid.timestamp, '!d')
# days from 2000-01-01
unpack_int_date, pack_int_date = _mk_fns(pgoid.date, '!l')
unpack_flt_date, pack_flt_date = _mk_fns(pgoid.date, '!f')
# uS, days, months
unpack_int_interval, pack_int_interval = _make_tuple_fns(pgoid.interval, '!qll')
unpack_flt_interval, pack_flt_interval = _make_tuple_fns(pgoid.interval, '!dll')

def unpack_numeric(buf):
    ndigits, weight, sign, dscale = struct.unpack('!hhhh', buf[:8])
    digits = buf[8:]
    return ndigits, weight, sign, dscale, digits

#       number of dimensions (int4)
#	flags (int4)
#	element type id (Oid)
#	for each dimension:
#		dimension length (int4)
#		dimension lower subscript bound (int4)
#	for each array element:
#		element value, in the appropriate format
def _pack_array(oid, dims, element_data):
    data = []
    data.append(struct.pack('!llL', len(dims), 0, pgoid.array_types[oid]))
    for dim in dims:
        data.append(struct.pack('!ll', dim, 0))
    for element in element_data:
        data.append(struct.pack('!l', len(element)))
        data.append(element)
    return oid, ''.join(data)

def pack_int_array(array):
    element_data = []
    for element in array:
        oid, data = pgtype.pack_int4(element)
        assert oid == pgoid.int4
        element_data.append(data)
    return _pack_array(pgoid._int4, [len(element_data)], element_data)
