from __future__ import division
# Standard Python Libs
import sys
import struct
# Module libs
from oclibpq import bytea
from . import pgoid

"""
PG binary wire formats
"""

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

_unpack_bool, pack_bool = _mk_fns(pgoid.bool, '!B')
def unpack_bool(buf):
    return bool(_unpack_bool(buf))

unpack_int2, pack_int2 = _mk_fns(pgoid.int2, '!h')
unpack_int4, pack_int4 = _mk_fns(pgoid.int4, '!l')
unpack_oid, pack_oid = _mk_fns(pgoid.oid, '!L')
unpack_int8, pack_int8 = _mk_fns(pgoid.int8, '!q')
unpack_float4, pack_float4 = _mk_fns(pgoid.float4, '!f')
unpack_float8, pack_float8 = _mk_fns(pgoid.float8, '!d')
if sys.maxint > 0x7fffffff:
    def pack_int(value):
        if value > 0x7fffffff or value < -0x80000000:
            return pack_int8(value)
        else:
            return pack_int4(value)
else:
    pack_int = pack_int4

unpack_str = str
def pack_str(value):
    return pgoid.text, value

unpack_bytea = bytea
def pack_bytea(value):
    return pgoid.bytea, value

def mk_pack_unicode(encoding):
    def pack_unicode(value):
        return pgoid.text, value.encode(encoding)
    return pack_unicode

def mk_unpack_unicode(encoding):
    def unpack_unicode(value):
        return value.decode(encoding)
    return unpack_unicode

# Depending on build time options, PG may send dates and times as floats or
# ints. The connection parameter integer_datetimes allows us to tell. When
# floats are used, typically the value is in seconds, but when ints are used,
# the value is typically microseconds.

usec_mul = 1000000.0
# uS into day
unpack_int_time, pack_int_time = _mk_fns(pgoid.time, '!q')
def unpack_flt_time(buf):
    return struct.unpack('!d', buf)[0] * usec_mul
def pack_flt_time(usecs):
    return pgoid.time, struct.pack('!d', usecs / usec_mul)
# uS from 2000-01-01
unpack_int_timestamp, pack_int_timestamp = _mk_fns(pgoid.timestamp, '!q')
def unpack_flt_timestamp(buf):
    return struct.unpack('!d', buf)[0] * usec_mul
def pack_flt_timestamp(usecs):
    return pgoid.timestamp, struct.pack('!d', usecs / usec_mul)
# days from 2000-01-01
unpack_int_date, pack_int_date = _mk_fns(pgoid.date, '!l')
unpack_flt_date, pack_flt_date = _mk_fns(pgoid.date, '!l')
# uS, days, months
unpack_int_interval, pack_int_interval = _make_tuple_fns(pgoid.interval, '!qll')
def unpack_flt_interval(buf):
    seconds, days, months = struct.unpack('!dll', buf)
    return seconds * usec_mul, days, months
def pack_flt_interval(usecs, days, months):
    return pgoid.interval, struct.pack('!dll', usecs / usec_mul, days, months)

#       number of dimensions (int4)
#	flags (int4)
#	element type id (Oid)
#	for each dimension:
#		dimension length (int4)
#		dimension lower subscript bound (int4)
#	for each array element:
#		element value, in the appropriate format
def pack_array(array_oid, data_oid, dims, element_data):
    data = []
    data.append(struct.pack('!llL', len(dims), 0, data_oid))
    for dim in dims:
        data.append(struct.pack('!ll', dim, 0))
    for element in element_data:
        data.append(struct.pack('!l', len(element)))
        data.append(element)
    return array_oid, ''.join(data)

def unpack_array(data):
    hdr_fmt = '!llL'
    hdr_size = struct.calcsize(hdr_fmt)
    ndims, flags, data_oid = struct.unpack(hdr_fmt, data[:hdr_size])
    assert flags == 0
    dims_fmt = '!' + 'll' * ndims
    dims_size = struct.calcsize(dims_fmt)
    dims = struct.unpack(dims_fmt, data[hdr_size:hdr_size+dims_size])[::2]
    offset = hdr_size+dims_size
    elements = []
    while offset < len(data):
        data_offset = offset+4
        size = struct.unpack('!l', data[offset:data_offset])[0]
        end_offset = data_offset + size
        elements.append(data[data_offset:end_offset])
        offset = end_offset
    return data_oid, dims, elements
