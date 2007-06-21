from __future__ import division
# Standard Python Libs
import struct
import decimal
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

NUMERIC_POS = 0x0000
NUMERIC_NEG = 0x4000
NUMERIC_NAN = 0xC000
def unpack_numeric(buf):
    def unpack_digits(words):
        shift = (1000, 100, 10, 1)
        digits = []
        for word in words:
            for s in shift:
                d = word // s % 10
                if digits or d:
                    digits.append(d)
        return tuple(digits)
    ndigits, weight, sign, dscale = struct.unpack('!HhHH', buf[:8])
    if sign == NUMERIC_POS:
        sign = 0
    elif sign == NUMERIC_NEG:
        sign = 1
    elif sign == NUMERIC_NAN:
        return decimal.Decimal('NaN')
    else:
        raise ValueError('Invalid numeric sign: %0x' % sign)
    if ndigits:
        words = struct.unpack('!%dH' % ndigits, buf[8:])
        digits = unpack_digits(words)
        cull = (4 - dscale) % 4
        exp = (weight + 1 - ndigits) * 4 + cull
        if cull:
            digits = digits[:-cull]
    else:
        exp = -dscale
        digits = (0,)
    return decimal.Decimal((sign, digits, exp))

def pack_numeric(num):
    def pack_digits(digits):
        words = []
        shift = 1, 10, 100, 1000
        i = len(digits)
        while i > 0:
            word = 0
            for s in shift:
                i -= 1
                word += digits[i] * s
                if i == 0:
                    break
            words.insert(0, word)
        return tuple(words)
    sign, digits, exp = num.as_tuple()
    if not isinstance(exp, int):
        if exp == 'n' or exp == 'N':
            return pgoid.numeric, struct.pack('!HhHH', 0, 0, NUMERIC_NAN, 0)
        elif exp == 'F':
            raise ValueError('No conversion available for Decimal(Infinity)')
        raise ValueError('Unsupported %r' % num)
    if exp < 0:
        dscale = -exp
    else:
        dscale = 0
    if sign:
        sign = NUMERIC_NEG
    else:
        sign = NUMERIC_POS
    digits = digits + (0,) * (exp % 4)
    words = pack_digits(digits)
    ndigits = len(words)
    weight = ndigits - 1 + exp // 4
#    print (ndigits, weight, sign, dscale) + words
    return pgoid.numeric, struct.pack('!HhHH%dH' % ndigits, 
                                     *((ndigits, weight, sign, dscale) + words))

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
