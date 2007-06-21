# Language behaviour
from __future__ import division
# Standard Python Libs
import struct
import decimal
# Module
import pgoid

NUMERIC_POS = 0x0000
NUMERIC_NEG = 0x4000
NUMERIC_NAN = 0xC000

def _unpack_digits(words):
    shift = (1000, 100, 10, 1)
    digits = []
    for word in words:
        for s in shift:
            d = word // s % 10
            if digits or d:
                digits.append(d)
    return tuple(digits)

def unpack_numeric(buf):
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
        digits = _unpack_digits(words)
        cull = (4 - dscale) % 4
        exp = (weight + 1 - ndigits) * 4 + cull
        if cull:
            digits = digits[:-cull]
    else:
        exp = -dscale
        digits = (0,)
    return decimal.Decimal((sign, digits, exp))


def _pack_digits(digits):
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

def pack_numeric(num):
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
    words = _pack_digits(digits)
    ndigits = len(words)
    weight = ndigits - 1 + exp // 4
#    print (ndigits, weight, sign, dscale) + words
    return pgoid.numeric, struct.pack('!HhHH%dH' % ndigits, 
                                     *((ndigits, weight, sign, dscale) + words))
