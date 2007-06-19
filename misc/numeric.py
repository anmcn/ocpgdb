import ocpgdb
import struct
import decimal

def unpack_digitsA(words):
    return tuple([int(digit) for word in words for digit in str(word)])

def unpack_digitsB(words):
    d = []
    for word in words[::-1]:
        for i in range(4):
            d.insert(0, word % 10)
            word /= 10
    return tuple(d)

def unpack_digits(words):
    shift = (1000, 100, 10, 1)
    digits = []
    for word in words:
        for s in shift:
            d = word / s % 10
            if digits or d:
                digits.append(d)
    return tuple(digits)

def decode(db, value):
    data = list(ocpgdb.PgConnection.execute(db, "select '%s'::numeric" % value, ()))[0][0].value
    words = struct.unpack('!%dh' % (len(data) / 2), data)
    ndigits, weight, sign, dscale = words[:4]
    words = words[4:]
    print "ndigits %d, weight %d, sign %d, dscale %d" % (ndigits, weight, sign, dscale)
    print words
    assert ndigits == len(words)
    if words:
        digits = unpack_digits(words)
        if sign == 16384:
            sign = 1
        cull = (4 - dscale) % 4
        exp = (weight + 1 - ndigits) * 4 + cull
        if cull:
            digits = digits[:-cull]
    else:
        exp = -dscale
        digits = (0,) * dscale
    print digits, exp
    return decimal.Decimal((sign, digits, exp))

db=ocpgdb.connect(database="andrewm",port=5433,use_mx_datetime=1)
