import oclibpq
import struct
import decimal

db=oclibpq.PgConnection('port=5433')

def _unpack_digits(words):
    shift = (1000, 100, 10, 1)
    digits = []
    for word in words:
        for s in shift:
            d = word // s % 10
            if digits or d:
                digits.append(d)
    return digits


def do(n):
    for row in db.execute('select $1::numeric', (n,)): break
    buf = row[0].value
    nwords, weight, sign, dscale = struct.unpack('!HhHH', buf[:8])
    words = list(struct.unpack('!%dH' % nwords, buf[8:]))
    twords = ['%04d' % d for d in words]

    digits = _unpack_digits(words)
    exp = (weight + 1 - nwords) * 4

    if 0 < exp < 20:
        digits = digits + [0] * exp
        exp = 0

    align = 0
    if dscale:
        align = dscale + exp
        if align < 0:
            del digits[align:]
        elif align > 0:
            digits += [0] * align
        exp -= align

    num = decimal.Decimal(((sign != 0), digits, exp))

    print '%10s | %2d %2d %2d %-16s | %-20s %3d %2d | %s' %(
        n, nwords, weight, dscale, ' '.join(twords),
        ''.join(str(d) for d in digits), exp, align,
        num)

print 'Nw - N words'
print 'We - Weight'
print 'Sc - Scale'
print
print '%10s | %-25s | %s ' % ('Num', 'PG', 'Decimal')
print '%10s | %2s %2s %2s %-16s | %-20s %3s %2s |' % (
    '', 'Nw', 'We', 'Sc', 'Words', 'digits', 'exp', 'al')
print '-' * 79
do('0.0000')
do('1')
do('1e4')
do('1e-9')
do('10001')
do('10001.0001')
do('.0001')

do('.001')
do('.0010')
do('.01')
do('10000000')
do('1000000.00')
do('10.00')
