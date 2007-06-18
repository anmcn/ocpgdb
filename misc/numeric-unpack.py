from time import time
def unpackA(i):
    return tuple([int(c) for c in str(i)])

def unpackB(n):
    d = []
    while n:
        d.append(n % 10)
        n /= 10
    d.reverse()
    return tuple(d)

def todecimal(words):
    d = []
    for word in words[::-1]:
        for i in range(4):
            d.insert(0, dig4 % 10)
            dig4 /= 10
    return tuple(d)

def unpackD(words):
    shift = (1000, 100, 10, 1)
    digits = []
    for word in words:
        for s in shift:
            d = word / s % 10
            if digits or d:
                digits.append(d)
    return tuple(digits)

def test(fn):
    r = []
    st = time()
    for i in range(0, 9999):
        r.append(fn(i))
    el = time() - st
    print el, 1 / (el / 10000)
    return r

def test_T(fn):
    r = []
    st = time()
    for i in range(0, 9999):
        r.append(fn((i, i, i, i)))
    el = time() - st
    print el, 1 / (el / 10000)
    return r


a = test(unpackA)
b = test(unpackB)
test_T(unpackD)
assert a == b
