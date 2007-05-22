from mx import DateTime
import datetime
from time import time

"""
Compare performance of MX.DateTime.Parser.DateTimeFromString with
python-coded parsing for ISO date/time format
"""

a='2002-03-04 11:22:33.44'

def timeit(fn):
    st = time()
    for i in xrange(10000):
        fn(a)
    print fn.__name__, time() - st

def iso_date_parse(date):
    return map(int, date.split('-'))

def iso_time_parse(time):
    time = time.split(':')
    time.extend(time.pop().split('.'))
    return map(int, time)

def from_iso_datetime(t):
    date, time = t.split()
    return datetime.datetime(*(iso_date_parse(date) + iso_time_parse(time)))

timeit(DateTime.Parser.DateTimeFromString)
timeit(from_iso_datetime)
