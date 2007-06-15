from oclibpq import *

__all__ = (
    'Binary', 'Date', 'Time', 'Timestamp', 
    'DateFromTicks', 'TimestampFromTicks', 'TimeFromTicks',
    'STRING', 'BINARY', 'NUMBER', 'DATETIME', 'ROWID',
)

# DB-API 2 types/constructors
Binary = bytea
import datetime
Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime
DateFromTicks = datetime.date.fromtimestamp
TimestampFromTicks = datetime.datetime.fromtimestamp
def TimeFromTicks(t):
    t, fract = divmod(t, 1)
    t, seconds = divmod(int(t), 60)
    t, minutes = divmod(t, 60)
    t, hours = divmod(t, 24)
    return datetime.time(hours, minutes, seconds, int(fract * 1000000))

class DBAPITypeObject(object):
    __slots__ = ('values',)

    def __init__(self, *values):
        self.values = values

    def __cmp__(self, other):
        if other in self.values:
            return 0
        if other < self.values:
            return 1
        else:
            return -1

import pgoid

STRING = DBAPITypeObject(
    pgoid.bpchar, 
    pgoid.char, 
    pgoid.name, 
    pgoid.text, 
    pgoid.varchar,
)
BINARY = DBAPITypeObject(
    pgoid.bytea
)
NUMBER = DBAPITypeObject(
    pgoid.float4, 
    pgoid.float8, 
    pgoid.int2, 
    pgoid.int4, 
    pgoid.int8, 
    pgoid.numeric,
)
DATETIME = DBAPITypeObject(
    pgoid.interval, 
    pgoid.timestamp, 
    pgoid.timestamptz, 
    pgoid.tinterval,
)
ROWID = DBAPITypeObject(
    pgoid.oid,
)
