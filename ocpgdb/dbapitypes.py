from . import *

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
    __slots__ = ('name', 'values',)

    def __init__(self, name, *values):
        self.name = name
        self.values = values

    def __repr__(self):
        return '<%s.%s %r>' % (__name__, self.name, self.values)

    def __eq__(self, other):
        return other in self.values

from . import pgoid

STRING = DBAPITypeObject('STRING',
    pgoid.bpchar, 
    pgoid.char, 
    pgoid.name, 
    pgoid.text, 
    pgoid.varchar,
)
BINARY = DBAPITypeObject('BINARY',
    pgoid.bytea
)
NUMBER = DBAPITypeObject('NUMBER',
    pgoid.float4, 
    pgoid.float8, 
    pgoid.int2, 
    pgoid.int4, 
    pgoid.int8, 
    pgoid.numeric,
)
DATETIME = DBAPITypeObject('DATETIME',
    pgoid.date, 
    pgoid.time, 
    pgoid.interval, 
    pgoid.timestamp, 
    pgoid.timestamptz, 
    pgoid.tinterval,
)
ROWID = DBAPITypeObject('ROWID',
    pgoid.oid,
)
