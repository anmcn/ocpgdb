import pgtype
from oclibpq import bytea

from_db = {}

def set_from_db(pgtype, fn):
    from_db[pgtype] = fn

set_from_db(pgtype.bool, lambda x: x == 't')
set_from_db(pgtype.float4, float)
set_from_db(pgtype.float8, float)
set_from_db(pgtype.int2, int)
set_from_db(pgtype.int4, int)
set_from_db(pgtype.int8, int)
set_from_db(pgtype.oid, int)
set_from_db(pgtype.text, str)
set_from_db(pgtype.varchar, str)
set_from_db(pgtype.bpchar, str)
set_from_db(pgtype.bytea, bytea)

try:
    import decimal
except ImportError:
    pass
else:
    # XXX - Note that cell.modifier contains precision and scale - we need
    #       to work out how to pass this to Decimal.
    set_from_db(pgtype.numeric, decimal.Decimal)
    del decimal

def _set_python_datetime(setfn):
    import datetime
    def iso_date_parse(date):
        return map(int, date.split('-'))
    def iso_time_parse(time):
        time = time.split(':')
        time.extend(time.pop().split('.'))
        return map(int, time)

    def from_iso_datetime(t):
        date, time = t.split()
        return datetime.datetime(*(iso_date_parse(date) + iso_time_parse(time)))
    setfn(pgtype.timestamp, from_iso_datetime)

    def from_iso_date(t):
        return datetime.date(*iso_date_parse(t))
    setfn(pgtype.date, from_iso_date)

    def from_iso_time(t):
        return datetime.time(*iso_time_parse(t))
    setfn(pgtype.time, from_iso_time)

    def from_interval(t):
        words = t.split()
        if len(words) == 1:
            days = 0
            time = words[0]
        else:
            assert words[1].startswith('day')
            days = int(words[0])
            if len(words) > 2:
                time = words[2]
            else:
                time = ''
        days = datetime.timedelta(days)
        if not time:
            return days
        if time[0] == '-':
            hms = iso_time_parse(time[1:])
            time = -datetime.timedelta(0, hms[2], 0, 0, hms[1], hms[0])
        else:
            hms = iso_time_parse(time)
            time = datetime.timedelta(0, hms[2], 0, 0, hms[1], hms[0])
        return days + time
    setfn(pgtype.interval, from_interval)

def use_python_datetime():
    _set_python_datetime(set_from_db)
use_python_datetime()
