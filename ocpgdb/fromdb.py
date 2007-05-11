# Module specific
import pgoid, pgtype
from oclibpq import bytea

from_db = {}

def set_from_db(pgtype, fn):
    from_db[pgtype] = fn

set_from_db(pgoid.bool, pgtype.unpack_bool)
set_from_db(pgoid.float4, pgtype.unpack_float4)
set_from_db(pgoid.float8, pgtype.unpack_float8)
set_from_db(pgoid.int2, pgtype.unpack_int2)
set_from_db(pgoid.int4, pgtype.unpack_int4)
set_from_db(pgoid.int8, pgtype.unpack_int8)
set_from_db(pgoid.oid, pgtype.unpack_oid)
set_from_db(pgoid.text, str)
set_from_db(pgoid.varchar, str)
set_from_db(pgoid.bpchar, str)
set_from_db(pgoid.bytea, bytea)

# Not sure how to handle pgoid.numeric

def _set_py_datetime(setfn, integer_datetimes):
    import datetime
    if integer_datetimes:
        unpack_time = pgtype.unpack_int_time
        unpack_timestamp = pgtype.unpack_int_timestamp
        unpack_date = pgtype.unpack_int_date
        unpack_interval = pgtype.unpack_int_interval
    else:
        unpack_time = pgtype.unpack_flt_time
        unpack_timestamp = pgtype.unpack_flt_timestamp
        unpack_date = pgtype.unpack_flt_date
        unpack_interval = pgtype.unpack_flt_interval
    timestamp_epoch = datetime.datetime(2000,1,1)
    date_epoch = datetime.date(2000,1,1)

    def from_timestamp(buf):
        delta = datetime.timedelta(microseconds=unpack_timestamp(buf))
        return timestamp_epoch + delta
    setfn(pgoid.timestamp, from_timestamp)

    def from_time(buf):
        microseconds = unpack_time(buf)
        seconds, microseconds = divmod(microseconds, 1000000)
        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        return datetime.time(hours, minutes, seconds, microseconds)
    setfn(pgoid.time, from_time)

    def from_date(buf):
        delta = datetime.timedelta(days=unpack_date(buf))
        return date_epoch + delta
    setfn(pgoid.date, from_date)

    # Unfortunately, python's datetime module doesn't support the semantics of
    # the PG interval type, which has the concept of relative months
    #def from_interval(t):
    #    microseconds, days, months = unpack_interval(buf)
    #setfn(pgoid.interval, from_interval)
