from __future__ import division
import sys
import math
# Module specific
import pgoid, pgtype
from oclibpq import bytea, InterfaceError, InternalError

from_db = {}

def set_from_db(pgtype, fn):
    from_db[pgtype] = fn

def array_from_db(from_db, data):
    data_oid, dims, elements = pgtype.unpack_array(data)
    if len(dims) != 1:
        raise InterfaceError('Cannot unpack multi-dimensional arrays - %s' % dims)
    try:
        cvt = from_db[data_oid]
    except KeyError:
        raise InterfaceError('No from_db function for oid %r' % data_oid)
    return [cvt(e) for e in elements]

def value_from_db(from_db, cell):
    if cell.value is None:
        return None
    try:
        cvt = from_db[cell.type]
    except KeyError:
        raise InterfaceError('No from_db function for type %r (column %r, value %r)'% (cell.type, cell.name, cell.value))
    try:
        print cvt, array_from_db
        if cvt is array_from_db:
            return cvt(from_db, cell.value)
        else:
            return cvt(cell.value)
    except Exception, e:
        raise InternalError, 'failed to convert column value %r (column %r, type %r): %s' % (cell.value, cell.name, cell.type, e), sys.exc_info()[2]

for array_oid in pgoid.array_to_data:
    set_from_db(array_oid, array_from_db)

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
set_from_db(pgoid.numeric, pgtype.unpack_numeric)

def _set_py_datetime(setfn, integer_datetimes):
    import datetime
    if integer_datetimes:
        unpack_time = pgtype.unpack_int_time
        unpack_timestamp = pgtype.unpack_int_timestamp
        unpack_date = pgtype.unpack_int_date
        unpack_interval = pgtype.unpack_int_interval
        usec_mul = 1000000L
    else:
        unpack_time = pgtype.unpack_flt_time
        unpack_timestamp = pgtype.unpack_flt_timestamp
        unpack_date = pgtype.unpack_flt_date
        unpack_interval = pgtype.unpack_flt_interval
        usec_mul = 1000000.0
    timestamp_epoch = datetime.datetime(2000,1,1)
    date_epoch = datetime.date(2000,1,1)

    def from_timestamp(buf):
        delta = datetime.timedelta(microseconds=unpack_timestamp(buf))
        return timestamp_epoch + delta
    setfn(pgoid.timestamp, from_timestamp)

    def from_time(buf):
        microseconds = unpack_time(buf)
        seconds, microseconds = divmod(microseconds, usec_mul)
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

def _set_mx_datetime(setfn, integer_datetimes):
    from mx import DateTime
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
    usec_mul = 1000000.0
    timestamp_epoch = DateTime.DateTime(2000,1,1)
    date_epoch = DateTime.Date(2000,1,1)

    def from_timestamp(buf):
        seconds = round(unpack_timestamp(buf) / usec_mul, 2)
        delta = DateTime.DateTimeDeltaFromSeconds(seconds)
        return timestamp_epoch + delta
    setfn(pgoid.timestamp, from_timestamp)

    def from_time(buf):
        seconds = round(unpack_time(buf) / usec_mul, 2)
        return DateTime.Time(seconds=seconds)
    setfn(pgoid.time, from_time)

    def from_date(buf):
        delta = DateTime.DateTimeDeltaFromDays(unpack_date(buf))
        return date_epoch + delta
    setfn(pgoid.date, from_date)

    def from_interval(buf):
        microseconds, days, months = unpack_interval(buf)
        seconds = round(microseconds / usec_mul, 2)
        # Unfortunately, we can't use divmod here...
        hours = int(seconds / 3600.0)
        seconds = math.fmod(seconds, 3600.0)
        minutes = int(seconds / 60.0)
        seconds = math.fmod(seconds, 60.0)
        years = int(months / 12.0)
        months = int(math.fmod(months, 12))
        return DateTime.RelativeDateTime(years, months, days, 
                                         hours, minutes, seconds)
    setfn(pgoid.interval, from_interval)
