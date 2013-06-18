# Standard libraries
from __future__ import division
import datetime
# Module specific
from . import pgoid, pgtype

def register_from(setfn, integer_datetimes):
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


def register_to(setfn, integer_datetimes):
    import datetime
    if integer_datetimes:
        pack_time = pgtype.pack_int_time
        pack_timestamp = pgtype.pack_int_timestamp
        pack_date = pgtype.pack_int_date
        usec_mul = 1000000L
    else:
        pack_time = pgtype.pack_flt_time
        pack_timestamp = pgtype.pack_flt_timestamp
        pack_date = pgtype.pack_flt_date
        usec_mul = 1000000.0
    timestamp_epoch = datetime.datetime(2000,1,1)
    date_epoch = datetime.date(2000,1,1)

    def to_time(value):
        microseconds = (value.microsecond + usec_mul * (value.second
                        + 60 * (value.minute + 60 * (value.hour))))
        return pack_time(microseconds)
    setfn(datetime.time, to_time)

    def to_timestamp(value):
        delta = value - timestamp_epoch
        microseconds = (delta.microseconds + usec_mul * (delta.seconds
                        + 86400 * delta.days))
        return pack_timestamp(microseconds)
    setfn(datetime.datetime, to_timestamp)

    def to_date(value):
        delta = value - date_epoch
        return pack_date(delta.days)
    setfn(datetime.date, to_date)

