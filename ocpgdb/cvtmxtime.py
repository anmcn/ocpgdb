# Standard libraries
from __future__ import division
import math
# 3rd party
from mx import DateTime
# Module specific
import pgoid, pgtype

def register_from(setfn, integer_datetimes):
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
    timestamp_epoch = DateTime.DateTime(2000,1,1)
    date_epoch = DateTime.Date(2000,1,1)

    def from_timestamp(buf):
        seconds = round(unpack_timestamp(buf) / pgtype.usec_mul, 2)
        delta = DateTime.DateTimeDeltaFromSeconds(seconds)
        return timestamp_epoch + delta
    setfn(pgoid.timestamp, from_timestamp)

    def from_time(buf):
        seconds = round(unpack_time(buf) / pgtype.usec_mul, 2)
        return DateTime.Time(seconds=seconds)
    setfn(pgoid.time, from_time)

    def from_date(buf):
        delta = DateTime.DateTimeDeltaFromDays(unpack_date(buf))
        return date_epoch + delta
    setfn(pgoid.date, from_date)

    def from_interval(buf):
        microseconds, days, months = unpack_interval(buf)
        seconds = round(microseconds / pgtype.usec_mul, 2)
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


def register_to(setfn, integer_datetimes):
    if integer_datetimes:
        pack_time = pgtype.pack_int_time
        pack_timestamp = pgtype.pack_int_timestamp
        pack_date = pgtype.pack_int_date
        pack_interval = pgtype.pack_int_interval
        usec_mul = 1000000L
    else:
        pack_time = pgtype.pack_flt_time
        pack_timestamp = pgtype.pack_flt_timestamp
        pack_date = pgtype.pack_flt_date
        pack_interval = pgtype.pack_flt_interval
        usec_mul = 1000000.0
    timestamp_epoch = DateTime.DateTime(2000,1,1)
    date_epoch = DateTime.Date(2000,1,1)

    def to_time(value):
        return pack_time(round(value.seconds, 2) * usec_mul)
    setfn(DateTime.DateTimeDeltaType, to_time)

    def to_timestamp(value):
        delta = value - timestamp_epoch
        return pack_timestamp(round(delta.seconds, 2) * usec_mul)
    setfn(DateTime.DateTimeType, to_timestamp)

    def to_interval(value):
        seconds = value.seconds + ((value.hours * 60.0) + value.minutes) * 60.0
        months = value.months + (value.years * 12.0)
        return pack_interval(round(seconds, 2) * usec_mul, value.days, months)
    setfn(DateTime.RelativeDateTime, to_interval)
