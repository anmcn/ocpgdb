# Module specific
import pgoid, pgtype
from oclibpq import bytea

to_db = {}

def set_to_db(pytype, fn):
    to_db[pytype] = fn

set_to_db(bool, pgtype.pack_bool)
set_to_db(float, pgtype.pack_float8)
set_to_db(int, pgtype.pack_int4)
set_to_db(long, pgtype.pack_int8)
set_to_db(str, pgtype.pack_str)
set_to_db(bytea, pgtype.pack_bytea)

def XXX(value):
    element_data = []
    assert pgoid.array_types[pgoid._int4] == pgoid.int4
    for element in value:
        oid, data = pgtype.pack_int4(element)
        assert oid == pgoid.int4
        element_data.append(data)
    return pgtype.pack_array(pgoid._int4, [len(element_data)], element_data)

set_to_db(list, XXX)
    
def _set_py_datetime(setfn, integer_datetimes):
    import datetime
    if integer_datetimes:
        pack_time = pgtype.pack_int_time
        pack_timestamp = pgtype.pack_int_timestamp
        pack_date = pgtype.pack_int_date
    else:
        pack_time = pgtype.pack_flt_time
        pack_timestamp = pgtype.pack_flt_timestamp
        pack_date = pgtype.pack_flt_date
    timestamp_epoch = datetime.datetime(2000,1,1)
    date_epoch = datetime.date(2000,1,1)

    def to_time(value):
        microseconds = (value.microsecond + 1000000L * (value.second
                        + 60 * (value.minute + 60 * (value.hour))))
        return pack_time(microseconds)
    setfn(datetime.time, to_time)

    def to_timestamp(value):
        delta = value - timestamp_epoch
        microseconds = (delta.microseconds + 1000000L * (delta.seconds
                        + 86400 * delta.days))
        return pack_timestamp(microseconds)
    setfn(datetime.datetime, to_timestamp)

    def to_date(value):
        delta = value - date_epoch
        return pack_date(delta.days)
    setfn(datetime.date, to_date)
