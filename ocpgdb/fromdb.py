import pgtype

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

try:
    import decimal
except ImportError:
    pass
else:
    # XXX - Note that cell.modifier contains precision and scale - we need
    #       to work out how to pass this to Decimal.
    set_from_db(pgtype.numeric, decimal.Decimal)
    del decimal
