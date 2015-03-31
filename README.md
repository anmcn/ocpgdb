# ocpgdb 

**ocpgdb** is a Python DB-API 2 [PEP-0249] (https://www.python.org/dev/peps/pep-0249/) adapter for PostgreSQL. 

The code is simple, modular and extensible, with most of the intelligence
implemented in Python with only a small C wrapper around the PostgreSQL
`libpq` client library. The module is used in several production systems,
and whilst there is little documentation at this stage, most things work
as described in PEP-0249.

Unlike most Python PG adapters, this module uses the newer binary PG
protocol 3 - in many cases this protocol is faster and safer, although
the protocol is less forgiving of implicit type-casting. As an example,
other adapters will accept:

```python
    curs.execute('SELECT * FROM foo WHERE bah > %s', '2006-1-1')
```

whereas protocol 3 requires:

```python
    curs.execute('SELECT * FROM foo WHERE bah > %s', datetime.datetime(2006,1,1))
```

or an explicit cast:

```python
    curs.execute('SELECT * FROM foo WHERE bah > %s::timestamp', '2006-1-1')
```

**ocpgdb** development was sponsored by [Object Craft] (http://www.object-craft.com.au/).

## Requirements

ocpgdb requires Python 2.3 or newer, and PostgreSQL 8.1 or newer. If
[mx.DateTime] (http://www.egenix.com/products/python/mxBase/mxDateTime/)
is available, `use_mx_datetime=True` can be passed to the connect()
function to enable support.

## Installation

To install, unpack the tar file, and as root run:

    python setup.py install

Alternatively, the latest stable version can be install via PIP:

    pip install ocpgdb
