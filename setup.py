#!/usr/bin/env python
#
# Usage: python setup.py install
#

import sys, os
import re
from distutils.core import setup, Extension
from distutils import sysconfig

COMMANDS = {}
if sys.version_info[0] == 3:
    from distutils.command.build_py import build_py_2to3
    COMMANDS['build_py'] = build_py_2to3


def collect(cmd, help):
    config = {}
    f = os.popen(cmd)
    try:
        for line in f:
            key, value = line.split('=', 1)
            config[key.strip()] = value.strip()
    finally:
        if f.close():
            sys.exit('%s failed - %s' % (cmd, help))
    return config


# Collect PG compile options
pg_config = collect('pg_config', '''\
you may need to install the postgres client library
package (libpq-dev or similar)''')

NAME = 'ocpgdb'
DESCRIPTION = 'A simple and safe PostgreSQL DB-API 2 adapter'
AUTHOR = 'Andrew McNamara', 'andrewm@object-craft.com.au'
HOMEPAGE = 'http://code.google.com/p/ocpgdb/'
DOWNLOAD = 'http://code.google.com/p/ocpgdb/downloads/list'
PG_INCL_DIR = pg_config['INCLUDEDIR']
PG_LIB_DIR = pg_config['LIBDIR']
# Extract __version__ from module
buf = open('ocpgdb/__init__.py').read()
VERSION = re.search("__version__ = '(.*)'", buf).group(1)

sources = [
    'oclibpq/oclibpq.c',
    'oclibpq/pqconstants.c',
    'oclibpq/pqconnection.c',
    'oclibpq/pqexception.c',
    'oclibpq/pqresult.c',
    'oclibpq/pqcell.c',
    'oclibpq/bytea.c',
    ]

includes = [
    PG_INCL_DIR,
    'oclibpq',
    ]

defines = [
    ]

library_dirs = [
    PG_LIB_DIR,
]

libraries = [
    'pq',
]

# --------------------------------------------------------------------
# distutils declarations

oclibpq_module = Extension(
    'oclibpq', sources,
    define_macros=defines,
    include_dirs=includes,
    library_dirs=library_dirs,
    libraries=libraries,
#    extra_compile_args=["-pg"],
#    extra_link_args=["-pg"],
)

setup(
    author=AUTHOR[0],
    author_email=AUTHOR[1],
    description=DESCRIPTION,
    download_url=DOWNLOAD,
    packages=['ocpgdb'],
    ext_modules=[oclibpq_module],
    license='BSD',
    long_description=DESCRIPTION,
    name=NAME,
    platforms='Python 2.3 and later, PostgreSQL 8.0 and later',
    url=HOMEPAGE,
    version=VERSION,
    cmdclass=COMMANDS,
)
