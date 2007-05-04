#!/usr/bin/env python
#
# Usage: python setup.py install
#

from distutils.core import setup, Extension
from distutils import sysconfig
import sys, os

# --------------------------------------------------------------------
# identification

NAME = 'ocpgdb'
VERSION = '1.0.0'
DESCRIPTION = 'A simple and safe PostgreSQL DB-API 2 adapter'
AUTHOR = 'Andrew McNamara', 'andrewm@object-craft.com.au'
HOMEPAGE = 'http://www.object-craft.com.au/projects/ocpgdb/'
DOWNLOAD = 'http://www.object-craft.com.au/projects/ocpgdb/download'
PG_INCL_DIR = os.popen('pg_config --includedir').read().strip()
PG_LIB_DIR = os.popen('pg_config --libdir').read().strip()


sources = [
    'oclibpq/oclibpq.c',
    'oclibpq/pqconnection.c',
    'oclibpq/pqexception.c',
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
    libraries = libraries,
)

setup(
    author=AUTHOR[0],
    author_email=AUTHOR[1],
    description=DESCRIPTION,
    download_url=DOWNLOAD,
    py_modules = ['ocpgdb'],
    ext_modules = [oclibpq_module],
    license='BSD',
    long_description=DESCRIPTION,
    name=NAME,
    platforms='Python 2.3 and later.',
    url=HOMEPAGE,
    version=VERSION,
)
