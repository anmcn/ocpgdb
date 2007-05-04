/* vi:set sw=8 ts=8 noet showmode ai: */

/* Python */
#include <Python.h>
#include <structmember.h>

/* PostgreSQL */
#include <libpq-fe.h>
#include <libpq/libpq-fs.h>

#define MODULE_NAME "oclibpq"

enum result_type {
	RESULT_DQL,
	RESULT_DDL,
	RESULT_DML,
	RESULT_EMPTY,
};

typedef struct {
	PyObject_HEAD
	PGconn		*connection;
	PyObject 	*conninfo;
	PyObject 	*notices;
} PyPgConnection;

typedef struct {
	PyObject_HEAD
	PyPgConnection	*connection;
	PGresult	*result;
	enum result_type type;
} PyPgResult;

extern PyObject *PqErr_Warning;
extern PyObject *PqErr_Error;
extern PyObject *PqErr_InterfaceError;
extern PyObject *PqErr_DatabaseError;
extern PyObject *PqErr_DataError;
extern PyObject *PqErr_OperationalError;
extern PyObject *PqErr_IntegrityError;
extern PyObject *PqErr_InternalError;
extern PyObject *PqErr_ProgrammingError;
extern PyObject *PqErr_NotSupportedError;


/* pqconnection.c */
extern void pg_connection_init(PyObject *module);
/* pqresult.c */
extern void pg_result_init(PyObject *module);
extern PyObject *PyPgResult_New(PyPgConnection *connection, PGresult *result);
/* pqexception.c */
extern void pg_exception_init(PyObject *module);
