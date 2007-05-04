/* vi:set sw=8 ts=8 noet showmode ai: */

/* Python */
#include <Python.h>
#include <structmember.h>

/* PostgreSQL */
#include <libpq-fe.h>
#include <libpq/libpq-fs.h>

#define MODULE_NAME "oclibpq"
typedef struct {
	PyObject_HEAD
	PGconn		*connection;
	PyObject 	*conninfo;
	PyObject 	*notices;
} PQConnection;

typedef struct {
	PyObject_HEAD
	PGresult	*result;
	PQConnection	*connection;
} PQResult;

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
extern void pqconnection_init(PyObject *module);
/* pqresult.c */
extern void pqresult_init(PyObject *module);
extern PyObject *PQResult_New(PQConnection *connection, PGresult *result);
/* pqexception.c */
extern void pqexception_init(PyObject *module);
