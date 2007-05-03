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
	PyObject 	*host;
	PyObject 	*port;
	PyObject 	*db;
	PyObject 	*tty;
	PyObject 	*user;
	PyObject 	*password;
	PyObject 	*options;
	PyObject 	*socket;
	PyObject 	*protocolVersion;
	PyObject 	*serverVersion;
	PyObject 	*notices;
} PQConnection;

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


extern void pqconnection_init(PyObject *module);
extern void pqexception_init(PyObject *module);
