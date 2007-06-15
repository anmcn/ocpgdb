/* vi:set sw=8 ts=8 noet showmode ai: */

#include "oclibpq.h"

static int
_not_open(PyPgConnection *self)
{
	if (self->connection == NULL) {
		PyErr_SetString(PqErr_ProgrammingError, 
				"Database connection not open");
		return 1;
	}
	return 0;
}

static int
PyPgConnection_init(PyObject *o, PyObject *args, PyObject *kwds)
{
	PyPgConnection *self = (PyPgConnection *)o;
	char	*conninfo;
	PGconn	*cnx;

	assert(self->connection == NULL);

	if (kwds != NULL) {
		int i = PyObject_Length(kwds);
		if (i < 0)
			return -1;
		else if (i > 0) {
			PyErr_SetString(PyExc_TypeError,
			    MODULE_NAME ".PgConnection takes "
			    "no keyword arguments");
			return -1;
		}
	}
	if (!PyArg_ParseTuple(args, "s:PyPgConnection", &conninfo))
		return -1;

	Py_BEGIN_ALLOW_THREADS
	cnx = PQconnectdb(conninfo);
	Py_END_ALLOW_THREADS

	if (cnx == NULL) {
		PyErr_SetString(PyExc_MemoryError,
			"Can't allocate new PGconn structure in PyPgConnection");
		return -1;
	}

	if (PQstatus(cnx) != CONNECTION_OK) {
		PyErr_SetString(PqErr_DatabaseError, PQerrorMessage(cnx));
		PQfinish(cnx);
		return -1;
	}

	if (PQprotocolVersion(cnx) < 3) {
		PyErr_SetString(PqErr_DatabaseError, MODULE_NAME " requires "
				"protocol 3 or greater server (7.4 and up)");
		PQfinish(cnx);
		return -1;
	}
	/*
	 * Serious issues exist with 7.4's protocol 3 implementation:
	 *    - no way t determine if server is using float or int datetimes
	 *    - cursors with parameters fail when fetching (bind lost)
	 */
	if (PQserverVersion(cnx) < 80000) {
		PyErr_SetString(PqErr_DatabaseError, MODULE_NAME " does not "
				"support pre v8 servers");
		PQfinish(cnx);
		return -1;
	}

	self->connection = cnx;
	if ((self->conninfo = PyString_FromString(conninfo)) == NULL)
		return -1;
	if ((self->notices = PyList_New(0)) == NULL)
		return -1;
	return 0;
}

static int
PyPgConnection_traverse(PyObject *o, visitproc visit, void *arg)
{
	PyPgConnection *self = (PyPgConnection *)o;
	Py_VISIT(self->notices);
	return 0;
}

static int
PyPgConnection_clear(PyObject *o)
{
	PyPgConnection *self = (PyPgConnection *)o;
	Py_CLEAR(self->notices);
	return 0;
}

static void
_finish(PyPgConnection *self)
{
	PGconn *cnx = self->connection;
	if (cnx != NULL) {
		self->connection = NULL;
		Py_BEGIN_ALLOW_THREADS
		PQfinish(cnx);
		Py_END_ALLOW_THREADS
	}
}

static void
PyPgConnection_dealloc(PyObject *o)
{
	PyPgConnection *self = (PyPgConnection *)o;

	PyObject_GC_UnTrack(o);
	_finish(self);
	Py_XDECREF(self->conninfo);
	Py_XDECREF(self->notices);
	o->ob_type->tp_free(o);
}

static PyObject *
connection_close(PyPgConnection *self, PyObject *unused) 
{
	if (_not_open(self)) return NULL;
	_finish(self);
	Py_INCREF(Py_None);
	return Py_None;
}

/* Extract 3-tuple parameters */
static int
_param_tuple(PyObject *param, Oid *oid, char **str, int *len)
{
	PyObject *o;
	Py_ssize_t l;

	if (PyTuple_Size(param) != 2) {
		PyErr_SetString(PyExc_TypeError, 
			"parameters must be a 2-tuple");
		return -1;
	}

	o = PyTuple_GET_ITEM(param, 0);
	if (!PyInt_Check(o)) {
		PyErr_SetString(PyExc_TypeError, 
			"parameter 2-tuple element 0 must be integer OID");
		return -1;
	}
	*oid = PyInt_AsLong(o);
	o = PyTuple_GET_ITEM(param, 1);
	if (!PyString_Check(o)) {
		PyErr_SetString(PyExc_TypeError, 
			"parameter 2-tuple element 1 must be str type");
		return -1;
	}
	if (PyString_AsStringAndSize(o, str, &l) < 0)
		return -1;
	*len = l;
	return 0;
}

static PyObject *
connection_execute(PyPgConnection *self, PyObject *args) 
{
	char *query;
	PyObject *params, *param;
	int nParams;
	int n;
	Oid *paramTypes = NULL;
	char **paramValues = NULL;
	int *paramLengths = NULL;
	int *paramFormats = NULL;
	PGresult *res;
	PyObject *result = NULL;


	if (_not_open(self)) return NULL;

	if (!PyArg_ParseTuple(args,"sO:execute", &query, &params)) 
		return NULL;

	if (!PySequence_Check(params))
	{
		PyErr_SetString(PyExc_TypeError, 
				"execute parameters must be a sequence");
		return NULL;
	}

	nParams = PySequence_Length(params);

	paramTypes = PyMem_Malloc(nParams * sizeof(Oid));
	if (paramTypes == NULL)
		return PyErr_NoMemory();
	paramValues = PyMem_Malloc(nParams * sizeof(char *));
	if (paramValues == NULL)
		return PyErr_NoMemory();
	paramLengths = PyMem_Malloc(nParams * sizeof(int));
	if (paramLengths == NULL)
		return PyErr_NoMemory();
	paramFormats = PyMem_Malloc(nParams * sizeof(int));
	if (paramFormats == NULL)
		return PyErr_NoMemory();

	for (n = 0; n < nParams; ++n)
	{
		paramTypes[n] = 0;
		paramValues[n] = NULL;
		paramLengths[n] = 0;
		paramFormats[n] = 0;
		param = PySequence_GetItem(params, n);
		if (param == NULL)
			goto error;
		if (param == Py_None) {
			/* */
		} else if (PyTuple_Check(param)) {
			if (_param_tuple(param, &paramTypes[n], &paramValues[n],
					 &paramLengths[n]) < 0) {
				Py_DECREF(param);
				goto error;
			}
			paramFormats[n] = 1;
		} else {
			char *str;
			if ((str = PyString_AsString(param)) == NULL) {
				Py_DECREF(param);
				goto error;
			}
			paramValues[n] = str;
		}
		Py_DECREF(param);
	}

	Py_BEGIN_ALLOW_THREADS
	res = PQexecParams(self->connection, query, nParams, paramTypes,
			   (const char **)paramValues, paramLengths, 
			   paramFormats, 1);
	Py_END_ALLOW_THREADS

	result = PyPgResult_New(self, res);

error:
	if (paramFormats != NULL)
		PyMem_Free(paramFormats);
	if (paramLengths != NULL)
		PyMem_Free(paramLengths);
	if (paramValues != NULL)
		PyMem_Free(paramValues);
	if (paramTypes != NULL)
		PyMem_Free(paramTypes);

	return result;
}

static PyObject *
connection_fileno(PyPgConnection *self, PyObject *unused)
{
	if (_not_open(self)) return NULL;
	return PyInt_FromLong(PQsocket(self->connection));
}

static PyObject *
get_closed(PyPgConnection *self)
{
	PyObject *res = self->connection ? Py_False : Py_True;
	Py_INCREF(res);
	return res;
}

static PyObject *
get_host(PyPgConnection *self)
{
	const char *host;
	if (_not_open(self)) return NULL;
	host = PQhost(self->connection);
	return PyString_FromString((host && *host) ? host : "localhost");
}

static PyObject *
get_port(PyPgConnection *self)
{
	if (_not_open(self)) return NULL;
	return PyInt_FromString(PQport(self->connection), NULL, 10);
}

static PyObject *
get_db(PyPgConnection *self)
{
	if (_not_open(self)) return NULL;
	return PyString_FromString(PQdb(self->connection));
}

static PyObject *
get_tty(PyPgConnection *self)
{
	if (_not_open(self)) return NULL;
	return PyString_FromString(PQtty(self->connection));
}

static PyObject *
get_user(PyPgConnection *self)
{
	if (_not_open(self)) return NULL;
	return PyString_FromString(PQuser(self->connection));
}

static PyObject *
get_password(PyPgConnection *self)
{
	if (_not_open(self)) return NULL;
	return PyString_FromString(PQpass(self->connection));
}

static PyObject *
get_options(PyPgConnection *self)
{
	if (_not_open(self)) return NULL;
	return PyString_FromString(PQoptions(self->connection));
}

static PyObject *
get_protocolVersion(PyPgConnection *self)
{
	if (_not_open(self)) return NULL;
	return PyInt_FromLong(PQprotocolVersion(self->connection));
}

static PyObject *
get_serverVersion(PyPgConnection *self)
{
	if (_not_open(self)) return NULL;
	return PyInt_FromLong(PQserverVersion(self->connection));
}

static PyObject *
get_transactionStatus(PyPgConnection *self)
{
	const char *status;
	if (_not_open(self)) return NULL;
	switch (PQtransactionStatus(self->connection)) {
	case PQTRANS_IDLE:	status = "IDLE"; break;
	case PQTRANS_ACTIVE:	status = "ACTIVE"; break;
	case PQTRANS_INTRANS:	status = "INTRANS"; break;
	case PQTRANS_INERROR:	status = "INERROR"; break;
	case PQTRANS_UNKNOWN:
	default:		status = "UNKNOWN"; break;
	}
	return PyString_FromString(status);
}

static PyObject *
_get_parameter(PyPgConnection *self, char *parameter)
{
	const char *value;

	if (_not_open(self)) return NULL;
	value = PQparameterStatus(self->connection, parameter);
	if (value == NULL) {
		Py_INCREF(Py_None);
		return Py_None;
	}
	else
		return PyString_FromString(value);
}

static PyObject *
get_client_encoding(PyPgConnection *self)
{
	return _get_parameter(self, "client_encoding");
}

static PyObject *
get_integer_datetimes(PyPgConnection *self)
{
	return _get_parameter(self, "integer_datetimes");
}



static PyMethodDef PyPgConnection_methods[] = {
	{"close", (PyCFunction)connection_close, METH_NOARGS,
		PyDoc_STR("Close the connection")},
	{"execute", (PyCFunction)connection_execute, METH_VARARGS,
		PyDoc_STR("Execute an SQL command")},
	{"fileno", (PyCFunction)connection_fileno, METH_NOARGS,
		PyDoc_STR("Returns socket file descriptor")},
	{NULL, NULL}
};

#define MO(m) offsetof(PyPgConnection, m)
static PyMemberDef PyPgConnection_members[] = {
	{"conninfo",	T_OBJECT,	MO(conninfo),	RO },
	{"notices",	T_OBJECT,	MO(notices),	RO },
	{NULL}
};
#undef MO

static PyGetSetDef PyPgConnection_getset[] = {
	{"client_encoding",	(getter)get_client_encoding},
	{"closed",		(getter)get_closed},
	{"db",			(getter)get_db},
	{"host",		(getter)get_host},
	{"integer_datetimes",	(getter)get_integer_datetimes},
	{"options",		(getter)get_options},
	{"password",		(getter)get_password},
	{"port",		(getter)get_port},
	{"protocolVersion",	(getter)get_protocolVersion},
	{"serverVersion",	(getter)get_serverVersion},
	{"transactionStatus",	(getter)get_transactionStatus, NULL,
   "Returns the current in-transaction status of the server.\n\n"
   "The status can be IDLE (currently idle), ACTIVE (a command is in\n"
   "progress), INTRANS (idle, in a valid transaction block), or INERROR\n"
   "(idle, in a failed transaction block). UNKNOWN is reported if the\n"
   "connection is bad. ACTIVE is reported only when a query has been sent\n"
   "to the server and not yet completed."},
	{"tty",			(getter)get_tty},
	{"user",		(getter)get_user},
	{NULL}
};

static char PyPgConnection_doc[] = "XXX PgConnection objects";

static PyTypeObject PyPgConnection_Type = {
	PyObject_HEAD_INIT(NULL)
	0,					/* ob_size */
	MODULE_NAME ".PgConnection",		/* tp_name */
	sizeof(PyPgConnection),			/* tp_basicsize */
	0,					/* tp_itemsize */
	PyPgConnection_dealloc,			/* tp_dealloc */
	0,					/* tp_print */
	0,					/* tp_getattr */
	0,					/* tp_setattr */
	0,					/* tp_compare */
	0,					/* tp_repr */
	0,					/* tp_as_number */
	0,					/* tp_as_sequence */
	0,					/* tp_as_mapping */
	0,					/* tp_hash */
	0,					/* tp_call */
	0,					/* tp_str */
	0,					/* tp_getattro */
	0,					/* tp_setattro */
	0,					/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_GC,
						/* tp_flags */
	PyPgConnection_doc,			/* tp_doc */
	PyPgConnection_traverse,			/* tp_traverse */
	PyPgConnection_clear,			/* tp_clear */
	0,					/* tp_richcompare */
	0,					/* tp_weaklistoffset */
	0,					/* tp_iter */
	0,					/* tp_iternext */
	PyPgConnection_methods,			/* tp_methods */
	PyPgConnection_members,			/* tp_members */
	PyPgConnection_getset,			/* tp_getset */
	0,					/* tp_base */
	0,					/* tp_dict */
	0,					/* tp_descr_get */
	0,					/* tp_descr_set */
	0,					/* tp_dictoffset */
	PyPgConnection_init,			/* tp_init */
	PyType_GenericAlloc,			/* tp_alloc */
	PyType_GenericNew,			/* tp_new */
	PyObject_GC_Del,			/* tp_free */
};

void
pg_connection_init(PyObject *module)
{
	if (PyType_Ready(&PyPgConnection_Type) < 0)
		return;
#define ADDEXC(n, v) \
	Py_INCREF(v); \
	if (PyDict_SetItemString(PyPgConnection_Type.tp_dict, n, v) < 0)\
		return;
	ADDEXC("Warning", PqErr_Warning)
	ADDEXC("Error", PqErr_Error)
	ADDEXC("InterfaceError", PqErr_InterfaceError)
	ADDEXC("DatabaseError", PqErr_DatabaseError)
	ADDEXC("DataError", PqErr_DataError)
	ADDEXC("OperationalError", PqErr_OperationalError)
	ADDEXC("IntegrityError", PqErr_IntegrityError)
	ADDEXC("InternalError", PqErr_InternalError)
	ADDEXC("ProgrammingError", PqErr_ProgrammingError)
	ADDEXC("NotSupportedError", PqErr_NotSupportedError)
#undef ADDEXC
	Py_INCREF(&PyPgConnection_Type);
	PyModule_AddObject(module, "PgConnection", 
			   (PyObject *)&PyPgConnection_Type);
}
