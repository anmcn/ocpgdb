/* vi:set sw=8 ts=8 noet showmode ai: */

#include "oclibpq.h"

static int
_PQC_not_open(PQConnection *self)
{
	if (self->connection == NULL) {
		PyErr_SetString(PqErr_ProgrammingError, 
				"Database connection not open");
		return 1;
	}
	return 0;
}

static int
PQConnection_init(PyObject *o, PyObject *args, PyObject *kwds)
{
	PQConnection *self = (PQConnection *)o;
	char	*conninfo;
	PGconn	*cnx;

	assert(self->connection == NULL);

	if (!_PyArg_NoKeywords("PQConnection", kwds))
		return -1;
	if (!PyArg_ParseTuple(args, "s:PQConnection", &conninfo))
		return -1;

	Py_BEGIN_ALLOW_THREADS
	cnx = PQconnectdb(conninfo);
	Py_END_ALLOW_THREADS

	if (cnx == NULL) {
		PyErr_SetString(PyExc_MemoryError,
			"Can't allocate new PGconn structure in PQConnection");
		return -1;
	}

	if (PQstatus(cnx) != CONNECTION_OK)
	{
		PyErr_SetString(PqErr_DatabaseError, PQerrorMessage(cnx));
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
PQConnection_traverse(PyObject *o, visitproc visit, void *arg)
{
	PQConnection *self = (PQConnection *)o;
	Py_VISIT(self->notices);
	return 0;
}

static int
PQConnection_clear(PyObject *o)
{
	PQConnection *self = (PQConnection *)o;
	Py_CLEAR(self->notices);
	return 0;
}

static void
_PQC_finish(PQConnection *self)
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
PQConnection_dealloc(PyObject *o)
{
	PQConnection *self = (PQConnection *)o;

	PyObject_GC_UnTrack(o);
	_PQC_finish(self);
	Py_XDECREF(self->conninfo);
	Py_XDECREF(self->notices);
	o->ob_type->tp_free(o);
}

static PyObject *
PQConnection_close(PQConnection *self, PyObject *unused) 
{
	_PQC_finish(self);
	Py_INCREF(Py_None);
	return Py_None;
}

static PyObject *
PQConnection_execute(PQConnection *self, PyObject *args) 
{
	return NULL;
}

static PyObject *
PQConnection_fileno(PQConnection *self, PyObject *unused)
{
	if (_PQC_not_open(self)) return NULL;
	return PyInt_FromLong(PQsocket(self->connection));
}

static PyObject *
PQC_get_host(PQConnection *self)
{
	const char *host;
	if (_PQC_not_open(self)) return NULL;
	host = PQhost(self->connection);
	return PyString_FromString((host && *host) ? host : "localhost");
}

static PyObject *
PQC_get_port(PQConnection *self)
{
	if (_PQC_not_open(self)) return NULL;
	return PyInt_FromString(PQport(self->connection), NULL, 10);
}

static PyObject *
PQC_get_db(PQConnection *self)
{
	if (_PQC_not_open(self)) return NULL;
	return PyString_FromString(PQdb(self->connection));
}

static PyObject *
PQC_get_tty(PQConnection *self)
{
	if (_PQC_not_open(self)) return NULL;
	return PyString_FromString(PQtty(self->connection));
}

static PyObject *
PQC_get_user(PQConnection *self)
{
	if (_PQC_not_open(self)) return NULL;
	return PyString_FromString(PQuser(self->connection));
}

static PyObject *
PQC_get_password(PQConnection *self)
{
	if (_PQC_not_open(self)) return NULL;
	return PyString_FromString(PQpass(self->connection));
}

static PyObject *
PQC_get_options(PQConnection *self)
{
	if (_PQC_not_open(self)) return NULL;
	return PyString_FromString(PQoptions(self->connection));
}

static PyObject *
PQC_get_protocolVersion(PQConnection *self)
{
	if (_PQC_not_open(self)) return NULL;
	return PyInt_FromLong(PQprotocolVersion(self->connection));
}

static PyObject *
PQC_get_serverVersion(PQConnection *self)
{
	if (_PQC_not_open(self)) return NULL;
	return PyInt_FromLong(PQserverVersion(self->connection));
}

static PyMethodDef PQConnection_methods[] = {
	{"close", (PyCFunction)PQConnection_close, METH_NOARGS,
		PyDoc_STR("Close the connection")},
	{"execute", (PyCFunction)PQConnection_execute, METH_VARARGS,
		PyDoc_STR("Execute an SQL command")},
	{"fileno", (PyCFunction)PQConnection_fileno, METH_NOARGS,
		PyDoc_STR("Returns socket file descriptor")},
	{NULL, NULL}
};

#define PQC_MO(m) offsetof(PQConnection, m)
static PyMemberDef PQConnection_members[] = {
	{"conninfo",	T_OBJECT,	PQC_MO(conninfo),	RO },
	{"notices",	T_OBJECT,	PQC_MO(notices),	RO },
	{NULL}
};

static PyGetSetDef PQConnection_getset[] = {
	{"host",		(getter)PQC_get_host},
	{"port",		(getter)PQC_get_port},
	{"db",			(getter)PQC_get_db},
	{"tty",			(getter)PQC_get_tty},
	{"user",		(getter)PQC_get_user},
	{"password",		(getter)PQC_get_password},
	{"options",		(getter)PQC_get_options},
	{"protocolVersion",	(getter)PQC_get_protocolVersion},
	{"serverVersion",	(getter)PQC_get_serverVersion},
	{NULL}
};

static char PQConnection_doc[] = "XXX PQConnection objects";

static PyTypeObject PQConnection_Type = {
	PyObject_HEAD_INIT(NULL)
	0,					/* ob_size */
	MODULE_NAME ".PQConnection",		/* tp_name */
	sizeof(PQConnection),			/* tp_basicsize */
	0,					/* tp_itemsize */
	PQConnection_dealloc,			/* tp_dealloc */
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
	PQConnection_doc,			/* tp_doc */
	PQConnection_traverse,			/* tp_traverse */
	PQConnection_clear,			/* tp_clear */
	0,					/* tp_richcompare */
	0,					/* tp_weaklistoffset */
	0,					/* tp_iter */
	0,					/* tp_iternext */
	PQConnection_methods,			/* tp_methods */
	PQConnection_members,			/* tp_members */
	PQConnection_getset,			/* tp_getset */
	0,					/* tp_base */
	0,					/* tp_dict */
	0,					/* tp_descr_get */
	0,					/* tp_descr_set */
	0,					/* tp_dictoffset */
	PQConnection_init,			/* tp_init */
	PyType_GenericAlloc,			/* tp_alloc */
	PyType_GenericNew,			/* tp_new */
	PyObject_GC_Del,			/* tp_free */
};

void
pqconnection_init(PyObject *module)
{
	if (PyType_Ready(&PQConnection_Type) < 0)
		return;
	Py_INCREF(&PQConnection_Type);
	PyModule_AddObject(module, "PQConnection", 
			   (PyObject *)&PQConnection_Type);
}
