/* vi:set sw=8 ts=8 noet showmode ai: */

#include "oclibpq.h"

static int
set_str(PyObject **member, char *value)
{
	if (value == NULL || *value == '\0') {
		Py_INCREF(Py_None);
		*member = Py_None;
		return 0;
	}
	if ((*member = PyString_FromString(value)) == NULL)
		return -1;
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
	if (set_str(&self->conninfo, conninfo) < 0)
		return -1;
	if (set_str(&self->host, PQhost(cnx)) < 0)
		return -1;
	if ((self->port = PyInt_FromString(PQport(cnx), NULL, 10)) == NULL)
		return -1;
	if (set_str(&self->db, PQdb(cnx)) < 0)
		return -1;
	if (set_str(&self->tty, PQtty(cnx)) < 0)
		return -1;
	if (set_str(&self->user, PQuser(cnx)) < 0)
		return -1;
	if (set_str(&self->password, PQpass(cnx)) < 0)
		return -1;
	if (set_str(&self->options, PQoptions(cnx)) < 0)
		return -1;
	if ((self->socket = PyInt_FromLong(PQsocket(cnx))) == NULL)
		return -1;
	if ((self->protocolVersion = PyInt_FromLong(PQprotocolVersion(cnx))) == NULL)
		return -1;
	if ((self->serverVersion = PyInt_FromLong(PQserverVersion(cnx))) == NULL)
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
PQConnection_dealloc(PyObject *o)
{
	PQConnection *self = (PQConnection *)o;
	PGconn *cnx = self->connection;

	PyObject_GC_UnTrack(o);
	if (cnx != NULL) {
		self->connection = NULL;
		Py_BEGIN_ALLOW_THREADS
		PQfinish(cnx);
		Py_END_ALLOW_THREADS
	}
	Py_XDECREF(self->conninfo);
	Py_XDECREF(self->host);
	Py_XDECREF(self->port);
	Py_XDECREF(self->db);
	Py_XDECREF(self->tty);
	Py_XDECREF(self->user);
	Py_XDECREF(self->password);
	Py_XDECREF(self->options);
	Py_XDECREF(self->socket);
	Py_XDECREF(self->protocolVersion);
	Py_XDECREF(self->serverVersion);
	o->ob_type->tp_free(o);
}


static PyMethodDef PQConnection_methods[] = {
	{NULL, NULL}
};

#define PQC_MO(m) offsetof(PQConnection, m)
static PyMemberDef PQConnection_members[] = {
	{"conninfo",	T_OBJECT,	PQC_MO(conninfo),	RO },
	{"host",	T_OBJECT,	PQC_MO(host),		RO },
	{"port",	T_OBJECT,	PQC_MO(port),		RO },
	{"db",		T_OBJECT,	PQC_MO(db),		RO },
	{"tty",		T_OBJECT,	PQC_MO(tty),		RO },
	{"user",	T_OBJECT,	PQC_MO(user),		RO },
	{"password",	T_OBJECT,	PQC_MO(password),	RO },
	{"socket",	T_OBJECT,	PQC_MO(socket),		RO },
	{"protocolVersion", T_OBJECT,	PQC_MO(protocolVersion),RO },
	{"serverVersion", T_OBJECT,	PQC_MO(serverVersion),	RO },
	{"notices",	T_OBJECT,	PQC_MO(notices),	RO },
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
	0,					/* tp_getset */
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
