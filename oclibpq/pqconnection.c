/* vi:set sw=8 ts=8 noet showmode ai: */

#include <Python.h>
#include <structmember.h>
#include "oclibpq.h"

static PyObject *
PQConnection_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
	PQConnection *self;

	self = (PQConnection *)type->tp_alloc(type, 0);
	if (self == NULL)
		return NULL;

	self->connection = NULL;

	return (PyObject *)self;
}

static int
set_member(PyObject **member, const char *fmt, char *value)
{
	if (value == NULL || *value == '\0')
		fmt = "";
	*member = Py_BuildValue(fmt, value);
	if (*member == NULL)
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
	if (set_member(&self->conninfo, "s", conninfo) < 0)
		return -1;
	if (set_member(&self->host, "s", PQhost(cnx)) < 0)
		return -1;
	if (set_member(&self->port, "b", PQport(cnx)) < 0)
		return -1;
	if (set_member(&self->db, "s", PQdb(cnx)) < 0)
		return -1;
	if (set_member(&self->tty, "s", PQtty(cnx)) < 0)
		return -1;
	if (set_member(&self->user, "s", PQuser(cnx)) < 0)
		return -1;
	if (set_member(&self->password, "s", PQpass(cnx)) < 0)
		return -1;
	if (set_member(&self->options, "s", PQoptions(cnx)) < 0)
		return -1;
	if ((self->socket = Py_BuildValue("i", PQsocket(cnx))) == NULL)
		return -1;
	return 0;
}


static void
PQConnection_dealloc(PyObject *o)
{
	PQConnection	*self = (PQConnection *)o;
	PGconn		*cnx = self->connection;

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
	Py_XDECREF(self->notices);
	o->ob_type->tp_free(o);
}


static PyMethodDef PQConnection_methods[] = {
	{NULL, NULL}
};

#define PQC_MO(m) offsetof(PQConnection, m)
static PyMemberDef PQConnection_members[] = {
	{"host",	T_OBJECT,	PQC_MO(host),		RO },
	{"port",	T_OBJECT,	PQC_MO(port),		RO },
	{"db",		T_OBJECT,	PQC_MO(db),		RO },
	{"tty",		T_OBJECT,	PQC_MO(tty),		RO },
	{"user",	T_OBJECT,	PQC_MO(user),		RO },
	{"password",	T_OBJECT,	PQC_MO(password),	RO },
	{"socket",	T_OBJECT,	PQC_MO(socket),		RO },
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
	Py_TPFLAGS_DEFAULT,			/* tp_flags */
	PQConnection_doc,			/* tp_doc */
	0,					/* tp_traverse */
	0,					/* tp_clear */
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
	0,					/* tp_alloc */
	PQConnection_new,			/* tp_new */
	0,					/* tp_free */
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
