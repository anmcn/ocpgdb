/* vi:set sw=8 ts=8 noet showmode ai: */

#include "oclibpq.h"

static void
PQResult_dealloc(PyObject *o)
{
	PQResult *self = (PQResult *)o;
	PGresult *res = self->result;

	if (res != NULL) {
		self->result = NULL;
		PQclear(res);
	}
	Py_XDECREF(self->connection);

	o->ob_type->tp_free(o);
}


static PyMethodDef PQResult_methods[] = {
	{NULL, NULL}
};

static PyMemberDef PQResult_members[] = {
	{NULL}
};

static PyGetSetDef PQResult_getset[] = {
	{NULL}
};

static char PQResult_doc[] = "XXX PQResult objects";

static PyTypeObject PQResult_Type = {
	PyObject_HEAD_INIT(NULL)
	0,					/* ob_size */
	MODULE_NAME ".PQResult",		/* tp_name */
	sizeof(PQResult),			/* tp_basicsize */
	0,					/* tp_itemsize */
	PQResult_dealloc,			/* tp_dealloc */
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
	PQResult_doc,				/* tp_doc */
	0,					/* tp_traverse */
	0,					/* tp_clear */
	0,					/* tp_richcompare */
	0,					/* tp_weaklistoffset */
	0,					/* tp_iter */
	0,					/* tp_iternext */
	PQResult_methods,			/* tp_methods */
	PQResult_members,			/* tp_members */
	PQResult_getset,			/* tp_getset */
	0,					/* tp_base */
	0,					/* tp_dict */
	0,					/* tp_descr_get */
	0,					/* tp_descr_set */
	0,					/* tp_dictoffset */
	0,					/* tp_init */
	0,					/* tp_alloc */
	0,					/* tp_new */
	0,					/* tp_free */
};

static PyObject *
PQResult_New(PQConnection *connection, PGresult *result)
{
	PQResult *self = (PQResult *)PyObject_New(PQResult, &PQResult_Type);

	if (self == NULL) 
		return NULL;
	Py_INCREF(connection);
	self->connection = connection;
	self->result = result;
	return (PyObject *)self;
}

void
pqresult_init(PyObject *module)
{
	if (PyType_Ready(&PQResult_Type) < 0)
		return;
/*
	Py_INCREF(&PQResult_Type);
	PyModule_AddObject(module, "PQResult", 
			   (PyObject *)&PQResult_Type);
 */
}

