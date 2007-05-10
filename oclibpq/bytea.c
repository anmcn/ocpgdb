/* vi:set sw=8 ts=8 noet showmode ai: */
#include "oclibpq.h"

static char PyPgBytea_doc[] = "XXX bytea objects";

static PyTypeObject PyPgBytea_Type = {
	PyObject_HEAD_INIT(NULL)
	0,					/* ob_size */
	MODULE_NAME ".bytea",			/* tp_name */
	sizeof(PyPgBytea),			/* tp_basicsize */
	0,					/* tp_itemsize */
	0,					/* tp_dealloc */
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
	Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE, /* tp_flags */
	PyPgBytea_doc,				/* tp_doc */
	0,					/* tp_traverse */
	0,					/* tp_clear */
	0,					/* tp_richcompare */
	0,					/* tp_weaklistoffset */
	0,					/* tp_iter */
	0,					/* tp_iternext */
	0,					/* tp_methods */
	0,					/* tp_members */
	0,					/* tp_getset */
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

int
PyPgBytea_Check(PyObject *op)
{
	return op->ob_type == &PyPgBytea_Type;
}

void
pg_bytea_init(PyObject *module)
{
	PyPgBytea_Type.tp_base = &PyString_Type;
	if (PyType_Ready(&PyPgBytea_Type) < 0)
		return;
	Py_INCREF(&PyPgBytea_Type);
	PyModule_AddObject(module, "bytea", 
			   (PyObject *)&PyPgBytea_Type);
}
