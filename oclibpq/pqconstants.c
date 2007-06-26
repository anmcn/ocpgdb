/* vi:set sw=8 ts=8 noet showmode ai: */

/*
 * This module implements a simple symbolic constant type. The type is an "int"
 * subclass, but displays with a symbolic name
 */

#include "oclibpq.h"

static int
pgconst_print(PyPgConst *self, FILE *fp, int flags)
{
	fputs(PyString_AsString(self->ob_name), fp);
	return 0;
}


static PyObject *
pgconst_repr(PyPgConst *self)
{
	Py_INCREF(self->ob_name);
	return self->ob_name;
}

PyDoc_STRVAR(pgconst_doc, 
"This type implements a simple symbolic constant. The type is an \"int\"\n"
"subclass, but displays with a symbolic name");

PyTypeObject PyPgConst_Type = {
	PyObject_HEAD_INIT(NULL)
	0,
	MODULE_NAME ".PgConst",
	sizeof(PyPgConst),
	0,
	0,					/* tp_dealloc */
	(printfunc)pgconst_print,		/* tp_print */
	0,					/* tp_getattr */
	0,					/* tp_setattr */
	0,					/* tp_compare */
	(reprfunc)pgconst_repr,			/* tp_repr */
	0,					/* tp_as_number */
	0,					/* tp_as_sequence */
	0,					/* tp_as_mapping */
	0,					/* tp_hash */
	0,					/* tp_call */
	(reprfunc)pgconst_repr,			/* tp_str */
	0,					/* tp_getattro */
	0,					/* tp_setattro */
	0,					/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_CHECKTYPES, /* tp_flags */
	pgconst_doc,				/* tp_doc */
	0,					/* tp_traverse */
	0,					/* tp_clear */
	0,					/* tp_richcompare */
	0,					/* tp_weaklistoffset */
	0,					/* tp_iter */
	0,					/* tp_iternext */
	0,					/* tp_methods */
	0,					/* tp_members */
	0,					/* tp_getset */
	&PyInt_Type,				/* tp_base */
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
pgconst_new(PyObject *name, int value)
{
	PyPgConst *self;
	self = (PyPgConst *)PyObject_New(PyPgConst, &PyPgConst_Type);
	if (self != NULL) {
		self->ob_ival = value;
		self->ob_name = name;
	}
	return (PyObject *)self;
}

PyObject *
set_module_const(PyObject *module, const char *name_str, int value)
{
	PyObject *name;
	PyObject *o;

	if ((name = PyString_InternFromString(name_str)) == NULL)
		return NULL;
	if ((o = pgconst_new(name, value)) == NULL) {
		Py_DECREF(name);
		return NULL;
	}
	if (PyObject_SetAttr(module, name, o) < 0) {
		Py_DECREF(name);
		Py_DECREF(o);
		return NULL;
	}
	return o;
}

void
pg_constants_init(PyObject *m)
{
	if (PyType_Ready(&PyPgConst_Type) < 0)
		return;
}
