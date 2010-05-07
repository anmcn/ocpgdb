/* vi:set sw=8 ts=8 noet showmode ai: */

#include "oclibpq.h"

static void
PyPgCell_dealloc(PyPgCell *self)
{
	Py_DECREF(self->type);
	Py_DECREF(self->name);
	Py_DECREF(self->modifier);
	Py_DECREF(self->format);
	Py_DECREF(self->value);

	Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *
PyPgCell_repr(PyPgCell *self)
{
        PyObject *s;
        PyObject *pieces;
        PyObject *result = NULL;

	/* Modern python's have %S and %R (str and repr) formatting available
	 * with PyUnicode_FromFormat, which is what we achieve the hard way
	 * here. */
	pieces = PyList_New(0);
	if (pieces == NULL)
		return NULL;

#define APPEND(p, n) {\
	s = (n); \
	if (!s) goto done; \
	if (PyList_Append(p, s) < 0) goto done; \
	Py_DECREF(s); \
}

	APPEND(pieces, PyString_FromString("<PyPgCell name "));
	APPEND(pieces, PyObject_Repr(self->name));
	APPEND(pieces, PyString_FromFormat(", type %ld, modifier %ld, value ",
				PyInt_AsLong(self->type),
				PyInt_AsLong(self->modifier)));
	APPEND(pieces, PyObject_Repr(self->value));
	APPEND(pieces, PyString_FromFormat(" at %p>", self));
	s = PyString_FromString("");
	if (!s) goto done;
	result = PyString_Join(s, pieces);
	Py_DECREF(s);
done:
	Py_DECREF(pieces);
	return result;
#undef APPEND
}

#define MO(m) offsetof(PyPgCell, m)
static PyMemberDef PyPgCell_members[] = {
	{"format",	T_OBJECT,	MO(format),	READONLY},
	{"modifier",	T_OBJECT,	MO(modifier),	READONLY},
	{"name",	T_OBJECT,	MO(name),	READONLY},
	{"type",	T_OBJECT,	MO(type),	READONLY},
	{"value",	T_OBJECT,	MO(value),	READONLY},
	{NULL}
};
#undef MO

static char PyPgCell_doc[] = "XXX PgCell objects";

static PyTypeObject PyPgCell_Type = {
	PyVarObject_HEAD_INIT(NULL, 0)
	MODULE_NAME ".PgCell",			/* tp_name */
	sizeof(PyPgCell),			/* tp_basicsize */
	0,					/* tp_itemsize */
	(destructor)PyPgCell_dealloc,		/* tp_dealloc */
	0,					/* tp_print */
	0,					/* tp_getattr */
	0,					/* tp_setattr */
	0,					/* tp_compare */
	(reprfunc)PyPgCell_repr,		/* tp_repr */
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
	PyPgCell_doc,				/* tp_doc */
	0,					/* tp_traverse */
	0,					/* tp_clear */
	0,					/* tp_richcompare */
	0,					/* tp_weaklistoffset */
	0,					/* tp_iter */
	0,					/* tp_iternext */
	0,					/* tp_methods */
	PyPgCell_members,			/* tp_members */
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

PyObject *
PyPgCell_New(PGresult *result, int col)
{
	PyPgCell *self;
	int format;
	char *name;

	format = PQfformat(result, col);
	name = PQfname(result, col);
	if (format != 0 && format != 1) {
		PyErr_Format(PqErr_InternalError, 
			"Column \"%s\": unknown column format: %d", 
			name, format);
		return NULL;
	}

	self = (PyPgCell *)PyObject_New(PyPgCell, &PyPgCell_Type);
	if (self == NULL)
		return NULL;

	Py_INCREF(Py_None);
	self->value = Py_None;

	self->name = self->type = self->modifier = self->format = NULL;

	if (!(self->format = PyInt_FromLong(format)))
		goto error;
	if (!(self->modifier = PyInt_FromLong(PQfmod(result, col))))
		goto error;
	if (!(self->name = PyString_FromString(name)))
		goto error;
	if (!(self->type = PyInt_FromLong(PQftype(result, col))))
		goto error;

	return (PyObject *)self;
error:
	Py_DECREF(self);
	return NULL;
}

PyObject *
PyPgCell_FromCell(PyPgCell *cell, PyObject *value)
{
	PyPgCell *self;

	if (!PyPgCell_Check((PyObject *)cell)) {
		PyErr_SetString(PyExc_TypeError, 
			"PyPgCell_FromCell first parameter must be a PyPgCell");
		return NULL;
	}

	self = (PyPgCell *)PyObject_New(PyPgCell, &PyPgCell_Type);
	if (self == NULL)
		return NULL;

	self->format = cell->format;
	Py_INCREF(cell->format);
	self->modifier = cell->modifier;
	Py_INCREF(cell->modifier);
	self->name = cell->name;
	Py_INCREF(cell->name);
	self->type = cell->type;
	Py_INCREF(cell->type);

	self->value = value;

	return (PyObject *)self;
}

int
PyPgCell_Check(PyObject *op)
{
	return op->ob_type == &PyPgCell_Type;
}

void
pg_cell_init(PyObject *module)
{
	if (PyType_Ready(&PyPgCell_Type) < 0)
		return;
}
