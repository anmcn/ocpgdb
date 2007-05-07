/* vi:set sw=8 ts=8 noet showmode ai: */

#include "oclibpq.h"

static void
PyPgCell_dealloc(PyPgCell *self)
{
	Py_DECREF(self->name);
	Py_DECREF(self->type);
	Py_DECREF(self->modifier);
	Py_DECREF(self->value);

	self->ob_type->tp_free((PyObject *)self);
}

#define MO(m) offsetof(PyPgCell, m)
static PyMemberDef PyPgCell_members[] = {
	{"name",	T_OBJECT,	MO(name),	RO},
	{"type",	T_OBJECT,	MO(type),	RO},
	{"modifier",	T_OBJECT,	MO(modifier),	RO},
	{"value",	T_OBJECT,	MO(value),	RO},
	{NULL}
};
#undef MO

static char PyPgCell_doc[] = "XXX PgCell objects";

static PyTypeObject PyPgCell_Type = {
	PyObject_HEAD_INIT(NULL)
	0,					/* ob_size */
	MODULE_NAME ".PgCell",		/* tp_name */
	sizeof(PyPgCell),			/* tp_basicsize */
	0,					/* tp_itemsize */
	(destructor)PyPgCell_dealloc,		/* tp_dealloc */
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
	PyPgCell_doc,				/* tp_doc */
	0,					/* tp_traverse */
	0,					/* tp_clear */
	0,					/* tp_richcompare */
	0,					/* tp_weaklistoffset */
	0,              			/* tp_iter */
	0,                              	/* tp_iternext */
	0,              			/* tp_methods */
	PyPgCell_members,			/* tp_members */
	0,              			/* tp_getset */
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
PyPgCell_New(PyObject *name, PyObject *type, PyObject *modifier)
{
	/* Steals a reference from caller for name, type and modifier */
	PyPgCell *self;

	self = (PyPgCell *)PyObject_New(PyPgCell, &PyPgCell_Type);
	if (self == NULL)
		return NULL;

	self->name = name;
        self->type = type;
        self->modifier = modifier;

        Py_INCREF(Py_None);
        self->value = Py_None;

	return (PyObject *)self;
}

PyObject *
PyPgCell_FromCell(PyObject *cell_object, PyObject *value)
{
	PyPgCell *self;
	PyPgCell *cell = (PyPgCell *)cell_object;

	if (!PyPgCell_Check(cell_object)) {
		PyErr_SetString(PyExc_TypeError, 
			"PyPgCell_FromCell first parameter must be a PyPgCell");
		return NULL;
	}

	self = (PyPgCell *)PyObject_New(PyPgCell, &PyPgCell_Type);
	if (self == NULL)
		return NULL;

        Py_INCREF(cell->name);
	self->name = cell->name;
        Py_INCREF(cell->type);
        self->type = cell->type;
        Py_INCREF(cell->modifier);
        self->modifier = cell->modifier;

        self->value = value;

	return (PyObject *)self;
}

void
pg_cell_init(PyObject *module)
{
	if (PyType_Ready(&PyPgCell_Type) < 0)
		return;
}
