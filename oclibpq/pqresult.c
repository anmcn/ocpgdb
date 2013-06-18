/* vi:set sw=8 ts=8 noet showmode ai: */

#include "oclibpq.h"

// PQresultStatus
static PyPgConstEnum *PyPg_PGRES;
// PQresultErrorField
static PyPgConstEnum *PyPg_DIAG;

static void
PyPgResult_dealloc(PyObject *o)
{
	PyPgResult *self = (PyPgResult *)o;
	PGresult *res = self->result;

	if (res != NULL) {
		self->result = NULL;
		PQclear(res);
	}
	Py_XDECREF(self->status);
	Py_XDECREF(self->connection);
	Py_XDECREF(self->columns);

	o->ob_type->tp_free(o);
}

static PyObject *
PyPgResult_iter(PyObject *self)
{
	Py_INCREF(self);
	return self;
}

static PyObject *
get_ntuples(PyPgResult *self)
{
	return PyInt_FromLong(PQntuples(self->result));
}

static PyObject *
get_nfields(PyPgResult *self)
{
	return PyInt_FromLong(PQnfields(self->result));
}

static PyObject *
get_binaryTuples(PyPgResult *self)
{
	return PyInt_FromLong(PQbinaryTuples(self->result));
}

static PyObject *
get_cmdStatus(PyPgResult *self)
{
	const char *cmdStatus = PQcmdStatus(self->result);
	if (cmdStatus)
		return PyString_FromString(cmdStatus);
	Py_INCREF(Py_None);
	return Py_None;
}

static PyObject *
get_cmdTuples(PyPgResult *self)
{
	char *cmdTuples = PQcmdTuples(self->result);
	if (cmdTuples && *cmdTuples)
		return PyInt_FromString(cmdTuples, NULL, 10);
	Py_INCREF(Py_None);
	return Py_None;
}

static PyObject *
get_oid(PyPgResult *self)
{
	long oid = PQoidValue(self->result);
	if (oid != InvalidOid)
		return PyInt_FromLong(oid);
	Py_INCREF(Py_None);
	return Py_None;
}

static PyObject *
get_errorMessage(PyPgResult *self)
{
	return PyString_FromString(PQresultErrorMessage(self->result));
}

static PyObject *
get_columns(PyPgResult *self)
{
	if (self->columns == NULL) {
		PGresult *result = self->result;
		int ncolumns = PQnfields(result);
		PyObject *columns, *cell;
		int i;

		columns = PyTuple_New(ncolumns);
		if (columns == NULL)
			return NULL;
		for (i = 0; i < ncolumns; ++i) {
			if ((cell = PyPgCell_New(result, i)) == NULL) {
				Py_DECREF(columns);
				return NULL;
			}
			PyTuple_SET_ITEM(columns, i, cell);
		}
		self->columns = columns;
	}
	Py_INCREF(self->columns);
	return self->columns;
}

static PyObject *
PyPgResult_iternext(PyPgResult *self)
{
	PyObject *row, *cell_value, *cell, *columns;
	PyPgCell *master_cell;
	char *value;
	size_t len;
	int i;
	int format;
	int ncolumns;
	PGresult *result = self->result;
	int row_number = self->row_number;

	if (row_number >= self->row_count)
		return NULL;

	if ((columns = get_columns(self)) == NULL)
		return NULL;
	ncolumns = PyTuple_GET_SIZE(columns);

	if ((row = PyTuple_New(ncolumns)) == NULL)
		return NULL;

	for (i = 0; i < ncolumns; ++i) {
		master_cell = (PyPgCell *)PyTuple_GET_ITEM(columns, i);
		format = PyInt_AsLong(master_cell->format);
		if (PQgetisnull(result, row_number, i)) {
			Py_INCREF(Py_None);
			cell_value = Py_None;
		} else if (format == 0) {
			value = PQgetvalue(result, row_number, i);
			value = (char *)PQunescapeBytea((unsigned char *)value, &len);
			cell_value = PyBytes_FromStringAndSize(value, len);
			PQfreemem(value);
			if (cell_value == NULL)
				goto failed;
		} else if (format == 1) {
			value = PQgetvalue(result, row_number, i);
			len = PQgetlength(result, row_number, i);
			cell_value = PyBytes_FromStringAndSize(value, len);
			if (cell_value == NULL)
				goto failed;
		} else {
			PyErr_Format(PqErr_InternalError, 
				"Result cell format %d not supported", format);
			goto failed;
		}
		cell = PyPgCell_FromCell(master_cell, cell_value);
		if (cell == NULL) {
			Py_DECREF(cell_value);
			goto failed;
		}
		PyTuple_SET_ITEM(row, i, cell);
	}
	Py_DECREF(columns);
	++self->row_number;
	return row;
failed:
	Py_DECREF(row);
	Py_DECREF(columns);
	return NULL;
}

static PyObject *
get_errorField(PyPgResult *self, PyObject *args)
{
	int fieldcode;
	char *errorfield;

	if (!PyArg_ParseTuple(args, "i:errorField", &fieldcode)) 
		return NULL;
	errorfield = PQresultErrorField(self->result, fieldcode);
	if (errorfield == NULL) {
		Py_INCREF(Py_None);
		return Py_None;
	}
	return PyString_FromString(errorfield);
}

static PyMethodDef PyPgResult_methods[] = {
	{"errorField", (PyCFunction)get_errorField, METH_VARARGS,
		PyDoc_STR("Returns an individual field of an error report.")},
	{NULL, NULL}
};

#define MO(m) offsetof(PyPgResult, m)
static PyMemberDef PyPgResult_members[] = {
	{"status",	T_OBJECT,	MO(status),	READONLY},
	{NULL}
};
#undef MO

static PyGetSetDef PyPgResult_getset[] = {
	{"binaryTuples",	(getter)get_binaryTuples},
	{"cmdStatus",		(getter)get_cmdStatus},
	{"cmdTuples",		(getter)get_cmdTuples},
	{"columns",		(getter)get_columns},
	{"errorMessage",	(getter)get_errorMessage},
	{"nfields",		(getter)get_nfields},
	{"ntuples",		(getter)get_ntuples},
	{"oid",			(getter)get_oid},
	{NULL}
};

static char PyPgResult_doc[] = "XXX PgResult objects";

static PyTypeObject PyPgResult_Type = {
	PyVarObject_HEAD_INIT(NULL, 0)
	MODULE_NAME ".PgResult",		/* tp_name */
	sizeof(PyPgResult),			/* tp_basicsize */
	0,					/* tp_itemsize */
	PyPgResult_dealloc,			/* tp_dealloc */
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
	PyPgResult_doc,				/* tp_doc */
	0,					/* tp_traverse */
	0,					/* tp_clear */
	0,					/* tp_richcompare */
	0,					/* tp_weaklistoffset */
	PyPgResult_iter,			/* tp_iter */
	(iternextfunc)PyPgResult_iternext,	/* tp_iternext */
	PyPgResult_methods,			/* tp_methods */
	PyPgResult_members,			/* tp_members */
	PyPgResult_getset,			/* tp_getset */
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
_getResultStatus(PGresult *result)
{
	return pgconst_from_enum(PyPg_PGRES, PQresultStatus(result));
}

PyObject *
PyPgResult_New(PyPgConnection *connection, PGresult *result)
{
	PyPgResult *self;

	if (!result) {
		PyErr_SetString(PqErr_OperationalError,
				PQerrorMessage(connection->connection));
		return NULL;
	}

	self = (PyPgResult *)PyObject_New(PyPgResult, &PyPgResult_Type);
	if (self == NULL) 
		return NULL;

	Py_INCREF(connection);
	self->connection = connection;
	self->result = result;
	self->row_number = 0;
	self->row_count = PQntuples(result);
	self->columns = NULL;
	self->status = _getResultStatus(self->result);
	return (PyObject *)self;
}

static PyPgConstEnumInit PGRES_init[] = {
	{ "PGRES_EMPTY_QUERY", PGRES_EMPTY_QUERY },
	{ "PGRES_COMMAND_OK", PGRES_COMMAND_OK },
	{ "PGRES_TUPLES_OK", PGRES_TUPLES_OK },
	{ "PGRES_COPY_OUT", PGRES_COPY_OUT },
	{ "PGRES_COPY_IN", PGRES_COPY_IN },
	{ "PGRES_BAD_RESPONSE", PGRES_BAD_RESPONSE },
	{ "PGRES_NONFATAL_ERROR", PGRES_NONFATAL_ERROR },
	{ "PGRES_FATAL_ERROR", PGRES_FATAL_ERROR },
	{ NULL }
};

static PyPgConstEnumInit DIAG_init[] = {
	{ "DIAG_SEVERITY", PG_DIAG_SEVERITY },
	{ "DIAG_SQLSTATE", PG_DIAG_SQLSTATE },
	{ "DIAG_MESSAGE_PRIMARY", PG_DIAG_MESSAGE_PRIMARY },
	{ "DIAG_MESSAGE_DETAIL", PG_DIAG_MESSAGE_DETAIL },
	{ "DIAG_MESSAGE_HINT", PG_DIAG_MESSAGE_HINT },
	{ "DIAG_STATEMENT_POSITION", PG_DIAG_STATEMENT_POSITION },
	{ "DIAG_INTERNAL_POSITION", PG_DIAG_INTERNAL_POSITION },
	{ "DIAG_INTERNAL_QUERY", PG_DIAG_INTERNAL_QUERY },
	{ "DIAG_CONTEXT", PG_DIAG_CONTEXT },
	{ "DIAG_SOURCE_FILE", PG_DIAG_SOURCE_FILE },
	{ "DIAG_SOURCE_LINE", PG_DIAG_SOURCE_LINE },
	{ "DIAG_SOURCE_FUNCTION", PG_DIAG_SOURCE_FUNCTION },
	{ NULL }
};

void
pg_result_init(PyObject *module)
{
	if (PyType_Ready(&PyPgResult_Type) < 0)
		return;

	// PQresultStatus
	PyPg_PGRES = pgconst_make_enum(module, "PGRES", PGRES_init);
	if (PyPg_PGRES == NULL)
		return;
	// PQresultErrorField
	PyPg_DIAG = pgconst_make_enum(module, "PG_DIAG", DIAG_init);
	if (PyPg_DIAG == NULL)
		return;
/*
	Py_INCREF(&PyPgResult_Type);
	PyModule_AddObject(module, "PgResult", 
			   (PyObject *)&PyPgResult_Type);
 */
}

