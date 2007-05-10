/* vi:set sw=8 ts=8 noet showmode ai: */

#include "oclibpq.h"

static void
PyPgResult_dealloc(PyObject *o)
{
	PyPgResult *self = (PyPgResult *)o;
	PGresult *res = self->result;

	if (res != NULL) {
		self->result = NULL;
		PQclear(res);
	}
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
PyPgResult_iternext(PyPgResult *self)
{
	PyObject *row, *cell_value, *cell;
	PyPgCell *master_cell;
	char *value;
	size_t len;
	int i;
	int format;
	int ncolumns = PyTuple_GET_SIZE(self->columns);
	PGresult *result = self->result;
	int row_number = self->row_number;

	if (row_number >= self->row_count)
		return NULL;

	row = PyTuple_New(ncolumns);
	if (row == NULL)
		return NULL;
	for (i = 0; i < ncolumns; ++i) {
		master_cell = (PyPgCell *)PyTuple_GET_ITEM(self->columns, i);
		format = PyInt_AS_LONG(master_cell->format);
		if (PQgetisnull(result, row_number, i)) {
			Py_INCREF(Py_None);
			cell_value = Py_None;
		} else if (format == 0) {
			value = PQgetvalue(result, row_number, i);
			value = (char *)PQunescapeBytea((unsigned char *)value, &len);
			cell_value = PyString_FromStringAndSize(value, len);
			PQfreemem(value);
			if (cell_value == NULL) {
				Py_DECREF(row);
				return NULL;
			}
		} else if (format == 1) {
			value = PQgetvalue(result, row_number, i);
			len = PQgetlength(result, row_number, i);
			cell_value = PyString_FromStringAndSize(value, len);
			if (cell_value == NULL) {
				Py_DECREF(row);
				return NULL;
			}
		}
		cell = PyPgCell_FromCell(master_cell, cell_value);
		if (cell == NULL) {
			Py_DECREF(cell_value);
			Py_DECREF(row);
			return NULL;
		}
		PyTuple_SET_ITEM(row, i, cell);
	}
	++self->row_number;
	return row;
}


static PyObject *
get_result_type(PyPgResult *self)
{
	char *type_str;

	switch (self->result_type) {
	case RESULT_DQL:	type_str = "DQL"; break;
	case RESULT_DDL:	type_str = "DDL"; break;
	case RESULT_DML:	type_str = "DML"; break;
	case RESULT_EMPTY:	type_str = "EMPTY"; break;
	default:
		PyErr_Format(PqErr_InternalError, 
		 	"Unknown query type: %d", self->result_type);
		return NULL;
	}
	return PyString_FromString(type_str);
}

static PyObject *
get_status(PyPgResult *self)
{
	return PyInt_FromLong(PQresultStatus(self->result));
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


static PyMethodDef PyPgResult_methods[] = {
	{NULL, NULL}
};

#define MO(m) offsetof(PyPgResult, m)
static PyMemberDef PyPgResult_members[] = {
	{"columns",	T_OBJECT,	MO(columns),	RO},
	{NULL}
};
#undef MO

static PyGetSetDef PyPgResult_getset[] = {
	{"result_type",		(getter)get_result_type},
	{"status",		(getter)get_status},
	{"ntuples",		(getter)get_ntuples},
	{"nfields",		(getter)get_nfields},
	{"binaryTuples",	(getter)get_binaryTuples},
	{"cmdStatus",		(getter)get_cmdStatus},
	{"cmdTuples",		(getter)get_cmdTuples},
	{"oid",			(getter)get_oid},
	{NULL}
};

static char PyPgResult_doc[] = "XXX PgResult objects";

static PyTypeObject PyPgResult_Type = {
	PyObject_HEAD_INIT(NULL)
	0,					/* ob_size */
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

static void 
PQErr_FromResult(PGresult *result, PGconn *connection)
{
	PyObject *exc;
	char *errmsg = PQerrorMessage(connection);

	switch (PQresultStatus(result))
	{
		case PGRES_NONFATAL_ERROR:
			exc = PqErr_ProgrammingError;
			break;

		case PGRES_FATAL_ERROR:
			if (strstr(errmsg, "referential integrity violation"))
				exc = PqErr_IntegrityError;
			else
				exc = PqErr_OperationalError;
			break;

		default:
			exc = PqErr_InternalError;
			break;
	}

	PyErr_SetString(exc, errmsg);
}

PyObject *
_PyPgResult_Columns(PGresult *result)
{
	PyObject *columns, *cell;
	int ncolumns = PQnfields(result);
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
	return columns;
}

PyObject *
PyPgResult_New(PyPgConnection *connection, PGresult *result)
{
	PyPgResult *self;
	enum result_type result_type;

	if (!result) {
		PyErr_SetString(PqErr_OperationalError,
				PQerrorMessage(connection->connection));
		return NULL;
	}

	switch (PQresultStatus(result)) {
	case PGRES_TUPLES_OK:
		result_type = RESULT_DQL;
		break;

	case PGRES_COMMAND_OK:
	case PGRES_COPY_OUT:
	case PGRES_COPY_IN:
		{
			char *ct;
			result_type = RESULT_DDL;
			ct = PQcmdTuples(result);
			if (ct[0])
				result_type = RESULT_DML;
		}
		break;

	case PGRES_EMPTY_QUERY:
		result_type = RESULT_EMPTY;
		break;

	default:
		PQErr_FromResult(result, connection->connection);
		PQclear(result);
		return NULL;
	}

	self = (PyPgResult *)PyObject_New(PyPgResult, &PyPgResult_Type);
	if (self == NULL) 
		return NULL;

	Py_INCREF(connection);
	self->connection = connection;
	self->result = result;
	self->result_type = result_type;
	self->row_number = 0;
	self->row_count = PQntuples(result);
	if ((self->columns = _PyPgResult_Columns(result)) == NULL)
		goto error;
	return (PyObject *)self;

error:
	Py_DECREF(self);
	return NULL;
}

void
pg_result_init(PyObject *module)
{
	if (PyType_Ready(&PyPgResult_Type) < 0)
		return;
/*
	Py_INCREF(&PyPgResult_Type);
	PyModule_AddObject(module, "PgResult", 
			   (PyObject *)&PyPgResult_Type);
 */
}

