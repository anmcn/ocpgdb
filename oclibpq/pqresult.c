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

	o->ob_type->tp_free(o);
}


static PyObject *
get_type(PyPgResult *self)
{
	char *type_str;

	switch (self->type) {
	case RESULT_DQL:	type_str = "DQL"; break;
	case RESULT_DDL:	type_str = "DDL"; break;
	case RESULT_DML:	type_str = "DML"; break;
	case RESULT_EMPTY:	type_str = "EMPTY"; break;
	default:
		PyErr_Format(PqErr_InternalError, 
		 	"Unknown query type: %d", self->type);
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

static PyMemberDef PyPgResult_members[] = {
	{NULL}
};

static PyGetSetDef PyPgResult_getset[] = {
	{"type",		(getter)get_type},
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
	0,					/* tp_iter */
	0,					/* tp_iternext */
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
	self->type = result_type;

	return (PyObject *)self;
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

