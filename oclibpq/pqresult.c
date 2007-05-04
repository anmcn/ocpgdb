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
PQResult_New(PQConnection *connection, PGresult *result)
{
	PQResult *self;
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

	self = (PQResult *)PyObject_New(PQResult, &PQResult_Type);
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

