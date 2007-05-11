#include "oclibpq.h"

static PyObject *
unescape(PyPgConnection *self, PyObject *str)
{
	unsigned char *escaped, *unescaped;
	size_t len;
        PyObject *result;

	escaped = (unsigned char *)PyString_AsString(str);
	if (escaped == NULL)
		return NULL;
	unescaped = PQunescapeBytea(escaped, &len);
        result = PyString_FromStringAndSize((char *)unescaped, len);
        PQfreemem(unescaped);
        return result;
}

static PyMethodDef OCPQMethods[] = {
	{"unescape", (PyCFunction)unescape, METH_O,
		PyDoc_STR("Unescape a bytea")},
        {NULL, NULL, 0, NULL}        /* Sentinel */
};

static char *OCPQ_doco = "XXX module docstring";


PyMODINIT_FUNC
initoclibpq(void)
{
    PyObject *module;

    module = Py_InitModule4("oclibpq", OCPQMethods, OCPQ_doco,
                            (PyObject*)NULL, PYTHON_API_VERSION);
    if (module == NULL)
	return;

    pg_bytea_init(module);
    pg_exception_init(module);
    pg_cell_init(module);
    pg_result_init(module);
    pg_connection_init(module);
}
