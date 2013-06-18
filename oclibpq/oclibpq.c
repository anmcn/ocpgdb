/* vi:set sw=8 ts=8 noet showmode ai: */

#include "oclibpq.h"

static PyObject *
unescape(PyPgConnection *self, PyObject *str)
{
	unsigned char *escaped, *unescaped;
	size_t len;
	PyObject *result;

	escaped = (unsigned char *)PyBytes_AsString(str);
	if (escaped == NULL)
		return NULL;
	unescaped = PQunescapeBytea(escaped, &len);
	result = PyBytes_FromStringAndSize((char *)unescaped, len);
	PQfreemem(unescaped);
	return result;
}

static PyMethodDef OCPQMethods[] = {
	{"unescape", (PyCFunction)unescape, METH_O,
		PyDoc_STR("Unescape a bytea")},
	{NULL, NULL, 0, NULL}	     /* Sentinel */
};

PyDoc_STRVAR(OCPQ_doco, "XXX TODO oclibpq docstring");

static void 
init_module_members(PyObject *module)
{
	pg_constants_init(module);
	pg_bytea_init(module);
	pg_exception_init(module);
	pg_cell_init(module);
	pg_result_init(module);
	pg_connection_init(module);
}

#if PY_MAJOR_VERSION < 3

PyMODINIT_FUNC
initoclibpq(void)
{
	PyObject *module;

	module = Py_InitModule4("oclibpq", OCPQMethods, OCPQ_doco,
				(PyObject*)NULL, PYTHON_API_VERSION);
	if (module != NULL)
		init_module_members(module);
}

#else /* PY_MAJOR_VERSION >= 3 */

static struct PyModuleDef moduledef = {
        PyModuleDef_HEAD_INIT,
        "oclibpq",
        OCPQ_doco,
        0,
        OCPQMethods,
        NULL,
        NULL,
        NULL,
        NULL
};

PyMODINIT_FUNC
PyInit_oclibpq(void)
{
	PyObject *module;

	module = PyModule_Create(&moduledef);
	if (module != NULL)
		init_module_members(module);
	return module;
}

#endif /* PY_MAJOR_VERSION >= 3 */
