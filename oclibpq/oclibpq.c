#include "oclibpq.h"

static PyMethodDef OCPQMethods[] = {
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

    pqconnection_init(module);
    pqexception_init(module);
}
