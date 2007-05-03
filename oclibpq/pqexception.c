/* vi:set sw=8 ts=8 noet showmode ai: */
#include "oclibpq.h"

					/* StandardError		*/
PyObject *PqErr_Warning;		/* |--Warning			*/
PyObject *PqErr_Error;			/* +--Error			*/
PyObject *PqErr_InterfaceError;		/*    |--InterfaceError		*/
PyObject *PqErr_DatabaseError;		/*    +--DatabaseError		*/
PyObject *PqErr_DataError;		/*	 |--DataError		*/
PyObject *PqErr_OperationalError;	/*	 |--OperationaError	*/
PyObject *PqErr_IntegrityError;		/*	 |--IntegrityError	*/
PyObject *PqErr_InternalError;		/*	 |--InternalError	*/
PyObject *PqErr_ProgrammingError;	/*	 |--ProgrammingError	*/
PyObject *PqErr_NotSupportedError;	/*	 +--NotSupportedError	*/

#define NEW_EXC(e, n, b) \
{								\
	e = PyErr_NewException(MODULE_NAME "." n, b, NULL);	\
	if (e == NULL)						\
		return;						\
	Py_INCREF(e);						\
	if (PyModule_AddObject(module, n, e) < 0)		\
		return;						\
}

void pqexception_init(PyObject *module)
{
	NEW_EXC(PqErr_Warning, "Warning", PyExc_StandardError);
	NEW_EXC(PqErr_Error, "Error", PyExc_StandardError);
	NEW_EXC(PqErr_InterfaceError, "InterfaceError", PqErr_Error);
	NEW_EXC(PqErr_DatabaseError, "DatabaseError", PqErr_Error);
	NEW_EXC(PqErr_DataError, "DataError", PqErr_DatabaseError);
	NEW_EXC(PqErr_OperationalError, "OperationalError", PqErr_DatabaseError);
	NEW_EXC(PqErr_IntegrityError, "IntegrityError", PqErr_DatabaseError);
	NEW_EXC(PqErr_InternalError, "InternalError", PqErr_DatabaseError);
	NEW_EXC(PqErr_ProgrammingError, "ProgrammingError", PqErr_DatabaseError);
	NEW_EXC(PqErr_NotSupportedError, "NotSupportedError", PqErr_DatabaseError);
}
