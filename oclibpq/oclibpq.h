/* vi:set sw=8 ts=8 noet showmode ai: */

/* Python */
#include <Python.h>
#include <structmember.h>

/* PostgreSQL */
#include <libpq-fe.h>
#include <libpq/libpq-fs.h>

/* Python 3 compatibility */
#if PY_MAJOR_VERSION >= 3
#define PyString_Type PyUnicode_Type
#define PyStringObject PyUnicodeObject
#define PyString_FromStringAndSize PyUnicode_FromStringAndSize
#define PyString_FromString PyUnicode_FromString
#define PyString_Check PyUnicode_Check
#define PyString_FromFormat PyUnicode_FromFormat
#define PyString_InternFromString PyUnicode_InternFromString
#define PyString_Join PyUnicode_Join
#define PyInt_Type PyLong_Type
#define PyInt_Check PyLong_Check
#define PyInt_AsLong PyLong_AsLong
#define PyInt_FromLong PyLong_FromLong
#define PyInt_FromString PyLong_FromString
#else /* PY_MAJOR_VERSION < 3 */
#define PyString_Join _PyString_Join
#define PyBytesObject PyStringObject
#define PyBytes_Type PyString_Type
#define PyBytes_Check PyString_Check
#define PyBytes_FromStringAndSize PyString_FromStringAndSize
#define PyBytes_FromString PyString_FromString
#define PyBytes_FromFormat PyString_FromFormat
#define PyBytes_Size PyString_Size
#define PyBytes_AsString PyString_AsString
#define PyBytes_Repr PyString_Repr
#define PyBytes_Format PyString_Format
#define PyBytes_AsStringAndSize PyString_AsStringAndSize
#endif /* PY_MAJOR_VERSION */

/* Python 2.5 compatibility - copied from Python 2.6 */
#ifndef PyVarObject_HEAD_INIT
#define PyVarObject_HEAD_INIT(type, size)	\
	PyObject_HEAD_INIT(type) size,
#endif
#ifndef Py_TYPE
#define Py_TYPE(ob)		(((PyObject*)(ob))->ob_type)
#endif

/* Python 2.3 compatibility - copied from Python 2.5 source */
#ifndef Py_VISIT
#define Py_VISIT(op)							\
        do { 								\
                if (op) {						\
                        int vret = visit((PyObject *)(op), arg);	\
                        if (vret)					\
                                return vret;				\
                }							\
        } while (0)
#endif
#ifndef Py_CLEAR
#define Py_CLEAR(op)				\
        do {                            	\
                if (op) {			\
                        PyObject *tmp = (PyObject *)(op);	\
                        (op) = NULL;		\
                        Py_DECREF(tmp);		\
                }				\
        } while (0)
#endif
#if (PY_VERSION_HEX < 0x02050000)
typedef int Py_ssize_t;
#endif

#define MODULE_NAME "oclibpq"

typedef struct {
	char		*name;
	int	 	value;
} PyPgConstEnumInit;

typedef struct {
	char		*name;
	int		 lower;
	int		 upper;
	PyObject       **values;
} PyPgConstEnum;

typedef struct {
	PyStringObject	str;
} PyPgBytea;

typedef struct {
	PyObject_HEAD
	PGconn		*connection;
	PyObject 	*conninfo;
	PyObject 	*notices;
} PyPgConnection;

typedef struct {
	PyObject_HEAD
	PyPgConnection	*connection;
	PGresult	*result;
	PyObject	*status;
	int		 row_number;
	int		 row_count;
	PyObject 	*columns;
} PyPgResult;

typedef struct {
	PyObject_HEAD
	PyObject *format;	
	PyObject *modifier;	
	PyObject *name;	
	PyObject *type;	
	PyObject *value;	
} PyPgCell;

typedef struct {
	PyObject_HEAD
	long ob_ival;
	PyObject *ob_name;
} PyPgConst;

extern PyObject *PqErr_Warning;
extern PyObject *PqErr_Error;
extern PyObject *PqErr_InterfaceError;
extern PyObject *PqErr_DatabaseError;
extern PyObject *PqErr_DataError;
extern PyObject *PqErr_OperationalError;
extern PyObject *PqErr_IntegrityError;
extern PyObject *PqErr_InternalError;
extern PyObject *PqErr_ProgrammingError;
extern PyObject *PqErr_NotSupportedError;

/* pqconstants.c */
extern void pg_constants_init(PyObject *module);
extern PyObject *set_module_const(PyObject *, const char *, int);
extern PyPgConstEnum *pgconst_make_enum(PyObject *, char *name, 
					PyPgConstEnumInit *);
extern PyObject *pgconst_from_enum(PyPgConstEnum *, int);

/* pqconnection.c */
extern void pg_connection_init(PyObject *module);
#define PyPgConnection_Check(op) ((op)->ob_type == &PyPgConnection_Type)

/* pqresult.c */
extern void pg_result_init(PyObject *module);
extern PyObject *PyPgResult_New(PyPgConnection *connection, PGresult *result);
#define PyPgResult_Check(op) ((op)->ob_type == &PyPgResult_Type)

/* pqexception.c */
extern void pg_exception_init(PyObject *module);

/* pqcell.c */
extern PyObject *PyPgCell_New(PGresult *, int);
extern PyObject *PyPgCell_FromCell(PyPgCell *cell, PyObject *value);
extern void pg_cell_init(PyObject *module);
extern int PyPgCell_Check(PyObject *);

/* bytea.c */
extern void pg_bytea_init(PyObject *module);
extern int PyPgBytea_Check(PyObject *);
