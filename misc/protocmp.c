/*
 * Compare speed of PQexec, PQexecParams (text) and PQexecParams (binary)
 */
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/time.h>
#include <time.h>
#include <libpq-fe.h>

void
exit_nicely(PGconn *conn)
{
	PQfinish(conn);
	exit(1);
}

double
ftime(void)
{
    struct timeval tv;
    double t;

    if (gettimeofday(&tv, NULL) < 0) {
        perror("gettimeofday");
        exit(1);
    }
    t = (double)tv.tv_sec;
    t += (double)tv.tv_usec / 1000000.0;
    return t;
}

void
exec(PGconn *conn, char *cmd)
{
	PGresult		*res;

	res = PQexec(conn, cmd);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "%s failed: %s", cmd, PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}
	PQclear(res);
}

void
insert(PGconn *conn)
{
	PGresult		*res;

	res = PQexec(conn, "INSERT INTO iorate VALUES (10000,10000,10000,10000)");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "INSERT failed: %s", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}
	PQclear(res);
}

void
insert_params(PGconn *conn)
{
	PGresult		*res;
        int                     i;
	const int		 l = 4;
	const char		*param_vals[l];

        for (i = 0; i < l; ++i)
            param_vals[i] = "10000";
	res = PQexecParams(conn, "INSERT INTO iorate VALUES ($1,$2,$3,$4)",
			   l, NULL, param_vals, NULL, NULL, 0);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "INSERT failed: %s", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}
	PQclear(res);
}

void
insert_params_bin(PGconn *conn)
{
	PGresult		*res;
        int                     i;
	const int		 l = 4;
	const char		*param_vals[l];
	int			 param_lens[l];
	int			 param_fmts[l];
	Oid			 param_oid[l];
        long                     v;


        v = htonl(10000);
        for (i = 0; i < l; ++i) {
            param_vals[i] = (void *)&v;
            param_lens[i] = 4;
            param_fmts[i] = 1;
            param_oid[i] = 23;
        }
	res = PQexecParams(conn, "INSERT INTO iorate VALUES ($1,$2,$3,$4)",
			   l, param_oid, param_vals, (const int *) param_lens, 
                           (const int *) param_fmts, 1);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "INSERT failed: %s", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}
	PQclear(res);
}

int
main(void)
{
	PGconn			*conn;
        int     i, repeat = 20000;
        double st, el;

	conn = PQconnectdb("dbname=andrewm port=5433");
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Error: PQconnectdb: %s", PQerrorMessage(conn));
		exit_nicely(conn);
	}

        exec(conn, "BEGIN WORK");
        st = ftime();
        for (i = 0; i < repeat; ++i)
            insert(conn);
        el = (ftime() - st) / repeat;
        printf("PQexec took %.3fms\n", el * 1000);
        exec(conn, "ROLLBACK");

        exec(conn, "BEGIN WORK");
        st = ftime();
        for (i = 0; i < repeat; ++i)
            insert_params(conn);
        el = (ftime() - st) / repeat;
        printf("PQexecParams took %.3fms\n", el * 1000);
        exec(conn, "ROLLBACK");

        exec(conn, "BEGIN WORK");
        st = ftime();
        for (i = 0; i < repeat; ++i)
            insert_params_bin(conn);
        el = (ftime() - st) / repeat;
        printf("PQexecParams (bin) took %.3fms\n", el * 1000);
        exec(conn, "ROLLBACK");

	return 0;
}
