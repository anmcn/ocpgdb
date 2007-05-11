/* vi:set sw=8 ts=8 noet showmode ai: */

#include <stdint.h>

uint64_t
get_pg_uint64(unsigned char **buf)
{
	uint32_t      h, l;

	h = ntohl(*(*(uint64_t **)buf)++);
	l = ntohl(*(*(uint64_t **)buf)++);
	return ((uint64_t)h) << 32 | l;
}

void
put_pg_uint64(unsigned char **buf, uint64_t value)
{
	*(*(uint64_t **)buf)++ = htonl(value);
}


uint32_t
get_pg_uint32(unsigned char **buf)
{
	return ntohl(*(*(uint32_t **)buf)++);
}

void
put_pg_uint32(unsigned char **buf, uint32_t value)
{
	*(*(uint32_t **)buf)++ = htonl(value);
}


uint16_t
get_pg_uint16(unsigned char **buf)
{
	return ntohs(*(*(uint16_t **)buf)++);
}

void
put_pg_uint16(unsigned char *buf, uint16_t value)
{
	*(*(uint16_t **)buf)++ = htons(value);
}

main()
{
	char buf[8] = {0, 0, 0, 0, 0, 0, 0, 1}, *p;
	p = buf;
	printf("%lld\n", get_pg_uint64(&p));
}
