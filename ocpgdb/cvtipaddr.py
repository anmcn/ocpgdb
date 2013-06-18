# Standard libraries
import struct
# 3rd party
try:
    import ipaddress
except ImportError:
    import ipaddr as ipaddress
# Module specific
from oclibpq import InterfaceError
from . import pgoid


def unpack_inet(buf):
    family, prefix, is_cidr, length = struct.unpack('!BBBB', buf[:4])
    if length == 4:
        octets = struct.unpack('!4B', buf[4:])
        return ipaddress.IPv4Address('%d.%d.%d.%d' % octets)
    elif length == 16:
        octets = struct.unpack('!8H', buf[4:])
        return ipaddress.IPv6Address('%x:%x:%x:%x:%x:%x:%x:%x' % octets)
    else:
        raise InterfaceError('Unable to unpack \'inet\' value %r' % buf)

def unpack_cidr(buf):
    family, prefix, is_cidr, length = struct.unpack('!BBBB', buf[:4])
    if length == 4:
        octets = struct.unpack('!4B', buf[4:])
        return ipaddress.IPv4Network('%d.%d.%d.%d/%d' % (octets + (prefix,)))
    elif length == 16:
        octets = struct.unpack('!8H', buf[4:])
        return ipaddress.IPv6Network('%x:%x:%x:%x:%x:%x:%x:%x/%d' % 
                (octets + (prefix,)))
    else:
        raise InterfaceError('Unable to unpack \'inet\' value %r' % buf)

PGSQL_AF_INET = 2
PGSQL_AF_INET6 = 3

def pack_ipv4addr(addr):
    hdr = struct.pack('!BBBB', PGSQL_AF_INET, 32, 0, 4)
    return pgoid.inet, hdr + addr.packed

def pack_ipv6addr(addr):
    hdr = struct.pack('!BBBB', PGSQL_AF_INET6, 128, 0, 16)
    return pgoid.inet, hdr + addr.packed

def pack_ipv4net(addr):
    hdr = struct.pack('!BBBB', PGSQL_AF_INET, addr.prefixlen, 0, 4)
    return pgoid.cidr, hdr + addr.packed

def pack_ipv6net(addr):
    hdr = struct.pack('!BBBB', PGSQL_AF_INET6, addr.prefixlen, 0, 16)
    return pgoid.cidr, hdr + addr.packed
