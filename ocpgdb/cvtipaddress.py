# Standard libraries
import struct
# 3rd party
import ipaddress
# Module specific
from oclibpq import InterfaceError
from . import pgoid


def unpack_inet(buf):
    family, prefix, is_cidr, length = struct.unpack('!BBBB', buf[:4])
    if length == 4:
        return ipaddress.IPv4Address(buf[4:])
    elif length == 16:
        return ipaddress.IPv6Address(buf[4:])
    else:
        raise InterfaceError('Unable to unpack \'inet\' value %r' % buf)

def unpack_cidr(buf):
    family, prefix, is_cidr, length = struct.unpack('!BBBB', buf[:4])
    if length == 4:
        val = ipaddress.IPv4Network(buf[4:])
    elif length == 16:
        val = ipaddress.IPv6Network(buf[4:])
    else:
        raise InterfaceError('Unable to unpack \'inet\' value %r' % buf)
    # This is violates the documented interface, but the interface provides
    # no efficient way to create an IP*Network from packed data *with* a mask.
    val._prefixlen = prefix
    val.netmask = val._ip_int_from_prefix(val._prefixlen)
    return val

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
    return pgoid.cidr, hdr + addr.network_address.packed

def pack_ipv6net(addr):
    hdr = struct.pack('!BBBB', PGSQL_AF_INET6, addr.prefixlen, 0, 16)
    return pgoid.cidr, hdr + addr.network_address.packed
