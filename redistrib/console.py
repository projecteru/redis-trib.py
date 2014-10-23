import sys

import communicate


def _parse_host_port(addr):
    host, port = addr.split(':')
    return host, int(port)


def start(host_port):
    communicate.start_cluster(*_parse_host_port(host_port))


def join(cluster_host_port, newin_host_port):
    cluster_host, cluster_port = _parse_host_port(cluster_host_port)
    newin_host, newin_port = _parse_host_port(newin_host_port)
    communicate.join_cluster(cluster_host, cluster_port,
                             newin_host, newin_port)


def main():
    if len(sys.argv) < 2:
        print >> sys.stderr, 'Usage:'
        print >> sys.stderr, '    redis-trib.py ACTION_NAME [arg0 arg1 ...]'
        sys.exit(1)

    getattr(sys.modules[__name__], sys.argv[1])(*sys.argv[2:])
