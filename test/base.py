import redistrib.command as comm


def list_nodes(host, port):
    return {(node.host, node.port): node for node in
            comm.list_nodes(host, port, '127.0.0.1')[0]}
