from redistrib.clusternode import ClusterNode


def list_nodes(rc):
    nodes = dict()
    for info in rc.send_cluster_command('cluster', 'nodes').split('\n'):
        if len(info) == 0:
            continue
        node = ClusterNode(*info.split(' '))
        if node.host == '':
            node.host = '127.0.0.1'
        nodes[(node.host, node.port)] = node
    return nodes
