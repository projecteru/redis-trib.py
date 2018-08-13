Redis cluster CLI tools and libraries in Python. It supports Python 2.7 and 3.5 or higher. It supports Redis 3.x and 4.x cluster mode.

# Installation

    pip install redis-trib
    easy_install redis-trib

# Usage

NOTE: The following console commands or APIs do not support simultaneous operations on one cluster.

## Console Commands

### Cluster Manipulation

Create a cluster in some redis nodes (the nodes are cluster enabled, and none of them in any cluster)

    redis-trib.py create NODE_HOST_a:PORT_a NODE_HOST_b:PORT_b ...

Add another node to a cluster, but neither set as slave nor migrating slots to it

    redis-trib.py add_node --existing-addr CLUSTER_HOST:PORT --new-addr NEW_NODE_HOST:PORT

Add a slave node to a master (slave should not in any cluster)

    redis-trib.py replicate --master-addr MASTER_HOST:PORT --slave-addr SLAVE_HOST:PORT

Remove a node from its cluster

    redis-trib.py del_node --addr NODE_HOST:PORT

Shutdown an empty cluster (there is only one node left and no keys in the node)

    redis-trib.py shutdown --addr NODE_HOST:PORT

Fix migrating slots in a node

    redis-trib.py fix --addr HOST_HOST:PORT

Migrate slots (require source node holding all the migrating slots, and the two nodes are in the same cluster)

    redis-trib.py migrate --src-addr SRC_HOST:PORT --dst-addr DST_HOST:PORT SLOT SLOT_BEGIN-SLOT_END

Rescue a failed cluster, specify host, port of one node in the cluster, and a free node

    redis-trib.py rescue --existing-addr CLUSTER_NODE_HOST:PORT --new-addr NEW_NODE_HOST:PORT

### List Cluster Nodes

List nodes in a cluster and output simple master slave relationship between them

    redis-trib.py list --addr HOST:PORT

Output:

    Total 4 nodes, 2 masters, 0 fail
    M  127.0.0.1:7001 master 0
    M  127.0.0.1:7003 master 8000
     S 127.0.0.1:7000 myself,slave 127.0.0.1:7003
     S 127.0.0.1:7002 slave 127.0.0.1:7003
    M  127.0.0.1:7005 master 8384
     S 127.0.0.1:7004 slave 127.0.0.1:7005

Each line represent one Redis node and a leading `M` means it is a master, while a leading `S` means slave. The second column contains address, the third contains its flags, the last column contains the number of slots assigned to it if it is a master, or its master's address if it is a slave.

Lines are sorted by the master addresses, and each slave will be displayed after its master.

The format may be changed in the future. If you need a stable API, use Redis `CLUSTER NODES` command instead. Or use the `redistrib.command.list_nodes` Python API, which is mentioned below.

### Execute Command

Execute a command on each cluster node

    redis-trib.py execute --addr HOST:PORT COMMAND ARGS...
    redis-trib.py execute --addr HOST:PORT --master-only COMMAND ARGS...
    redis-trib.py execute --addr HOST:PORT --slave-only COMMAND ARGS...

For example:

    redis-trib.py execute --addr 127.0.0.1 PING

Output:

    127.0.0.1:7001 +PONG
    127.0.0.1:7002 +PONG
    127.0.0.1:7003 +PONG
    127.0.0.1:7000 +PONG

### More Examples

Please read the [wiki](https://github.com/projecteru/redis-trib.py/wiki/How-to-Cluster).

## Python APIs

### Cluster Operation APIs

    import redistrib.command

    # start cluster on multiple nodes, all the slots will be shared among them
    # the first argument is a list of (HOST, PORT) tuples
    # for example, the following call will start a cluster on 127.0.0.1:7000 and 127.0.0.1:7001
    # this API will run the "cluster addslots" command on the Redis server;
    #   you can limit the number of slots added to the Redis in each command by the `max_slots` argument
    redistrib.command.create([('127.0.0.1', 7000), ('127.0.0.1', 7001)], max_slots=16384)

    # add node 127.0.0.1:7001 to the cluster as a master
    redistrib.command.add_node('127.0.0.1', 7000, '127.0.0.1', 7001)

    # add node 127.0.0.1:7002 to the cluster as a slave to 127.0.0.1:7000
    redistrib.command.replicate('127.0.0.1', 7000, '127.0.0.1', 7002)

    # remove node 127.0.0.7000 from the cluster
    redistrib.command.del_node('127.0.0.1', 7000)

    # shut down the cluster
    redistrib.command.shutdown_cluster('127.0.0.1', 7001)

    # fix a migrating slot in a node
    redistrib.command.fix_migrating('127.0.0.1', 7001)

    # migrate slots; require source node holding the slots

    # migrate slots #1, #2, #3 from 127.0.0.1:7001 to 127.0.0.1:7002
    redistrib.command.migrate_slots('127.0.0.1', 7001, '127.0.0.1', 7002, [1, 2, 3])

    # rescue a failed cluster
    # 127.0.0.1:7000 is one of the nodes that is still alive in the cluster
    # and 127.0.0.1:8000 is the node that would take care of all failed slots
    redistrib.command.rescue_cluster('127.0.0.1', 7000, '127.0.0.1', 8000)

### Cluster Status APIs

    import redistrib.command

    # list all cluster nodes (attributes of which shown in the next section)
    # args
    #   - host: host of specified node
    #   - port: port of specified node
    #   - default_host: default host string if the specified node doesn't know its host
    #   - filter_func: filter function that takes one ClusterNode parameter
    #                  and returns False if the node should be excluded from the result
    # returns
    #   - nodes: all cluster nodes
    #   - myself: the specified node itself, also contained by nodes
    nodes, myself = redistrib.command.list_nodes('127.0.0.1', 7000, default_host='127.0.0.1',
                                                 filter_func=lambda node: True)

    # list all master nodes
    # args same as list_nodes
    # returns
    #   - nodes: all master nodes
    #   - myself: the specified node itself, contained by nodes if it's a master; won't be None even if it's a slave
    nodes, myself = redistrib.command.list_masters('127.0.0.1', 7000, default_host='127.0.0.1')

### Classes

`redistrib.clusternode.ClusterNode`: cluster node, attributes:

* `node_id`: node id
* `host`: known host, this value could be empty string if the node is newly launched
* `port`: listening port
* `master`: if the node is a master
* `slave`: if the node is a slave
* `role_in_cluster`: `"master"` or `"slave"`
* `fail`: if the node is marked as "fail" or "fail?"
* `master_id`: master's `node_id` if it's a slave, or `None` otherwise
* `assigned_slots`: a list of assigned slots if it's a master; it won't contain slots being migrated
* `slots_migrating`: boolean value for whether there are any slot(s) migrating or importing on this node
