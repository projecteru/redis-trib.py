# Installation

    pip install redis-trib
    easy_install redis-trib

# Usage

WARNING: The following console commands or APIs not support simultaneous operations on one cluster.

## Console Commands

Start a cluster in a single redis node (the node should have cluster enabled, and not in a cluster)

    redis-trib.py start NODE_HOST:PORT

Start a cluster in some redis nodes (the nodes should have cluster enabled, and none of them in any cluster)

    redis-trib.py start_multi NODE_HOST_a:PORT_a NODE_HOST_b:PORT_b ...

Add another master node to a cluster

    redis-trib.py join CLUSTER_HOST:PORT NEW_NODE_HOST:PORT

The above APIs balance slots automatically and not configurable.

Add another node to a cluster, but neither set as slave nor migrating slots to it

    redis-trib.py join_no_load CLUSTER_HOST:PORT NEW_NODE_HOST:PORT

Add a slave node to a master (slave should not in any cluster)

    redis-trib.py replicate MASTER_HOST:PORT SLAVE_HOST:PORT

Remove a node from its cluster

    redis-trib.py quit NODE_HOST:PORT

Shutdown an empty cluster (there is only one node left and no keys in the node)

    redis-trib.py shutdown NODE_HOST:PORT

Fix a migrating slot in a node

    redis-trib.py fix HOST_HOST:PORT

Migrate slots (require source node holding all the migrating slots, and the two nodes are in the same cluster)

    redis-trib.py migrate_slots SRC_HOST:PORT DST_HOST:PORT SLOT SLOT_BEGIN-SLOT_END

each of "slot" argument tuple could be an integer (indicating a single slot number) or a range (begin and end, both inclusive). For example

    redis-trib.py migrate_slots 127.0.0.1:7000 127.0.0.1:7001 0 2 4-7

means migrate slot #0 #2 #4 #5 #6 #7 from `127.0.0.1:7000` to `127.0.0.1:7001`.

Rescue a failed cluster, specify host, port of one node in the cluster, and a free node

    redis-trib.py rescue 127.0.0.1:7000 127.0.0.1:8000

The program would check which slots are failed in the cluster which contains `127.0.0.1:7000`, and add them to `127.0.0.1:8000`.

## Python APIs

### Cluster Operation APIs

    import redistrib.command

    # start cluster at node 127.0.0.1:7000
    # this API will run the "cluster addslots" command on the Redis server;
    #   you can limit the number of slots added to the Redis in each command
    #   by default, all 16384 slots are added at once
    redistrib.command.start_cluster('127.0.0.1', 7000, max_slots=16384)

    # start cluster on multiple nodes, all the slots will be shared among them
    # the first argument is a list of (HOST, PORT) tuples
    # for example, the following call will start a cluster on 127.0.0.1:7000 and 127.0.0.1:7001
    # the second argument is the same as "max_slots" in "start_cluster" API
    redistrib.command.start_cluster_on_multi([('127.0.0.1', 7000), ('127.0.0.1', 7001)], max_slots=16384)

    # add node 127.0.0.1:7001 to the cluster as a master
    redistrib.command.join_cluster('127.0.0.1', 7000, '127.0.0.1', 7001)

    # add node 127.0.0.1:7002 to the cluster as a slave to 127.0.0.1:7000
    redistrib.command.replicate('127.0.0.1', 7000, '127.0.0.1', 7002)

    # just add node 127.0.0.1:7001 to the cluster, not specifying its role
    # could call migrate_slot(s) on it later, so that it becomes a master
    redistrib.command.join_no_load('127.0.0.1', 7000, '127.0.0.1', 7001)

    # remove node 127.0.0.7000 from the cluster
    redistrib.command.quit_cluster('127.0.0.1', 7000)

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

See also https://github.com/antirez/redis/blob/3.0/src/redis-trib.rb

The `join_cluster` function takes 2 optional arguments `balancer` and `balance_plan`. The former is an object for calculating the weights of cluster nodes, and the latter is a function that calculates how the slots migrate to balance the load between nodes.

As crude examples, you could refer to `redistrib.clusternode.BaseBalancer` and `redistrib.clusternode.base_balance_plan`. An instance of `BaseBalancer` should implement `weight` method that returns the weight of a specified node, and a function like `base_balance_plan` should return a list of migration tuples (source node, destination node, slots count).

### Cluster Status APIs

    import redistrib.command

    # list all cluster nodes (attributes of which shown in the next section)
    # args
    #   - host: host of specified node
    #   - port: port of specified node
    #   - default_host: default host string if the specified node doesn't know its host
    # returns
    #   - nodes: all cluster nodes
    #   - myself: the specified node itself, also contained by nodes
    nodes, myself = redistrib.command.list_nodes('127.0.0.1', 7000, default_host='127.0.0.1')

    # list all master nodes
    # args same as list_nodes
    # returns
    #   - nodes: all master nodes
    #   - myself: the specified node itself, contained by nodes if it's a master; won't be None even if it's a slave
    nodes, myself = redistrib.command.list_masters('127.0.0.1', 7000, default_host='127.0.0.1')

### Classes

`redistrib.cluster.ClusterNode`: cluster node, attributes:

* `node_id`: node id
* `host`: known host, this value could be empty string if the node is newly launched
* `port`: listening port
* `role_in_cluster`: `"master"` or `"slave"`
* `master_id`: master's `node_id` if it's a slave
* `assigned_slots`: a list of assigned slots if it's a master; it won't contain slots being migrated
* `slots_migrating`: boolean value for whether there are any slot(s) migrating or importing on this node
