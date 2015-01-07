DEMO VERSION WARNING:

THIS PROJECT IS A DEMO ON HOW TO MANIPULATE REDIS CLUSTER WITH PYTHON SOCKET, WITH UNSTABLE API, LACK OF TESTS. AND IT DOES NOT SUPPORT SIMULTANEOUS OPERATIONS ON ONE CLUSTER.

Installation
===

    pip install redis-trib
    easy_install redis-trib

Console Entry
===

Start a cluster in a single redis node (the node should have cluster enabled)

    redis-trib.py start NODE_HOST:PORT

Add another master node to a cluster

    redis-trib.py join CLUSTER_HOST:PORT NEW_NODE_HOST:PORT

Add a slave node to a master (slave should not in any cluster)

    redis-trib.py replicate MASTER_HOST:PORT SLAVE_HOST:PORT

Remove a node from its cluster

    redis-trib.py quit NODE_HOST:PORT

Shutdown an empty cluster (there is only one node left and no keys in the node)

    redis-trib.py shutdown NODE_HOST:PORT

Fix a migrating slot in a node

    redis-trib.py fix HOST_HOST:PORT

The above APIs balance slots automatically and not configurable.

Python API
===

Classes
---

`redistrib.cluster.ClusterNode`: cluster node, attributes:

* `node_id`: node id
* `host`: known host, this value could be empty string if the node is newly launched
* `port`: listening port
* `role_in_cluster`: `"master"` or `"slave"`
* `master_id`: master's `node_id` if it's a slave
* `assigned_slots`: a list of assigned slots if it's a master; it won't contain slots being migrated

Cluster Operation APIs
---

    import redistrib.command

    # start cluster at node 127.0.0.1:7000
    redistrib.command.start_cluster('127.0.0.1', 7000)

    # add node 127.0.0.1:7001 to the cluster as a master
    redistrib.command.join_cluster('127.0.0.1', 7000, '127.0.0.1', 7001)

    # add node 127.0.0.1:7002 to the cluster as a slave to 127.0.0.1:7000
    redistrib.command.replicate('127.0.0.1', 7000, '127.0.0.1', 7002)

    # remove node 127.0.0.7000 from the cluster
    redistrib.command.quit_cluster('127.0.0.1', 7000)

    # shut down the cluster
    redistrib.command.shutdown_cluster('127.0.0.1', 7001)

    # fix a migrating slot in a node
    redistrib.command.fix_migrating('127.0.0.1', 7001)

See also https://github.com/antirez/redis/blob/3.0/src/redis-trib.rb

The `join_cluster` function takes 2 optional arguments `balancer` and `balance_plan`. The former is an object for calculating the weights of cluster nodes, and the latter is a function that calculates how the slots migrate to balance the load between nodes.

As crude examples, you could refer to `redistrib.clusternode.BaseBalancer` and `redistrib.clusternode.base_balance_plan`. An instance of `BaseBalancer` should implement `weight` method that returns the weight of a specified node, and a function like `base_balance_plan` should return a list of migration tuples (source node, destination node, slots count).

Cluster Status APIs
---

    import redistrib.command

    # list all cluster nodes
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
