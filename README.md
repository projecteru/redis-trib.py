DEMO VERSION WARNING:

THIS PROJECT IS A DEMO ON HOW TO MANIPULATE REDIS CLUSTER WITH PYTHON SOCKET, WITH UNSTABLE API, LACK OF TESTS. AND IT DOES NOT SUPPORT SIMULTANEOUS OPERATIONS ON ONE CLUSTER.

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

Support master nodes only. Auto slots balancing.

The Python API

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
