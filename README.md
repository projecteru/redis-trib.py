DEMO VERSION WARNING:

THIS PROJECT IS A DEMO ON HOW TO MANIPULATE REDIS CLUSTER WITH PYTHON SOCKET, WITH UNSTABLE API, LACK OF TESTS. AND IT DOES NOT SUPPORT SIMULTANEOUS OPERATIONS ON ONE CLUSTER.

Start a cluster in a single redis node (the node should have cluster enabled)

    redis-trib.py start NODE_HOST:PORT

Add another node to a cluster

    redis-trib.py join CLUSTER_HOST:PORT NEW_NODE_HOST:PORT

Remove a node from its cluster

    redis-trib.py quit NODE_HOST:PORT

Shutdown an empty cluster (there is only one node left and no keys in the node)

    redis-trib.py shutdown NODE_HOST:PORT

Support master nodes only. Auto slots balancing.

The Python API

    import redistrib.communicate

    # start cluster at node 127.0.0.1:7000
    redistrib.communicate.start_cluster('127.0.0.1', 7000)

    # add node 127.0.0.1:7001 to the cluster
    redistrib.communicate.join_cluster('127.0.0.1', 7000, '127.0.0.1', 7001)

    # remove node 127.0.0.7000 from the cluster
    redistrib.communicate.quit_cluster('127.0.0.1', 7000)

    # shut down the cluster
    redistrib.communicate.shutdown_cluster('127.0.0.1', 7001)

See also https://github.com/antirez/redis/blob/3.0/src/redis-trib.rb
