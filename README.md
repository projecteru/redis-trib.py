Start a cluster in a single redis node (the node should have cluster enabled)

    redis-trib.py start HOST:PORT

Add another node to a cluster

    redis-trib.py join CLUSTER_HOST:PORT NEW_NODE_HOST:PORT

Support master nodes only. Auto slots balancing.

The Python API

    import redistrib.communicate

    # start cluster at node 127.0.0.1:7000
    redistrib.communicate.start_cluster('127.0.0.1', 7000)

    # add node 127.0.0.1:7001 to the cluster
    redistrib.communicate.join_cluster('127.0.0.1', 7000, '127.0.0.1', 7001)

See also https://github.com/antirez/redis/blob/3.0/src/redis-trib.rb
