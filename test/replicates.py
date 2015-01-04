import time
import unittest
from rediscluster import RedisCluster
from redis.exceptions import ResponseError

import base
import redistrib.command as comm
from redistrib.clusternode import Talker, CMD_CLUSTER_NODES


class ReplicationTest(unittest.TestCase):
    def test_api(self):
        comm.start_cluster('127.0.0.1', 7100)
        comm.join_cluster('127.0.0.1', 7100, '127.0.0.1', 7101)
        comm.replicate('127.0.0.1', 7100, '127.0.0.1', 7102)
        time.sleep(1)

        rc = RedisCluster([{'host': '127.0.0.1', 'port': 7100}])

        for i in xrange(20):
            rc.set('key_%s' % i, 'value_%s' % i)
        for i in xrange(20):
            self.assertEqual('value_%s' % i, rc.get('key_%s' % i))

        nodes = base.list_nodes(rc)
        self.assertEqual(3, len(nodes))
        self.assertEqual(range(8192),
                         nodes[('127.0.0.1', 7101)].assigned_slots)
        self.assertEqual(range(8192, 16384),
                         nodes[('127.0.0.1', 7100)].assigned_slots)

        comm.quit_cluster('127.0.0.1', 7101)

        nodes = base.list_nodes(rc)
        self.assertEqual(range(16384),
                         nodes[('127.0.0.1', 7100)].assigned_slots)

        for i in xrange(20):
            self.assertEqual('value_%s' % i, rc.get('key_%s' % i))

        for i in xrange(20):
            rc.delete('key_%s' % i)

        comm.quit_cluster('127.0.0.1', 7102)
        comm.shutdown_cluster('127.0.0.1', 7100)

    def test_quit_problems(self):
        comm.start_cluster('127.0.0.1', 7100)
        comm.join_cluster('127.0.0.1', 7100, '127.0.0.1', 7101)
        comm.replicate('127.0.0.1', 7100, '127.0.0.1', 7102)
        time.sleep(1)

        rc = RedisCluster([{'host': '127.0.0.1', 'port': 7100}])

        for i in xrange(20):
            rc.set('key_%s' % i, 'value_%s' % i)
        for i in xrange(20):
            self.assertEqual('value_%s' % i, rc.get('key_%s' % i))

        nodes = base.list_nodes(rc)
        self.assertEqual(3, len(nodes))
        self.assertEqual(range(8192),
                         nodes[('127.0.0.1', 7101)].assigned_slots)
        self.assertEqual(range(8192, 16384),
                         nodes[('127.0.0.1', 7100)].assigned_slots)
        for i in xrange(20):
            rc.delete('key_%s' % i)

        self.assertRaisesRegexp(ValueError, '^The master still has slaves$',
                                comm.quit_cluster, '127.0.0.1', 7100)
        comm.quit_cluster('127.0.0.1', 7102)
        comm.quit_cluster('127.0.0.1', 7101)
        self.assertRaisesRegexp(ValueError, '^This is the last node',
                                comm.quit_cluster, '127.0.0.1', 7100)
        comm.shutdown_cluster('127.0.0.1', 7100)
