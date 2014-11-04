import unittest
from rediscluster import RedisCluster
from redis.exceptions import ResponseError

import redistrib.communicate as comm
from redistrib.exceptions import RedisStatusError
from redistrib.clusternode import ClusterNode


class ApiTest(unittest.TestCase):
    def test_api(self):
        comm.start_cluster('127.0.0.1', 7100)
        rc = RedisCluster([{'host': '127.0.0.1', 'port': 7100}])
        rc.set('key', 'value')
        self.assertEqual('value', rc.get('key'))

        comm.join_cluster('127.0.0.1', 7100, '127.0.0.1', 7101)
        for i in xrange(20):
            rc.set('key_%s' % i, 'value_%s' % i)

        for i in xrange(20):
            self.assertEqual('value_%s' % i, rc.get('key_%s' % i))

        nodes = dict()
        for info in rc.send_cluster_command('cluster', 'nodes').split('\n'):
            if len(info) == 0:
                continue
            node = ClusterNode(*info.split(' '))
            nodes[(node.host, node.port)] = node

        self.assertEqual(2, len(nodes))
        self.assertEqual(range(8192),
                         nodes[('127.0.0.1', 7101)].assigned_slots)
        self.assertEqual(range(8192, 16384),
                         nodes[('127.0.0.1', 7100)].assigned_slots)

        comm.quit_cluster('127.0.0.1', 7100)

        for i in xrange(20):
            self.assertEqual('value_%s' % i, rc.get('key_%s' % i))
        self.assertEqual('value', rc.get('key'))

        nodes = dict()
        for info in rc.send_cluster_command('cluster', 'nodes').split('\n'):
            if len(info) == 0:
                continue
            node = ClusterNode(*info.split(' '))
            if node.host == '':
                node.host = '127.0.0.1'
            nodes[(node.host, node.port)] = node
        self.assertEqual(1, len(nodes))
        self.assertEqual(range(16384),
                         nodes[('127.0.0.1', 7101)].assigned_slots)

        self.assertRaisesRegexp(
            RedisStatusError, r'Slot [0-9]+ not empty\.',
            comm.shutdown_cluster, '127.0.0.1', 7101)

        rc.delete('key', *['key_%s' % i for i in xrange(20)])
        comm.shutdown_cluster('127.0.0.1', 7101)

        self.assertRaisesRegexp(ResponseError, 'CLUSTERDOWN .*', rc.get, 'key')
