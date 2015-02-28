import unittest
from rediscluster import RedisCluster
from redis.exceptions import ResponseError

import base
import redistrib.command as comm
from redistrib.exceptions import RedisStatusError
from redistrib.clusternode import Talker, CMD_CLUSTER_NODES


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

        nodes = base.list_nodes('127.0.0.1', 7100)

        self.assertEqual(2, len(nodes))
        self.assertEqual(range(8192),
                         nodes[('127.0.0.1', 7101)].assigned_slots)
        self.assertEqual(range(8192, 16384),
                         nodes[('127.0.0.1', 7100)].assigned_slots)

        comm.migrate_slot('127.0.0.1', 7100, '127.0.0.1', 7101, 8192)

        nodes = base.list_nodes('127.0.0.1', 7100)
        self.assertEqual(2, len(nodes))
        self.assertEqual(range(8193),
                         nodes[('127.0.0.1', 7101)].assigned_slots)
        self.assertEqual(range(8193, 16384),
                         nodes[('127.0.0.1', 7100)].assigned_slots)

        comm.migrate_slots('127.0.0.1', 7100, '127.0.0.1', 7101,
                           [8193, 8194, 8195])

        nodes = base.list_nodes('127.0.0.1', 7100)
        self.assertEqual(2, len(nodes))
        self.assertEqual(range(8196),
                         nodes[('127.0.0.1', 7101)].assigned_slots)
        self.assertEqual(range(8196, 16384),
                         nodes[('127.0.0.1', 7100)].assigned_slots)

        self.assertRaisesRegexp(
            ValueError, 'Slot not held by', comm.migrate_slot,
            '127.0.0.1', 7100, '127.0.0.1', 7101, 8192)

        self.assertRaisesRegexp(
            ValueError, 'Not all slot held by', comm.migrate_slots,
            '127.0.0.1', 7100, '127.0.0.1', 7101, [8195, 8196])

        self.assertRaisesRegexp(
            ValueError, 'Two nodes are not in the same cluster',
            comm.migrate_slot, '127.0.0.1', 7100, '127.0.0.1', 7102, 8196)

        comm.quit_cluster('127.0.0.1', 7100)

        for i in xrange(20):
            self.assertEqual('value_%s' % i, rc.get('key_%s' % i))
        self.assertEqual('value', rc.get('key'))

        nodes = base.list_nodes('127.0.0.1', 7101)
        self.assertEqual(1, len(nodes))
        self.assertEqual(range(16384),
                         nodes[('127.0.0.1', 7101)].assigned_slots)

        self.assertRaisesRegexp(
            RedisStatusError, 'Cluster containing keys',
            comm.shutdown_cluster, '127.0.0.1', 7101)

        rc.delete('key', *['key_%s' % i for i in xrange(20)])
        comm.shutdown_cluster('127.0.0.1', 7101)

        self.assertRaisesRegexp(ResponseError, 'CLUSTERDOWN .*', rc.get, 'key')

    def test_fix(self):
        def migrate_one_slot(nodes, _):
            if nodes[0].port == 7100:
                source, target = nodes
            else:
                target, source = nodes
            return [(source, target, 1)]

        comm.start_cluster('127.0.0.1', 7100)
        rc = RedisCluster([{'host': '127.0.0.1', 'port': 7100}])
        comm.join_cluster('127.0.0.1', 7100, '127.0.0.1', 7101,
                          balance_plan=migrate_one_slot)

        rc.set('h-893', 'I am in slot 0')
        comm.fix_migrating('127.0.0.1', 7100)
        self.assertEqual('I am in slot 0', rc.get('h-893'))

        t7100 = Talker('127.0.0.1', 7100)
        nodes = base.list_nodes('127.0.0.1', 7100)
        self.assertEqual(2, len(nodes))

        n7100 = nodes[('127.0.0.1', 7100)]
        n7101 = nodes[('127.0.0.1', 7101)]
        t7100.talk('cluster', 'setslot', 0, 'importing', n7101.node_id)

        comm.fix_migrating('127.0.0.1', 7100)
        self.assertEqual('I am in slot 0', rc.get('h-893'))

        nodes = base.list_nodes('127.0.0.1', 7100)
        self.assertEqual(2, len(nodes))
        n7100 = nodes[('127.0.0.1', 7100)]
        n7101 = nodes[('127.0.0.1', 7101)]
        self.assertEqual(16384, len(n7100.assigned_slots))
        self.assertEqual(0, len(n7101.assigned_slots))

        t7101 = Talker('127.0.0.1', 7101)
        nodes = base.list_nodes('127.0.0.1', 7100)
        self.assertEqual(2, len(nodes))
        n7100 = nodes[('127.0.0.1', 7100)]
        n7101 = nodes[('127.0.0.1', 7101)]
        self.assertEqual(16384, len(n7100.assigned_slots))
        self.assertEqual(0, len(n7101.assigned_slots))

        t7100.talk('cluster', 'setslot', 0, 'migrating', n7101.node_id)
        comm.fix_migrating('127.0.0.1', 7100)
        self.assertEqual('I am in slot 0', rc.get('h-893'))

        comm.quit_cluster('127.0.0.1', 7101)
        rc.delete('h-893')
        comm.shutdown_cluster('127.0.0.1', 7100)

        t7100.close()
        t7101.close()
