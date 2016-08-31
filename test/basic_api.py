from rediscluster import RedisCluster
from redis.exceptions import ResponseError

import base
import redistrib.command as comm
from redistrib.exceptions import RedisStatusError
from redistrib.connection import Connection


class ApiTest(base.TestCase):
    def test_api(self):
        comm.create([('127.0.0.1', 7100)])
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

        comm.migrate_slots('127.0.0.1', 7100, '127.0.0.1', 7101, [8192])

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
            ValueError, 'Not all slot held by', comm.migrate_slots,
            '127.0.0.1', 7100, '127.0.0.1', 7101, [8192])

        self.assertRaisesRegexp(
            ValueError, 'Not all slot held by', comm.migrate_slots,
            '127.0.0.1', 7100, '127.0.0.1', 7101, [8195, 8196])

        self.assertRaisesRegexp(
            ValueError, 'Two nodes are not in the same cluster',
            comm.migrate_slots, '127.0.0.1', 7100, '127.0.0.1', 7102, [8196])

        comm.quit_cluster('127.0.0.1', 7100)

        for i in xrange(20):
            self.assertEqual('value_%s' % i, rc.get('key_%s' % i))
        self.assertEqual('value', rc.get('key'))

        nodes = base.list_nodes('127.0.0.1', 7101)
        self.assertEqual(1, len(nodes))
        self.assertEqual(range(16384),
                         nodes[('127.0.0.1', 7101)].assigned_slots)

        self.assertRaisesRegexp(
            RedisStatusError, 'still contains keys',
            comm.shutdown_cluster, '127.0.0.1', 7101)

        rc.delete('key', *['key_%s' % i for i in xrange(20)])
        comm.shutdown_cluster('127.0.0.1', 7101)

        self.assertRaisesRegexp(ResponseError, 'CLUSTERDOWN .*', rc.get, 'key')

    def test_start_with_max_slots_set(self):
        comm.create([('127.0.0.1', 7100)], max_slots=7000)
        rc = RedisCluster([{'host': '127.0.0.1', 'port': 7100}])
        rc.set('key', 'value')
        self.assertEqual('value', rc.get('key'))
        rc.delete('key')
        comm.shutdown_cluster('127.0.0.1', 7100)

        comm.start_cluster_on_multi([('127.0.0.1', 7100), ('127.0.0.1', 7101)],
                                    max_slots=7000)
        rc = RedisCluster([{'host': '127.0.0.1', 'port': 7100}])
        rc.set('key', 'value')
        self.assertEqual('value', rc.get('key'))
        rc.delete('key')
        comm.quit_cluster('127.0.0.1', 7101)
        comm.shutdown_cluster('127.0.0.1', 7100)

    def test_start_multi(self):
        comm.start_cluster_on_multi([('127.0.0.1', 7100), ('127.0.0.1', 7101)])
        nodes = base.list_nodes('127.0.0.1', 7100)
        self.assertEqual(2, len(nodes))
        self.assertEqual(8192, len(nodes[('127.0.0.1', 7100)].assigned_slots))
        self.assertEqual(8192, len(nodes[('127.0.0.1', 7101)].assigned_slots))
        comm.quit_cluster('127.0.0.1', 7100)
        comm.shutdown_cluster('127.0.0.1', 7101)

        comm.start_cluster_on_multi([('127.0.0.1', 7100), ('127.0.0.1', 7101),
                                     ('127.0.0.1', 7102)])
        nodes = base.list_nodes('127.0.0.1', 7100)
        self.assertEqual(3, len(nodes))
        self.assertEqual(5462, len(nodes[('127.0.0.1', 7100)].assigned_slots))
        self.assertEqual(5461, len(nodes[('127.0.0.1', 7101)].assigned_slots))
        self.assertEqual(5461, len(nodes[('127.0.0.1', 7102)].assigned_slots))
        comm.quit_cluster('127.0.0.1', 7100)
        comm.quit_cluster('127.0.0.1', 7101)
        comm.shutdown_cluster('127.0.0.1', 7102)

        comm.start_cluster_on_multi([('127.0.0.1', 7100), ('127.0.0.1', 7101),
                                     ('127.0.0.1', 7100), ('127.0.0.1', 7102)])
        nodes = base.list_nodes('127.0.0.1', 7100)
        self.assertEqual(3, len(nodes))
        self.assertEqual(5462, len(nodes[('127.0.0.1', 7100)].assigned_slots))
        self.assertEqual(5461, len(nodes[('127.0.0.1', 7101)].assigned_slots))
        self.assertEqual(5461, len(nodes[('127.0.0.1', 7102)].assigned_slots))
        comm.quit_cluster('127.0.0.1', 7100)
        comm.quit_cluster('127.0.0.1', 7101)
        comm.shutdown_cluster('127.0.0.1', 7102)

    def test_fix(self):
        def migrate_one_slot(nodes, _):
            if nodes[0].port == 7100:
                source, target = nodes
            else:
                target, source = nodes
            return [(source, target, 1)]

        comm.create([('127.0.0.1', 7100)])
        rc = RedisCluster([{'host': '127.0.0.1', 'port': 7100}])
        comm.join_cluster('127.0.0.1', 7100, '127.0.0.1', 7101,
                          balance_plan=migrate_one_slot)

        rc.set('h-893', 'I am in slot 0')
        comm.fix_migrating('127.0.0.1', 7100)
        self.assertEqual('I am in slot 0', rc.get('h-893'))

        t7100 = Connection('127.0.0.1', 7100)
        nodes = base.list_nodes('127.0.0.1', 7100)
        self.assertEqual(2, len(nodes))

        n7100 = nodes[('127.0.0.1', 7100)]
        n7101 = nodes[('127.0.0.1', 7101)]
        t7100.execute('cluster', 'setslot', 0, 'importing', n7101.node_id)

        comm.fix_migrating('127.0.0.1', 7100)
        self.assertEqual('I am in slot 0', rc.get('h-893'))

        nodes = base.list_nodes('127.0.0.1', 7100)
        self.assertEqual(2, len(nodes))
        n7100 = nodes[('127.0.0.1', 7100)]
        n7101 = nodes[('127.0.0.1', 7101)]
        self.assertEqual(16384, len(n7100.assigned_slots))
        self.assertEqual(0, len(n7101.assigned_slots))

        t7101 = Connection('127.0.0.1', 7101)
        nodes = base.list_nodes('127.0.0.1', 7100)
        self.assertEqual(2, len(nodes))
        n7100 = nodes[('127.0.0.1', 7100)]
        n7101 = nodes[('127.0.0.1', 7101)]
        self.assertEqual(16384, len(n7100.assigned_slots))
        self.assertEqual(0, len(n7101.assigned_slots))

        t7100.execute('cluster', 'setslot', 0, 'migrating', n7101.node_id)
        comm.fix_migrating('127.0.0.1', 7100)
        self.assertEqual('I am in slot 0', rc.get('h-893'))

        comm.quit_cluster('127.0.0.1', 7101)
        rc.delete('h-893')
        comm.shutdown_cluster('127.0.0.1', 7100)

        t7100.close()
        t7101.close()

    def test_join_no_load(self):
        comm.create([('127.0.0.1', 7100)])

        rc = RedisCluster([{'host': '127.0.0.1', 'port': 7100}])
        rc.set('x-{h-893}', 'y')
        rc.set('y-{h-893}', 'zzZ')
        rc.set('z-{h-893}', 'w')
        rc.incr('h-893')

        comm.join_no_load('127.0.0.1', 7100, '127.0.0.1', 7101)
        nodes = base.list_nodes('127.0.0.1', 7100)
        self.assertEqual(2, len(nodes))
        n7100 = nodes[('127.0.0.1', 7100)]
        n7101 = nodes[('127.0.0.1', 7101)]

        self.assertEqual(16384, len(n7100.assigned_slots))
        self.assertEqual(0, len(n7101.assigned_slots))

        comm.join_no_load('127.0.0.1', 7100, '127.0.0.1', 7102)
        comm.migrate_slots('127.0.0.1', 7100, '127.0.0.1', 7101, [0])

        nodes = base.list_nodes('127.0.0.1', 7102)
        self.assertEqual(3, len(nodes))
        n7100 = nodes[('127.0.0.1', 7100)]
        n7101 = nodes[('127.0.0.1', 7101)]
        n7102 = nodes[('127.0.0.1', 7102)]

        self.assertEqual(16383, len(n7100.assigned_slots))
        self.assertEqual(1, len(n7101.assigned_slots))
        self.assertEqual(0, len(n7102.assigned_slots))

        try:
            t = n7101.get_conn()
            m = t.execute('get', 'h-893')
            self.assertEqual('1', m)

            m = t.execute('get', 'y-{h-893}')
            self.assertEqual('zzZ', m)

            comm.quit_cluster('127.0.0.1', 7102)
            comm.quit_cluster('127.0.0.1', 7101)
            t = n7100.get_conn()

            rc.delete('x-{h-893}')
            rc.delete('y-{h-893}')
            rc.delete('z-{h-893}')
            rc.delete('h-893')
            comm.shutdown_cluster('127.0.0.1', 7100)
        finally:
            n7100.close()
            n7101.close()
