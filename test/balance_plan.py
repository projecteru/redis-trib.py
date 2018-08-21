import redistrib.clusternode

import base


class FakeNode(object):
    def __init__(self, node_id, slot_count):
        self.node_id = node_id
        self.assigned_slots = range(slot_count)
        self.role_in_cluster = 'master'


class BalancePlanTest(base.TestCase):
    def test_default_balance_plan(self):
        r = redistrib.clusternode.base_balance_plan([
            FakeNode('a', 16384),
            FakeNode('b', 0),
        ])
        self.assertEqual(1, len(r))
        source, target, count = r[0]
        self.assertEqual('a', source.node_id)
        self.assertEqual('b', target.node_id)
        self.assertEqual(8192, count)

        r = redistrib.clusternode.base_balance_plan([
            FakeNode('a', 8192),
            FakeNode('b', 8192),
            FakeNode('c', 0),
        ])
        self.assertEqual(2, len(r))
        r = sorted(r, key=lambda x: x[0].node_id)

        source, target, count = r[0]
        self.assertEqual('a', source.node_id)
        self.assertEqual('c', target.node_id)
        self.assertEqual(2730, count)

        source, target, count = r[1]
        self.assertEqual('b', source.node_id)
        self.assertEqual('c', target.node_id)
        self.assertEqual(2731, count)

        r = redistrib.clusternode.base_balance_plan([
            FakeNode('a', 1),
            FakeNode('b', 1),
            FakeNode('c', 0),
        ])
        self.assertEqual(0, len(r))

        r = redistrib.clusternode.base_balance_plan([
            FakeNode('a', 0),
            FakeNode('b', 1),
            FakeNode('c', 1),
        ])
        self.assertEqual(0, len(r))

        r = redistrib.clusternode.base_balance_plan([
            FakeNode('a', 1),
            FakeNode('b', 2),
            FakeNode('c', 1),
        ])
        self.assertEqual(0, len(r))
