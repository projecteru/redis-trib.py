from redistrib.clusternode import ClusterNode

import base


class ApiTest(base.TestCase):
    def test_parse_3(self):
        with open('test/data/3.0.txt', 'r') as nodes_file:
            nodes_txt = filter(None, nodes_file.readlines())
        nodes = [ClusterNode(*node_info.split(' ')) for node_info in nodes_txt]
        self.assertEqual(3, len(nodes))

        i = 0
        self.assertEqual('e7f4fcc0dd003fc107333a4132a471ad306d5513',
                         nodes[i].node_id)
        self.assertEqual('127.0.0.1', nodes[i].host)
        self.assertEqual(8001, nodes[i].port)
        self.assertTrue(nodes[i].master)
        self.assertFalse(nodes[i].slave)
        self.assertIsNone(None, nodes[i].master_id)
        self.assertFalse(nodes[i].fail)

        i = 1
        self.assertEqual('bd239f7dbeaba9541586a708484cdce0ca99aba5',
                         nodes[i].node_id)
        self.assertEqual('127.0.0.1', nodes[i].host)
        self.assertEqual(8000, nodes[i].port)
        self.assertTrue(nodes[i].master)
        self.assertFalse(nodes[i].slave)
        self.assertIsNone(None, nodes[i].master_id)
        self.assertFalse(nodes[i].fail)

        i = 2
        self.assertEqual('787e06e9d96e6a9a3d02c7f3ec14e243882293e9',
                         nodes[i].node_id)
        self.assertEqual('127.0.0.1', nodes[i].host)
        self.assertEqual(7999, nodes[i].port)
        self.assertTrue(nodes[i].master)
        self.assertIsNone(None, nodes[i].master_id)
        self.assertFalse(nodes[i].slave)
        self.assertFalse(nodes[i].fail)

    def test_parse_4(self):
        with open('test/data/4.0.txt', 'r') as nodes_file:
            nodes_txt = filter(None, nodes_file.readlines())
        nodes = [ClusterNode(*node_info.split(' ')) for node_info in nodes_txt]
        self.assertEqual(3, len(nodes))

        i = 0
        self.assertEqual('2d1866134ef5fabdfae0ca9ada4ea169f0e0c3fa',
                         nodes[i].node_id)
        self.assertEqual('127.0.0.1', nodes[i].host)
        self.assertEqual(7100, nodes[i].port)
        self.assertTrue(nodes[i].master)
        self.assertFalse(nodes[i].slave)
        self.assertIsNone(None, nodes[i].master_id)
        self.assertFalse(nodes[i].fail)

        i = 1
        self.assertEqual('2ec421bd92fec4823e64f963e29792803ce5c13c',
                         nodes[i].node_id)
        self.assertEqual('127.0.0.1', nodes[i].host)
        self.assertEqual(7102, nodes[i].port)
        self.assertFalse(nodes[i].master)
        self.assertTrue(nodes[i].slave)
        self.assertEqual('2d1866134ef5fabdfae0ca9ada4ea169f0e0c3fa',
                         nodes[i].master_id)
        self.assertFalse(nodes[i].fail)

        i = 2
        self.assertEqual('1739bb3232ef733500888051203b06b704f935a5',
                         nodes[i].node_id)
        self.assertEqual('127.0.0.1', nodes[i].host)
        self.assertEqual(7101, nodes[i].port)
        self.assertTrue(nodes[i].master)
        self.assertFalse(nodes[i].slave)
        self.assertIsNone(None, nodes[i].master_id)
        self.assertFalse(nodes[i].fail)
