import os
import time
from subprocess import Popen

import redistrib.command as comm
import six
from redistrib.exceptions import RedisIOError

import base


class ReplicationTest(base.TestCase):
    def test_api(self):
        conf_file = '/tmp/redis_cluster_node_7103.conf'
        redis_server = Popen([
            os.environ['REDIS_SERVER'],
            '--cluster-enabled',
            'yes',
            '--cluster-config-file',
            conf_file,
            '--save',
            '',
            '--appendonly',
            'no',
            '--port',
            '7103',
        ])
        time.sleep(1)
        try:
            comm.create([
                ('127.0.0.1', 7101),
                ('127.0.0.1', 7102),
                ('127.0.0.1', 7103),
            ])

            nodes = base.list_nodes('127.0.0.1', 7101)
            slots = nodes[('127.0.0.1', 7103)].assigned_slots

            redis_server.terminate()
            redis_server = None

            # wait for process stop
            try:
                while True:
                    time.sleep(3)
                    base.list_nodes('127.0.0.1', 7103)
            except RedisIOError as e:
                # redis cluster-node-timeout + 1
                time.sleep(6)

            comm.rescue_cluster('127.0.0.1', 7102, '127.0.0.1', 7100)

            nodes = base.list_nodes('127.0.0.1', 7100)
            self.assertEqual(slots, nodes[('127.0.0.1', 7100)].assigned_slots)

            nodes = base.list_nodes('127.0.0.1', 7101)
            self.assertEqual(slots, nodes[('127.0.0.1', 7100)].assigned_slots)

            nodes = base.list_nodes('127.0.0.1', 7102)
            self.assertEqual(slots, nodes[('127.0.0.1', 7100)].assigned_slots)

            comm.quit_cluster('127.0.0.1', 7100)
            comm.quit_cluster('127.0.0.1', 7101)
            comm.shutdown_cluster('127.0.0.1', 7102, ignore_failed=True)
        finally:
            if redis_server != None:
                redis_server.kill()
            os.remove(conf_file)
