import os
import logging
import tempfile
import unittest

import redistrib.command as comm

unittest.TestCase.maxDiff = None
logging.basicConfig(
    level=logging.DEBUG, format='%(levelname)s:%(asctime)s:%(message)s',
    filename=os.path.join(tempfile.gettempdir(), 'redistribpytest'))


def list_nodes(host, port):
    return {(node.host, node.port): node for node in
            comm.list_nodes(host, port, '127.0.0.1')[0]}


class TestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)

    def run(self, result=None):
        if not (result and (result.failures or result.errors)):
            unittest.TestCase.run(self, result)
