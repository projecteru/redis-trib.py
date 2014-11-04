import os
import logging
import tempfile
from unittest import TestCase

TestCase.maxDiff = None
logging.basicConfig(
    level=logging.DEBUG, format='%(levelname)s:%(asctime)s:%(message)s',
    filename=os.path.join(tempfile.gettempdir(), 'redistribpytest'))
