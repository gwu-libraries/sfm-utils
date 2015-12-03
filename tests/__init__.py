import unittest
import logging


class TestCase(unittest.TestCase):
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger("sfmutils").setLevel(logging.DEBUG)
