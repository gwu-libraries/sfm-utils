from __future__ import absolute_import
from sfmutils.state_store import JsonHarvestStateStore
import tempfile
import os
import shutil
from unittest import TestCase


class TestJsonHarvestStateStore(TestCase):

    def setUp(self):
        self.path = os.path.join(tempfile.mkdtemp(), "test")
        self.store = JsonHarvestStateStore(self.path)

    def tearDown(self):
        if os.path.exists(self.path):
            shutil.rmtree(self.path)

    def test_set_state(self):
        self.assertIsNone(self.store.get_state("resource_type1", "key1"), "Has state before state is set")
        # Set the state
        self.store.set_state("resource_type1", "key1", "value1")
        self.assertEqual("value1", self.store.get_state("resource_type1", "key1"), "Retrieved state not value1")

        # Change the value
        self.store.set_state("resource_type1", "key1", "value2")
        self.assertEqual("value2", self.store.get_state("resource_type1", "key1"), "Retrieved state not value2")

        # Clear the value
        self.store.set_state("resource_type1", "key1", None)
        self.assertIsNone(self.store.get_state("resource_type1", "key1"), "Has state after state is cleared")

    def test_persist(self):
        # Set the state
        self.store.set_state("resource_type1", "key1", "value1")
        self.assertEqual("value1", self.store.get_state("resource_type1", "key1"), "Retrieved state not value1")

        # Create a new store and test for value
        self.store = JsonHarvestStateStore(self.path)
        self.assertEqual("value1", self.store.get_state("resource_type1", "key1"), "Retrieved state not value1")
