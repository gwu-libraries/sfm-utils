import logging
import codecs
import os
import json
import shutil

log = logging.getLogger(__name__)

"""
A harvest state store keeps track of the state of harvesting of different types of resources.

For example, it might be used to keep track of the last tweet fetched from a user timeline.

A harvest state store should implement the signature of DictHarvestStateStore.
"""


class DictHarvestStateStore:
    """
    A harvest state store implementation backed by a dictionary and not persisted.
    """
    def __init__(self, verbose=True):
        self.state = {}
        self.verbose = verbose

    def get_state(self, resource_type, key):
        """
        Retrieves a state value from the harvest state store.

        :param resource_type: Key for the resource that has stored state.
        :param key: Key for the state that is being retrieved.
        :return: Value if the state or None.
        """
        if resource_type in self.state and key in self.state[resource_type]:
            return self.state[resource_type][key]
        else:
            return None

    def set_state(self, resource_type, key, value):
        """
        Adds a state value to the harvest state store.

        The resource type is used to separate namespaces for keys.

        :param resource_type: Key for the resource that is storing state.
        :param key: Key for the state that is being stored.
        :param value: Value for the state that is being stored.  None to delete an existing value.
        """
        if self.verbose:
            log.debug("Setting state for %s with key %s to %.200s", resource_type, key, value)
        if value is not None:
            if resource_type not in self.state:
                self.state[resource_type] = {}
            self.state[resource_type][key] = value
        else:
            # Clearing value
            if resource_type in self.state and key in self.state[resource_type]:
                # Delete key
                del self.state[resource_type][key]
                # If resource type is empty then delete
                if not self.state[resource_type]:
                    del self.state[resource_type]


class JsonHarvestStateStore(DictHarvestStateStore):
    """
    A harvest state store implementation backed by a dictionary and stored as JSON.

    The JSON is written to <path>/state.json. It is loaded and saved on
    every get and set.
    """
    def __init__(self, path):
        DictHarvestStateStore.__init__(self)

        self.path = path
        self.state_filepath = os.path.join(path, "state.json")
        self.state_tmp_filepath = os.path.join(path, "state.json.tmp")

    def _load_state(self):
        if os.path.exists(self.state_filepath):
            with codecs.open(self.state_filepath, "r") as state_file:
                self.state = json.load(state_file)

    def get_state(self, resource_type, key):
        self._load_state()
        return DictHarvestStateStore.get_state(self, resource_type, key)

    def set_state(self, resource_type, key, value):
        self._load_state()
        DictHarvestStateStore.set_state(self, resource_type, key, value)
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        # This way if the write fails, the original file will still be in place.
        with codecs.open(self.state_tmp_filepath, 'w', encoding="utf-8") as state_file:
            json.dump(self.state, state_file)
        shutil.move(self.state_tmp_filepath, self.state_filepath)


class NullHarvestStateStore:
    """
    A harvest state store that does nothing.
    """

    def __init__(self):
        pass

    def get_state(self, resource_type, key):
        return None

    def set_state(self, resource_type, key, value):
        pass


class DelayedSetStateStoreAdapter:
    """
    An adapter for a state store that keeps track of sets and delays
    passing them on the underlying state store.
    """
    def __init__(self, state_store):
        self.state_store = state_store
        self.delayed_state = DictHarvestStateStore(verbose=False)

    def get_state(self, resource_type, key):
        return self.delayed_state.get_state(resource_type, key) or self.state_store.get_state(resource_type, key)

    def set_state(self, resource_type, key, value):
        self.delayed_state.set_state(resource_type, key, value)

    def pass_state(self):
        """
        Set the state on the underlying state store.
        """
        for resource_type, key_values in self.delayed_state.state.items():
            for key, value in key_values.items():
                self.state_store.set_state(resource_type, key, value)
        self.delayed_state = DictHarvestStateStore(verbose=False)
