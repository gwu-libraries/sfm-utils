import logging
import codecs
import os
import json

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
    def __init__(self):
        self._state = {}

    def get_state(self, resource_type, key):
        """
        Retrieves a state value from the harvest state store.

        :param resource_type: Key for the resource that has stored state.
        :param key: Key for the state that is being retrieved.
        :return: Value if the state or None.
        """
        if resource_type in self._state and key in self._state[resource_type]:
            return self._state[resource_type][key]
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
        log.debug("Setting state for %s with key %s to %s", resource_type, key, value)
        if value is not None:
            if resource_type not in self._state:
                self._state[resource_type] = {}
            self._state[resource_type][key] = value
        else:
            # Clearing value
            if resource_type in self._state and key in self._state[resource_type]:
                # Delete key
                del self._state[resource_type][key]
                # If resource type is empty then delete
                if not self._state[resource_type]:
                    del self._state[resource_type]


class JsonHarvestStateStore(DictHarvestStateStore):
    """
    A harvest state store implementation backed by a dictionary and stored as JSON.

    The JSON is written to <collection_path>/state.json. It is loaded and saved on
    every get and set.
    """
    def __init__(self, collection_path):
        DictHarvestStateStore.__init__(self)

        self.collection_path = collection_path
        self.state_filepath = os.path.join(collection_path, "state.json")

    def _load_state(self):
        if os.path.exists(self.state_filepath):
            with codecs.open(self.state_filepath, "r") as state_file:
                self._state = json.load(state_file)

    def get_state(self, resource_type, key):
        self._load_state()
        return DictHarvestStateStore.get_state(self, resource_type, key)

    def set_state(self, resource_type, key, value):
        self._load_state()
        DictHarvestStateStore.set_state(self, resource_type, key, value)
        with codecs.open(self.state_filepath, 'w') as state_file:
            json.dump(self._state, state_file)


class NullHarvestStateStore():
    """
    A harvest state store that does nothing.
    """

    def __init__(self):
        pass

    def get_state(self, resource_type, key):
        return None

    def set_state(self, resource_type, key, value):
        pass
