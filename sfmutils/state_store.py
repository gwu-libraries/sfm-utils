import logging
import codecs
import os
import json

log = logging.getLogger(__name__)

"""
A harvest state store keeps track of the state of harvesting of different types of resources.

For example, it might be used to keep track of the last tweet fetched from a user timeline.

A harvest state store should implement the signature of DictHarvestStateStore.

The behavior of the harvest state store after close() is called is unspecified.
"""


class DictHarvestStateStore():
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
            #Clearing value
            if resource_type in self._state and key in self._state[resource_type]:
                #Delete key
                del self._state[resource_type][key]
                #If resource type is empty then delete
                if not self._state[resource_type]:
                    del self._state[resource_type]

    def close(self):
        """
        Close the harvest state store.

        Close should be called when the harvest state store is no longer needed.
        """
        pass


class JsonHarvestStateStore(DictHarvestStateStore):
    """
    A harvest state store implementation backed by a dictionary and stored as JSON.

    The JSON is written to <collection_path>/state.json.
    """
    def __init__(self, collection_path, load_existing=True, persist_on_close=True):
        DictHarvestStateStore.__init__(self)
        #Load state.  State is what has already been processed from a feed.
        self.state_filepath = os.path.join(collection_path, "state.json")
        if load_existing and os.path.exists(self.state_filepath):
            log.debug("Loading state from %s", self.state_filepath)
            with codecs.open(self.state_filepath, "r") as state_file:
                self._state = json.load(state_file)

        self.persist_on_close = persist_on_close

    def close(self):
        if self.persist_on_close:
            log.debug("Storing harvest state to %s", self.state_filepath)
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

    def close(self):
        pass
