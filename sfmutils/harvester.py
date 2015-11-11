from collections import Counter, namedtuple


STATUS_SUCCESS = "completed success"
STATUS_FAILURE = "completed failure"
STATUS_RUNNING = "running"

class HarvestResult():
    """
    A data transfer object for keeping track of the results of a harvest.
    """
    def __init__(self):
        self.success = True
        self.infos = []
        self.warnings = []
        self.errors = []
        self.urls = []
        self.summary = Counter()
        #Map of uids to tokens for which tokens have been found to have changed.
        self.token_updates = {}
        #Map of tokens to uids for tokens for which uids have been found.
        self.uids = {}

    def __nonzero__(self):
        return 1 if self.success else 0

    def __str__(self):
        harv_str = "Harvest response is {}.".format(self.success)
        harv_str += self._str_messages(self.infos, "Informational")
        harv_str += self._str_messages(self.warnings, "Warning")
        harv_str += self._str_messages(self.errors, "Error")
        if self.urls:
            harv_str += " Urls: %s" % self.urls
        if self.summary:
            harv_str += " Harvest summary: {}".format(self.summary)
        if self.token_updates:
            harv_str += " Token updates: {}".format(self.token_updates)
        if self.uids:
            harv_str += " Uids: {}".format(self.uids)
        return harv_str

    @staticmethod
    def _str_messages(messages, name):
        msg_str = ""
        if messages:
            msg_str += " {} messages are:".format(name)

        for (i, msg) in enumerate(messages, start=1):
            msg_str += "({}) [{}] {}".format(i, msg["code"], msg["message"])

        return msg_str

    def merge(self, other):
        self.success = self.success and other.success
        self.infos.extend(other.infos)
        self.warnings.extend(other.warnings)
        self.errors.extend(other.errors)
        self.urls.extend(other.urls)
        self.summary.update(other.summary)

    def urls_as_set(self):
        return set(self.urls)

    def increment_summary(self, key, increment=1):
        self.summary[key] += increment

CODE_UNKNOWN_ERROR = "unknown_error"


class Msg():
    """
    An informational, warning, or error message to be included in the harvest
    status.

    Where possible, code should be selected from harvest.CODE_*.
    """
    def __init__(self, code, message):
        assert code
        assert message
        self.code = code
        self.message = message

    def to_map(self):
        return {
            "code": self.code,
            "message": self.message
        }