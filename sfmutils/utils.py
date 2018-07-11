import string
import os
import datetime
from pytz import timezone

time_zone = timezone(os.getenv('TZ', "America/New_York"))


def safe_string(unsafe_str, replace_char="_"):
    """
    Replace all non-ascii characters or digits in provided string with a
    replacement character.
    """
    return ''.join([c if c in string.ascii_letters or c in string.digits else replace_char for c in unsafe_str])


def datetime_now():
    return datetime.datetime.now(time_zone)


def datetime_from_stamp(timestamp):
    return datetime.datetime.fromtimestamp(timestamp, time_zone)
