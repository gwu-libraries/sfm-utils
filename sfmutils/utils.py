import string


def safe_string(str, replace_char="_"):
    """
    Replace all non-ascii characters or digits in provided string with a
    replacement character.
    """
    return ''.join([c if c in string.ascii_letters or c in string.digits else replace_char for c in str])