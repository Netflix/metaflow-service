from typing import Tuple, Optional


# Shared helpers


def unpack_processed_value(value) -> Tuple[bool, Optional[str], Optional[str], Optional[str]]:
    '''
    Unpack cached value returning tuple of: success, value, detail, stacktrace

    Defaults to None in case values are not defined.

    Success example:
        True, 'foo', None

    Failure examples:
        False, 'failure-id', 'error-details', None
        False, 'failure-id-without-details', None, None
        False, None, None, None
        False, 'CustomError', 'Custom failure description', 'stacktrace of error'

    Returns
    -------
    tuple : (bool, optional(str), optional(str), optional(str))
        success, value, description, stacktrace
    '''
    return (list(value) + [None] * 4)[:4]
