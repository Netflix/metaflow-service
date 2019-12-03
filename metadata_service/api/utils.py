import json
import sys
import traceback


async def read_body(request_content):
    byte_array = bytearray()
    while not request_content.at_eof():
        data = await request_content.read(4)
        byte_array.extend(data)

    return json.loads(byte_array.decode("utf-8"))


def get_traceback_str():
    """Get the traceback as a string."""

    exc_info = sys.exc_info()
    stack = traceback.extract_stack()
    _tb = traceback.extract_tb(exc_info[2])
    full_tb = stack[:-1] + _tb
    exc_line = traceback.format_exception_only(*exc_info[:2])
    return "\n".join(
        [
            "Traceback (most recent call last):",
            "".join(traceback.format_list(full_tb)),
            "".join(exc_line),
        ]
    )
