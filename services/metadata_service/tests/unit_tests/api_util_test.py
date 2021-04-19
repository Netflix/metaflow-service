import json
from services.metadata_service.api.utils import handle_exceptions, format_response

async def test_handle_exceptions():

    @handle_exceptions
    async def do_not_raise():
        return True

    @format_response
    @handle_exceptions
    async def raise_without_id():
        raise Exception("test")

    # wrapper should not touch successful calls.
    assert (await do_not_raise())

    # NOTE: aiohttp Response StringPayload only has the internal property _value for accessing the payload value.

    response_without_id = await raise_without_id()
    assert response_without_id.status == 500
    _body = json.loads(response_without_id.body._value)
    assert _body['traceback'] is not None
