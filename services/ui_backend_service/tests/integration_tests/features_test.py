import pytest
from .utils import (
    init_app, set_env
)
pytestmark = [pytest.mark.integration_tests]

# Fixtures begin


@pytest.fixture
def cli(loop, aiohttp_client):
    return init_app(loop, aiohttp_client)

# Fixtures end


async def get_features(cli):
    return await (await cli.get('/features')).json()


async def expect_features(cli, expected_features={}):
    assert await get_features(cli) == expected_features


async def test_features_none(cli):
    with set_env():
        assert await get_features(cli) == {}


async def test_features_true(cli):
    with set_env({
        'FEATURE_ONE': 'true',
        'FEATURE_SECOND': 'foo',
        'FEATURE_THIRD': '1',
        'FEATURE_FOURTH': '',
        'FEATURE_FIFTH': ' '
    }):
        await expect_features(cli, {
            'FEATURE_ONE': True,
            'FEATURE_SECOND': True,
            'FEATURE_THIRD': True,
            'FEATURE_FOURTH': True,
            'FEATURE_FIFTH': True
        })


async def test_features_false(cli):
    with set_env({'FEATURE_ONE': 'false'}):
        await expect_features(cli, {
            'FEATURE_ONE': False
        })


async def test_features_f(cli):
    with set_env({'FEATURE_ONE': 'f'}):
        await expect_features(cli, {
            'FEATURE_ONE': False
        })


async def test_features_0(cli):
    with set_env({'FEATURE_ONE': '0'}):
        await expect_features(cli, {
            'FEATURE_ONE': False
        })


async def test_features_only(cli):
    with set_env({
        'FEATURE_FOO': 'true',
        'ANOTHER_ENV_VAR': 'bar'
    }):
        await expect_features(cli, {
            'FEATURE_FOO': True
        })
