from services.utils import logging
from ..api.utils import get_json_from_env

from .plugin import (Plugin, PluginException)

_PLUGINS = []

logger = logging.getLogger("Plugin")


def list_plugins():
    global _PLUGINS
    return _PLUGINS


def init_plugins():
    global _PLUGINS

    logger.info("Init plugins")

    plugins = get_json_from_env("PLUGINS")
    if plugins:
        for identifier, value in plugins.items():
            if isinstance(value, str):
                repository = value
                ref = None
                parameters = {}
                paths = None
            elif isinstance(value, dict):
                repository = value.get("repository", None)
                ref = value.get("ref", None)
                parameters = value.get("parameters", {})
                paths = value.get("paths", None)
            else:
                logger.warn("   [{}] Invalid plugin format, skipping".format(identifier))
                continue

            if paths and isinstance(paths, list):
                for path in paths:
                    _load_plugin(identifier=identifier, repository=repository, ref=ref, parameters=parameters, path=path)
            else:
                _load_plugin(identifier=identifier, repository=repository, ref=ref, parameters=parameters)

    logger.info("Plugins ready: {}".format(list(_PLUGINS)))


def _load_plugin(identifier: str, repository: str = None, ref: str = None, parameters: dict = {}, path: str = None):
    global _PLUGINS
    try:
        plugin = Plugin(identifier=identifier, repository=repository, ref=ref, parameters=parameters, path=path)
        _PLUGINS.append(plugin.init())
    except PluginException as err:
        logger.error("  [{}:{}] PluginException: {}".format(identifier, path, err))
    except Exception as err:
        logger.error("  [{}:{}] Unknown error loading plugin {}".format(identifier, path, err))


def _reset_plugins():
    global _PLUGINS
    _PLUGINS = []
