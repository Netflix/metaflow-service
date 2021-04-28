from services.utils import logging
from ..api.utils import get_json_from_env

from .plugin import (Plugin, PluginException)

_PLUGINS = {}

logger = logging.getLogger("Plugin")


def list_plugins():
    global _PLUGINS
    return _PLUGINS


def init_plugins():
    global _PLUGINS

    logger.info("Init plugins")

    plugins = get_json_from_env("PLUGINS")
    if plugins:
        for name, value in plugins.items():
            if isinstance(value, str):
                repository = value
                ref = None
                paramaters = {}
            elif isinstance(value, dict):
                repository = value.get("repository", None)
                ref = value.get("ref", None)
                paramaters = value.get("parameters", {})
            else:
                logger.warn("   [{}] Invalid plugin format, skipping".format(name))
                continue

            try:
                plugin = Plugin(name, repository=repository, ref=ref, parameters=paramaters)
                _PLUGINS[name] = plugin.init()
            except PluginException as err:
                logger.error("  [{}] PluginException: {}".format(name, err))
            except Exception as err:
                logger.error("  [{}] Unknown error loading plugin {}".format(name, err))

    logger.info("Plugins ready: {}".format(list(_PLUGINS.keys())))


def reset_plugins():
    global _PLUGINS
    _PLUGINS = {}
