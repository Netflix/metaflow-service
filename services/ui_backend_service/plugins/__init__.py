from services.utils import logging
from ..api.utils import get_json_config

from .plugin import (Plugin, PluginException)

_PLUGINS = []

logger = logging.getLogger("Plugin")


def list_plugins():
    global _PLUGINS
    return _PLUGINS


def init_plugins():
    global _PLUGINS

    logger.info("Init plugins")

    plugins = get_json_config("plugins")
    if plugins:
        global_auth = None
        if "auth" in plugins and isinstance(plugins["auth"], dict):
            global_auth = plugins["auth"]

        for identifier, value in plugins.items():
            if isinstance(value, str):
                repository = value
                ref = None
                parameters = {}
                paths = None
                auth = global_auth
            elif identifier == "auth":
                continue
            elif isinstance(value, dict):
                repository = value.get("repository", None)
                ref = value.get("ref", None)
                parameters = value.get("parameters", {})
                paths = value.get("paths", None)
                if "auth" in value:
                    auth = value.get("auth", None)
                else:
                    auth = global_auth
            else:
                logger.warning("   [{}] Invalid plugin format, skipping".format(identifier))
                continue

            if paths and isinstance(paths, list):
                for path in paths:
                    _load_plugin(identifier=identifier, repository=repository, ref=ref, parameters=parameters, path=path, auth=auth)
            else:
                _load_plugin(identifier=identifier, repository=repository, ref=ref, parameters=parameters, auth=auth)

    logger.info("Plugins ready: {}".format(list(map(lambda p: p.identifier, _PLUGINS))))


def _load_plugin(identifier: str, repository: str = None, ref: str = None, parameters: dict = {}, path: str = None, auth: dict = {}):
    global _PLUGINS
    try:
        plugin = Plugin(identifier=identifier, repository=repository, ref=ref, parameters=parameters, path=path, auth=auth)
        _PLUGINS.append(plugin.init())
    except PluginException as err:
        logger.error("  [{}:{}] PluginException: {}".format(identifier, path, err))
    except Exception as err:
        logger.error("  [{}:{}] Unknown error loading plugin {}".format(identifier, path, err))


def _reset_plugins():
    global _PLUGINS
    _PLUGINS = []
