import os
from services.utils import handle_exceptions, web_response

from ..plugins import PLUGINS

from aiohttp import web


class PluginsApi(object):
    """
    Adds an Api endpoint for fetching required configuration variables for the frontend.
    """

    def __init__(self, app):
        app.router.add_route('GET', '/plugin', self.get_plugins)
        app.router.add_route("GET", "/plugin/{plugin_name}", self.get_plugin)
        app.router.add_route("GET", "/plugin/{plugin_name}/{filename:.+}", self.get_plugin_asset)

    @handle_exceptions
    async def get_plugins(self, request):
        plugins = []
        for plugin in PLUGINS.values():
            plugins.append(_plugin_dict(plugin))

        return web_response(200, plugins)

    @handle_exceptions
    async def get_plugin(self, request):
        plugin_name = request.match_info.get("plugin_name")
        if plugin_name not in PLUGINS:
            return web_response(404, "Plugin not found")

        plugin = PLUGINS[plugin_name]

        return web_response(200, _plugin_dict(plugin))

    @handle_exceptions
    async def get_plugin_asset(self, request):
        plugin_name = request.match_info.get("plugin_name")
        if plugin_name not in PLUGINS:
            return web_response(404, "Plugin not found")

        plugin = PLUGINS[plugin_name]

        try:
            filename = request.match_info.get("filename")
            if filename and plugin.has_file(filename):
                return web.FileResponse(os.path.join(plugin.path, filename))
        except:
            return web_response(404, "File not found")


def _plugin_dict(plugin):
    return {
        "name": plugin.name,
        "repository": plugin.repository,
        "ref": plugin.ref,
        "parameters": plugin.parameters,
        "config": plugin.config,
        "files": plugin.list_files()
    }
