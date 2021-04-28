from services.utils import handle_exceptions, web_response
from ..plugins import list_plugins


class PluginsApi(object):
    """
    Adds an Api endpoint for fetching UI plugins.
    """

    def __init__(self, app):
        app.router.add_route('GET', '/plugin', self.get_plugins)
        app.router.add_route("GET", "/plugin/{plugin_name}", self.get_plugin)
        app.router.add_route("GET", "/plugin/{plugin_name}/{filename:.+}", self.get_plugin_asset)

    @handle_exceptions
    async def get_plugins(self, request):
        """
        ---
        description: List all plugins
        tags:
        - Plugin
        produces:
        - application/json
        responses:
            "200":
                description: Returns list of all plugins
                schema:
                  $ref: '#/definitions/ResponsesPluginList'
            "405":
                description: invalid HTTP Method
                schema:
                  $ref: '#/definitions/ResponsesError405'
        """
        plugins = []
        for plugin in list_plugins().values():
            plugins.append(_plugin_dict(plugin))

        return web_response(200, plugins)

    @handle_exceptions
    async def get_plugin(self, request):
        """
        ---
        description: Get one plugin
        tags:
        - Plugin
        parameters:
          - $ref: '#/definitions/Params/Path/plugin_name'
        produces:
        - application/json
        responses:
            "200":
                description: Returns one plugin
                schema:
                  $ref: '#/definitions/ResponsesPlugin'
            "405":
                description: invalid HTTP Method
                schema:
                  $ref: '#/definitions/ResponsesError405'
        """
        plugin = _get_plugin_from_request(request)
        if not plugin:
            return web_response(404, "Plugin not found")

        return web_response(200, _plugin_dict(plugin))

    @handle_exceptions
    async def get_plugin_asset(self, request):
        """
        ---
        description: Serve plugin asset
        tags:
        - Plugin
        parameters:
          - $ref: '#/definitions/Params/Path/plugin_name'
          - $ref: '#/definitions/Params/Path/plugin_filename'
        produces:
        - application/json
        responses:
            "200":
                description: Serve plugin asset, e.g. dist/index.html
            "405":
                description: invalid HTTP Method
                schema:
                  $ref: '#/definitions/ResponsesError405'
        """
        plugin = _get_plugin_from_request(request)
        if not plugin:
            return web_response(404, "Plugin not found")

        filename = request.match_info.get("filename")
        try:
            return plugin.serve(filename)
        except:
            return web_response(500, "Internal server error")


def _get_plugin_from_request(request):
    _plugins = list_plugins()
    plugin_name = request.match_info.get("plugin_name")
    if plugin_name not in _plugins:
        return None
    return _plugins[plugin_name]


def _plugin_dict(plugin):
    return {
        "name": plugin.name,
        "repository": plugin.repository,
        "ref": plugin.ref,
        "parameters": plugin.parameters,
        "config": plugin.config,
        "files": plugin.files
    }
