import os
import glob

from aiohttp import web

dirname = os.path.dirname(os.path.realpath(__file__))
static_ui_path = os.path.join(dirname, "ui")


METAFLOW_SERVICE = os.environ.get("METAFLOW_SERVICE", "/")


class Frontend(object):
    '''Provides routes for the static UI webpage.
    Require this as the last Api, as it is a catch-all route.
    '''

    def __init__(self, app):
        app.router.add_static('/static',
                              path=os.path.join(static_ui_path, "static"),
                              name='static')

        # serve the root static files separately.
        static_files = glob.glob(os.path.join(static_ui_path, "*.*"))
        for filepath in static_files:
            filename = filepath[len(static_ui_path) + 1:]
            app.router.add_route(
                '*', f'/{filename}', self.serve_file(filename))

        # catch-all route that unfortunately messes with root static file serving.
        # Refreshing SPA pages won't work without the tail.
        app.router.add_route('*', '/{tail:.*}', self.serve_index_html)

    def serve_file(self, filename: str):
        "Generator for single static file serving handlers"
        async def filehandler(request):
            return web.FileResponse(os.path.join(static_ui_path, filename))
        return filehandler

    async def serve_index_html(self, request):
        "Serve index.html by injecting `METAFLOW_SERVICE` variable to define API base url."
        try:
            with open(os.path.join(static_ui_path, "index.html")) as f:
                content = f.read() \
                    .replace("</head>",
                             "<script>window.METAFLOW_SERVICE=\"{METAFLOW_SERVICE}\";</script></head>".format(METAFLOW_SERVICE=METAFLOW_SERVICE))
                return web.Response(text=content, content_type='text/html')
        except Exception as err:
            return web.Response(text=str(err), status=500, content_type='text/plain')
