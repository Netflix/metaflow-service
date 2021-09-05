import os
from services.utils import handle_exceptions, web_response

# These environment values will be available to the frontend
ALLOWED_CONFIG_KEYS = [
    'GA_TRACKING_ID'
]


class ConfigApi(object):
    """
    Adds an Api endpoint for fetching required configuration variables for the frontend.
    """
    def __init__(self, app):
        app.router.add_route('GET', '/config', self.get_config)

    @handle_exceptions
    async def get_config(self, request):
        """
        ---
        description: Get all frontend configuration key-value pairs.
        tags:
        - Admin
        produces:
        - application/json
        responses:
            "200":
                description: Returns all allowed configuration key-value pairs for the frontend.
                schema:
                    type: object
                    properties:
                        "ALLOWED_CONFIG_VARIABLE":
                            type: string
                            example: "value-to-pass-frontend-1234"
                            description: "A frontend configuration variable from the server environment. These are exposed based on a whitelist on the server."
            "405":
                description: invalid HTTP Method
         """
        config = {}
        for key in ALLOWED_CONFIG_KEYS:
            val = os.environ.get(key, None)
            if val:
                config[key] = val
        return web_response(200, config)
