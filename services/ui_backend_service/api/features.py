import os

from services.utils import handle_exceptions, web_response

from ..features import get_features


class FeaturesApi(object):
    """
    Adds an Api endpoint that returns a list of enabled/disabled features for the UI Backend Service
    """

    def __init__(self, app):
        app.router.add_route("GET", "/features", self.get_all_features)

    @handle_exceptions
    async def get_all_features(self, request):
        """
        ---
        description: Get all of enabled/disabled features as key-value pairs.
        tags:
        - Admin
        produces:
        - application/json
        responses:
            "200":
                description: Returns all features to be enabled or disabled by the frontend.
                schema:
                    type: object
                    properties:
                        "FEATURE_*":
                            type: boolean
                            example: true
                            description: "An environment variable from the server with a FEATURE_ prefix, and its value as a boolean"
            "405":
                description: invalid HTTP Method
        """
        return web_response(200, get_features())
