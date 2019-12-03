import boto3
import json
import os
from aiohttp import web
from botocore.client import Config
from .utils import get_traceback_str


class AuthApi(object):
    def __init__(self, app):
        app.router.add_route("GET", "/auth/token", self.get_authorization_token)
        app.router.add_route("GET", "/ping", self.ping)

    async def ping(self, request):
        """
        ---
        description: This end-point allow to test that service is up.
        tags:
        - Admin
        produces:
        - 'text/plain'
        responses:
            "202":
                description: successful operation. Return "pong" text
            "405":
                description: invalid HTTP Method
        """
        return web.Response(text="pong")

    async def get_authorization_token(self, request):
        """
        ---
        description: this is used exclusively for sandbox auth
        tags:
        - Auth
        produces:
        - text/plain
        responses:
            "200":
                description: successfully returned certs
            "403":
                description: no token for you
            "405":
                description: invalid HTTP Method
            "500":
                description: internal server error
        """
        try:
            role_arn = os.environ.get("MF_USER_IAM_ROLE")
            region_name = os.environ.get("MF_REGION", "us-west-2")
            endpoint_url = os.environ.get(
                "MF_STS_ENDPOINT", "https://sts.us-west-2.amazonaws.com"
            )
            config = Config(connect_timeout=1, read_timeout=1)
            sts_connection = boto3.client(
                "sts", config=config, region_name=region_name, endpoint_url=endpoint_url
            )

            assumed_role = sts_connection.assume_role(
                RoleArn=role_arn, RoleSessionName="acct_role"
            )

            credentials = {}
            credentials["aws_access_key_id"] = assumed_role["Credentials"][
                "AccessKeyId"
            ]
            credentials["aws_secret_access_key"] = assumed_role["Credentials"][
                "SecretAccessKey"
            ]
            credentials["aws_session_token"] = assumed_role["Credentials"][
                "SessionToken"
            ]

            return web.Response(status=200, body=json.dumps(credentials))
        except Exception as ex:
            body = {"err_msg": str(ex), "traceback": get_traceback_str()}
            return web.Response(status=500, body=json.dumps(body))
