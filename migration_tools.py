import os
import click
import requests
import json


def _init_config():
    # Read configuration from $METAFLOW_HOME/config_<profile>.json.
    config = {}
    try:
        home = os.environ.get('METAFLOW_HOME', '~/.metaflowconfig')
        profile = os.environ.get('METAFLOW_PROFILE')
        path_to_config = os.path.join(home, 'config.json')
        if profile:
            path_to_config = os.path.join(home, 'config_%s.json' % profile)
        path_to_config = os.path.expanduser(path_to_config)

        if os.path.exists(path_to_config):
            with open(path_to_config) as f:
                return json.load(f)
    except Exception:
        pass
    return config


def _get_url(url, service='migration'):
    if url:
        return url

    metaflow_config = _init_config()

    default = 'http://localhost:8082'
    if service is not 'migration':
        default = 'http://localhost:8080'

    return metaflow_config.get('METAFLOW_SERVICE_URL', default)


@click.group()
def tools():
    pass


@tools.command()
@click.option('--base-url',
              default=None,
              required=False,
              help='defaults to reading ~/.metaflowconfig and fallback to '
                   'http://localhost:8082')
def upgrade(base_url):
    """Upgrade to latest db schema"""
    _url = _get_url(base_url)
    url = _url + "/upgrade"
    response = requests.patch(url)
    print(response.text)


@tools.command()
@click.option('--base-url',
              default=None,
              required=False,
              help='defaults to reading ~/.metaflowconfig and fallback to '
                   'http://localhost:8082')
def db_status(base_url):
    """get status of current db schema"""
    _url = _get_url(base_url)
    url = _url + "/db_schema_status"
    response = requests.get(url)
    print(response.json())


@tools.command()
@click.option('--base-url',
              default=None,
              required=False,
              help='defaults to reading ~/.metaflowconfig and fallback to '
                   'http://localhost:8080')
def metadata_service_version(base_url):
    """get status of current db schema"""
    _url = _get_url(base_url, 'metadata')
    url = _url + "/version"
    response = requests.get(url)
    print(response.text)


cli = click.CommandCollection(sources=[tools])


if __name__ == "__main__":
    cli()
