import click
import requests


@click.group()
def tools():
    pass


@tools.command()
@click.option('--base-url',
              default=None,
              required=True,
              help='url to migration service ex: http://localhost:8082')
def upgrade(base_url):
    """Upgrade to latest db schema"""
    url = base_url + "/upgrade"
    response = requests.patch(url)
    print(response.text)


@tools.command()
@click.option('--base-url',
              default=None,
              required=True,
              help='url to migration service ex: http://localhost:8082')
def db_status(base_url):
    """get status of current db schema"""
    url = base_url + "/db_schema_status"
    response = requests.get(url)
    print(response.json())


@tools.command()
@click.option('--base-url',
              default=None,
              required=True,
              help='url to metadata service ex: http://localhost:8080')
def metadata_service_version(base_url):
    """get status of current db schema"""
    url = base_url + "/version"
    response = requests.get(url)
    print(response.text)


cli = click.CommandCollection(sources=[tools])


if __name__ == "__main__":
    cli()
