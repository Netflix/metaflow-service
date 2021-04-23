import os
import json
import glob
import pygit2

PLUGINS = {}
CONFIG_FILENAME = 'manifest.json'

_dirname = os.path.dirname(os.path.realpath(__file__))


class RepositoryCallbacks(pygit2.RemoteCallbacks):
    def credentials(self, url, username_from_url, allowed_types):
        if allowed_types & pygit2.credentials.GIT_CREDENTIAL_USERNAME:
            return pygit2.Username("git")
        elif allowed_types & pygit2.credentials.GIT_CREDENTIAL_SSH_KEY:
            return pygit2.Keypair("git", "id_rsa.pub", "id_rsa", "")
        else:
            return None


class Plugin(object):
    name: str = None
    repository: str = None
    ref: str = None
    path: str = None
    parameters: dict = {}

    commit: pygit2.Commit = None
    repo: pygit2.Repository = None

    def __init__(self, name: str, repository: str, ref: str = None, parameters: dict = {}):
        self.name = name
        self.repository = repository
        self.ref = ref
        self.path = os.path.join(_dirname, 'installed', self.name)
        self.parameters = parameters

    def init(self):
        print("Init plugin {}".format(self.name), flush=True)

        local_repository = pygit2.discover_repository(self.path)
        if local_repository:
            self.repo = pygit2.Repository(local_repository)
            self.checkout()
        elif self.repository:
            self.repo = pygit2.clone_repository(self.repository, self.path, bare=False, callbacks=RepositoryCallbacks())
            self.checkout()
        else:
            # Not Git repository
            pass

        self.config = self.get_config()
        if not self.config:
            raise PluginException

        return self

    def checkout(self):
        # Fetch latest changes
        for remote in self.repo.remotes:
            remote.fetch()

        # Resolve refish and checkout
        self.commit, resolved_refish = self.repo.resolve_refish(self.ref if self.ref else 'master')
        self.repo.checkout(resolved_refish, strategy=pygit2.GIT_CHECKOUT_FORCE)

    def get_config(self):
        try:
            return json.loads(self.get_file(CONFIG_FILENAME))
        except:
            return None

    def list_files(self):
        files = []
        static_files = glob.glob(os.path.join(self.path, '**'), recursive=True)
        for filepath in static_files:
            filename = filepath[len(self.path) + 1:]
            if not os.path.isdir(filepath):
                files.append(filename)
        return files

    def has_file(self, filename):
        return filename in self.list_files()

    def get_file(self, filename):
        try:
            with open(os.path.join(self.path, filename), 'r') as file:
                return file.read()
        except:
            return None


def _get_list_of_plugins_from_env():
    try:
        return json.loads(os.environ.get('PLUGINS'))
    except:
        return None


def init_plugins():
    print("Init plugins", flush=True)

    plugins = _get_list_of_plugins_from_env()

    for name, value in plugins.items():
        print("Found plugin {}: {}".format(name, value), flush=True)

        if isinstance(value, str):
            repository = value
            ref = None
            paramaters = {}
        elif isinstance(value, dict):
            repository = value.get("repository", None)
            ref = value.get("ref", None)
            paramaters = value.get("parameters", {})
        else:
            print("Invalid plugin format, skipping {}")
            continue

        try:
            plugin = Plugin(name, repository=repository, ref=ref, parameters=paramaters)
            plugin.init()
            PLUGINS[name] = plugin
        except PluginException as e:
            print("PluginException {}".format(e), flush=True)
        except Exception as e:
            print("Unknown error loading plugin {}".format(e), flush=True)


class PluginException(Exception):
    def __str__(self):
        return "Error loading plugin"
