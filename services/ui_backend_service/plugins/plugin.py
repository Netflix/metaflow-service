import os
import json
import glob
import collections
import pygit2
from typing import List
from services.utils import logging
from aiohttp import web

CONFIG_FILENAME = 'manifest.json'
INSTALLED_PLUGINS_DIR = 'installed'

_dirname = os.path.dirname(os.path.realpath(__file__))
installed_plugins_base_dir = os.environ.get("INSTALLED_PLUGINS_BASE_DIR", _dirname)

PluginConfig = collections.namedtuple("PluginConfig", "name version entrypoint")


class Plugin(object):
    identifier: str = None
    repository: str = None
    ref: str = None
    path: str = None
    parameters: dict = {}

    config: PluginConfig = {}
    files: List[str] = []

    _repo: pygit2.Repository = None

    def __init__(self, identifier: str, repository: str, ref: str = None, parameters: dict = {}, path: str = None, auth: dict = {}):
        self.logger = logging.getLogger("Plugin:{}:{}".format(identifier, path))

        self.identifier = identifier
        self.repository = repository
        self.ref = ref
        self.parameters = parameters

        # Base path for plugin folder
        self.basepath = os.path.join(installed_plugins_base_dir, INSTALLED_PLUGINS_DIR, self.identifier)
        self.logger.info("self.basepath: {}".format(self.basepath))

        # Path to plugin files such as manifest.json.
        # Differs from root path in case of multi-plugin repositories.
        self.filepath = os.path.join(self.basepath, path or "")

        self.credentials = _get_credentials(auth)
        self.callbacks = pygit2.RemoteCallbacks(credentials=self.credentials) if self.credentials else None

    def init(self):
        """
        Init plugin by loading manifest.json and listing available files from filesystem.

        In case of Git repository, clone, fetch changes and checkout to target ref.
        """
        local_repository = pygit2.discover_repository(self.basepath)
        if local_repository:
            self._repo = pygit2.Repository(local_repository)
            self.checkout(self.repository)
        elif self.repository:
            self._repo = pygit2.clone_repository(
                self.repository, self.basepath, bare=False, callbacks=self.callbacks)
            self.checkout()
        else:
            # Target directory is not a Git repository, no need to checkout
            pass

        self.files = self._list_files()
        if not self.files:
            raise PluginException("Error loading plugin files", "plugin-error-files")

        self.config = self._load_config()
        if not self.config:
            raise PluginException("Error loading plugin config", "plugin-error-config")

        return self

    def checkout(self, repository_url: str = None):
        """Fetch latest changes and checkout repository"""
        if self._repo:
            # Update repository url in case it has changed
            if repository_url:
                self._repo.remotes.set_url("origin", repository_url)

            # Fetch latest changes
            for remote in self._repo.remotes:
                remote.fetch(callbacks=self.callbacks)

            # Resolve ref and checkout
            commit, resolved_refish = self._repo.resolve_refish(self.ref if self.ref else 'origin/master')
            self._repo.checkout(resolved_refish, strategy=pygit2.GIT_CHECKOUT_FORCE)

            self.logger.info("Checkout {} at {}".format(resolved_refish.name, commit.short_id))

    @property
    def name(self) -> str:
        return self.config.get("name", self.identifier)

    def _load_config(self) -> PluginConfig:
        try:
            config = json.loads(self.get_file(CONFIG_FILENAME))

            if not all(key in config for key in ('name', 'version', 'entrypoint')):
                return None
            return dict(config)
        except Exception as e:
            self.logger.info("load_config exception:{}".format(e))
            return None

    def _list_files(self) -> List[str]:
        files = []
        static_files = glob.glob(os.path.join(self.filepath, '**'), recursive=True)
        for filepath in static_files:
            filename = os.path.basename(filepath)
            if not os.path.isdir(filepath):
                files.append(filename)
        return files

    def has_file(self, filename) -> bool:
        return filename in self.files

    def get_file(self, filename):
        """Return file contents"""
        if not self.has_file(filename):
            return None

        try:
            with open(os.path.join(self.filepath, filename), 'r') as file:
                return file.read()
        except Exception as e:
            self.logger.info("get_file exception for: {}: {}".format(filename, e))
            return None

    def serve(self, filename):
        """Serve files from plugin repository"""
        if not self.has_file(filename):
            return web.Response(status=404, body="File not found")
        return web.FileResponse(os.path.join(self.filepath, filename))

    def __iter__(self):
        for key in ["identifier", "name", "repository", "ref", "parameters", "config", "files"]:
            yield key, getattr(self, key)


class PluginException(Exception):
    def __init__(self, msg="Error loading plugin", id="plugin-error-unknown", traceback_str=None):
        self.message = msg
        self.id = id
        self.traceback_str = traceback_str

    def __str__(self):
        return self.message


def _get_credentials(_auth):
    if not _auth:
        return None

    _agent = _auth.get("agent", False)
    _public_key = _auth.get("public_key", None)
    _private_key = _auth.get("private_key", None)
    _username = _auth.get("user", None)
    _passphrase = _auth.get("pass", None)

    if _agent:
        credentials = pygit2.KeypairFromAgent(_username or "git")
    elif _public_key and _private_key:
        if os.path.exists(_public_key) and os.path.exists(_private_key):
            credentials = pygit2.Keypair(_username or "git", _public_key, _private_key, _passphrase or "")
        else:
            credentials = pygit2.KeypairFromMemory(_username or "git", _public_key, _private_key, _passphrase or "")
    elif _username and _passphrase:
        credentials = pygit2.UserPass(_username, _passphrase)
    elif _username:
        credentials = pygit2.Username(_username)
    else:
        credentials = None

    return credentials
