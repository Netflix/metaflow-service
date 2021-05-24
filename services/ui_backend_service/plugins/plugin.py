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

    def __init__(self, identifier: str, repository: str, ref: str = None, parameters: dict = {}, path: str = None):
        self.logger = logging.getLogger("Plugin:{}:{}".format(identifier, path))

        self.identifier = identifier
        self.repository = repository
        self.ref = ref
        self.parameters = parameters

        if path:
            self.path = os.path.join(_dirname, INSTALLED_PLUGINS_DIR, self.identifier, path)
        else:
            self.path = os.path.join(_dirname, INSTALLED_PLUGINS_DIR, self.identifier)

    def init(self):
        """
        Init plugin by loading manifest.json and listing available files from filesystem.

        In case of Git repository, clone, fetch changes and checkout to target ref.
        """
        local_repository = pygit2.discover_repository(self.path)
        if local_repository:
            self._repo = pygit2.Repository(local_repository)
            self.checkout(self.repository)
        elif self.repository:
            self._repo = pygit2.clone_repository(self.repository, self.path, bare=False)
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
                remote.fetch()

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
        except:
            return None

    def _list_files(self) -> List[str]:
        files = []
        static_files = glob.glob(os.path.join(self.path, '**'), recursive=True)
        for filepath in static_files:
            filename = filepath[len(self.path) + 1:]
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
            with open(os.path.join(self.path, filename), 'r') as file:
                return file.read()
        except:
            return None

    def serve(self, filename):
        """Serve files from plugin repository"""
        if not self.has_file(filename):
            return web.Response(status=404, body="File not found")
        return web.FileResponse(os.path.join(self.path, filename))


class PluginException(Exception):
    def __init__(self, msg="Error loading plugin", id="plugin-error-unknown", traceback_str=None):
        self.message = msg
        self.id = id
        self.traceback_str = traceback_str

    def __str__(self):
        return self.message
