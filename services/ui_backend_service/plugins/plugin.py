import os
import json
import glob
import pygit2
from typing import List
from services.utils import logging
from aiohttp import web

CONFIG_FILENAME = 'manifest.json'
INSTALLED_PLUGINS_DIR = 'installed'

_dirname = os.path.dirname(os.path.realpath(__file__))


class Plugin(object):
    name: str = None
    repository: str = None
    ref: str = None
    path: str = None
    parameters: dict = {}

    config: dict = {}
    files: List[str] = []

    _repo: pygit2.Repository = None

    def __init__(self, name: str, repository: str, ref: str = None, parameters: dict = {}):
        self.logger = logging.getLogger("Plugin:{}".format(name))

        self.name = name
        self.repository = repository
        self.ref = ref
        self.path = os.path.join(_dirname, INSTALLED_PLUGINS_DIR, self.name)
        self.parameters = parameters

    def init(self):
        """
        Init plugin by loading manifest.json and listing available files from filesystem.

        In case of Git repository, clone, fetch changes and checkout to target ref.
        """
        local_repository = pygit2.discover_repository(self.path)
        if local_repository:
            self._repo = pygit2.Repository(local_repository)
            self.checkout()
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

    def checkout(self):
        """Fetch latest changes and checkout repository"""
        if self._repo:
            # Fetch latest changes
            for remote in self._repo.remotes:
                remote.fetch()

            # Resolve ref and checkout
            commit, resolved_refish = self._repo.resolve_refish(self.ref if self.ref else 'HEAD')
            self._repo.checkout(resolved_refish, strategy=pygit2.GIT_CHECKOUT_FORCE)

            self.logger.info("Checkout {} at {}".format(resolved_refish.name, commit.short_id))

    def _load_config(self):
        try:
            config = json.loads(self.get_file(CONFIG_FILENAME))
            if not all(key in config for key in ('name', 'version', 'entrypoint')):
                return None
            return config
        except:
            return None

    def _list_files(self):
        files = []
        static_files = glob.glob(os.path.join(self.path, '**'), recursive=True)
        for filepath in static_files:
            filename = filepath[len(self.path) + 1:]
            if not os.path.isdir(filepath):
                files.append(filename)
        return files

    def has_file(self, filename):
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
