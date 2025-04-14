from metaflow.plugins.datastores.azure_storage import AzureStorage
from metaflow.plugins.datastores.gs_storage import GSStorage
from metaflow.plugins.datastores.s3_storage import S3Storage
from metaflow.metaflow_config import DEFAULT_DATASTORE, CARD_SUFFIX
from metaflow.plugins.cards.card_datastore import (
    CardPathSuffix,
    CardDatastore,
    CardNameSuffix,
)
import time
from typing import Tuple, Any

import os


def _make_path(base_pth, pathspec=None, with_steps=False, suffix=CardPathSuffix.CARD):
    sysroot = base_pth
    flow_name, run_id, step_name, task_id = pathspec.split("/")

    # We have a condition that checks for `with_steps` because
    # when cards were introduced there was an assumption made
    # about task-ids being unique.
    # This assumption is incorrect since pathspec needs to be
    # unique but there is no such guarantees on task-ids.
    # This is why we have a `with_steps` flag that allows
    # constructing the path with and without steps so that
    # older-cards (cards with a path without `steps/<stepname>` in them)
    # can also be accessed by the card cli and the card client.
    if with_steps:
        pth_arr = [
            sysroot,
            flow_name,
            "runs",
            run_id,
            "steps",
            step_name,
            "tasks",
            task_id,
            suffix,
        ]
    else:
        pth_arr = [
            sysroot,
            flow_name,
            "runs",
            run_id,
            "tasks",
            task_id,
            suffix,
        ]
    if sysroot == "" or sysroot is None:
        pth_arr.pop(0)
    return os.path.join(*pth_arr)


def get_storage_client(storage_type, storage_root):
    _base_root = os.path.join(storage_root, CARD_SUFFIX)
    if storage_type == "s3":
        return S3Storage(_base_root)
    if storage_type == "gs":
        return GSStorage(_base_root)
    if storage_type == "azure":
        return AzureStorage(_base_root)


class DynamicCardGetClients:

    def __init__(self, client_refresh_timings=60 * 60) -> None:
        self._init_time = time.time()
        self.client_refresh_timings = client_refresh_timings
        self._clients = {}

    def __getitem__(self, storage_root) -> "CardGetClient":
        if storage_root not in self._clients:
            self._clients[storage_root] = CardGetClient(storage_root, self.client_refresh_timings)
        return self._clients[storage_root]


class CardGetClient:

    def __init__(self, datastore_root , client_refresh_timings=60 * 60) -> None:
        self._init_time = time.time()
        self.client_refresh_timings = client_refresh_timings
        self._datastore_root = datastore_root
        self._setup_client(datastore_root)

    def _setup_client(self, datastore_root):
        self._client = get_storage_client(DEFAULT_DATASTORE, datastore_root)
        self._init_time = time.time()

    @property
    def client(self):
        if time.time() - self._init_time > self.client_refresh_timings:
            self._setup_client(
                self._datastore_root
            )
        return self._client

    def _make_card_path(
        self, pathspec, card_type, card_uuid, card_user_id, object_type="card"
    ):
        path_suffix, name_suffix = CardPathSuffix.CARD, CardNameSuffix.CARD
        if object_type == "data":
            path_suffix, name_suffix = CardPathSuffix.DATA, CardNameSuffix.DATA

        path = _make_path(
            "",
            pathspec,
            with_steps=True,
            suffix=path_suffix,
        )

        return CardDatastore.get_card_location(
            path,
            card_type,
            card_uuid,
            card_user_id,
            name_suffix,
        )

    def download(
        self, pathspec, card_type, card_uuid, card_user_id, object_type="card"
    ):
        final_path = self._make_card_path(
            pathspec, card_type, card_uuid, card_user_id, object_type
        )
        with self.client.load_bytes([final_path]) as ff:
            for key, file_path, metadata in ff:
                with open(file_path) as f:
                    return f.read()

    def fetch(
        self,
        pathspec,
        card_type,
        card_uuid,
        card_user_id,
        object_type="card",
    ) -> Tuple[str, str, Any]:
        final_path = self._make_card_path(
            pathspec, card_type, card_uuid, card_user_id, object_type
        )
        return self.client.load_bytes([final_path])
