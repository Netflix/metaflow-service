from time import perf_counter
from metaflow._vendor import click
import time
import os
from threading import Thread
from metaflow import Task, namespace, Run
from metaflow.cards import get_cards
from metaflow.plugins.cards.card_client import Card
from metaflow.exception import MetaflowNotFound
from metaflow.plugins.cards.exception import CardNotPresentException, UnresolvableDatastoreException
import json
import hashlib
import shutil
import logging
from typing import Dict, List
from collections import namedtuple

ResolvedCards = namedtuple("ResolvedCards", ["cards", "unresolvable",])


def get_logger():
    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(format=format)
    # set channel to stdout
    _loggr = logging.getLogger("CARD_CACHE_SERVICE")
    _loggr.setLevel(logging.INFO)
    return _loggr


def gracefull_file_not_found(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except FileNotFoundError:
            return None

    return wrapper


def _get_task(pathspec):
    try:
        namespace(None)
        return Task(pathspec)
    except MetaflowNotFound as e:
        return None


def _card_dir_path(cache_path, cache_dir, task_pathspec, card_hash):
    return os.path.join(_task_dir_path(cache_path, cache_dir, task_pathspec), str(card_hash))


def _task_dir_path(cache_path, cache_dir, task_pathspec):
    return os.path.join(cache_path, cache_dir, _make_hash(task_pathspec))


def _make_hash(_str):
    return hashlib.md5(_str.encode()).hexdigest()


def cleanup_non_running_caches(cache_path, cache_dir, pathspecs):
    task_dirs = os.listdir(os.path.join(cache_path, cache_dir))
    task_dir_names = [_make_hash(p) for p in pathspecs]
    for task_dir in task_dirs:
        if task_dir in task_dir_names:
            continue
        shutil.rmtree(os.path.join(cache_path, cache_dir, task_dir), ignore_errors=True)


class CardCache:
    CACHE_DIR = "card_cache_service"

    _DATA_FILE = "data.json"

    _HTML_FILE = "card.html"

    _METADATA_FILE = "metadata.json"

    LIST_METADATA = "available_cards.json"

    def __init__(self, pathspec, card_hash, cache_path="./", init=False):
        self.pathspec = pathspec
        self.card_hash = card_hash
        self.cache_path = cache_path
        self.card_id = None
        self.card_type = None
        if init:
            self._init_cache()

    def _init_cache(self):
        os.makedirs(self.base_dir, exist_ok=True)

    def read_ready(self):
        return all([self.card_type, self.card_hash, self.pathspec, self.cache_path])

    def refresh(self):
        metadata = self.read_metadata()
        if metadata is not None:
            self.card_id, self.card_type = metadata

    @classmethod
    def load_from_disk(cls, pathspec, card_hash, cache_path="./",):
        cache = cls(pathspec, card_hash, cache_path=cache_path)
        metadata = cache.read_metadata()
        if metadata is not None:
            cache.card_id, cache.card_type = metadata
        return cache

    @property
    def parent_dir(self):
        return _task_dir_path(self.cache_path, self.CACHE_DIR, self.pathspec)

    @property
    def base_dir(self):
        return _card_dir_path(
            self.cache_path, self.CACHE_DIR, self.pathspec, self.card_hash
        )

    def _write_data(self, data):
        with open(os.path.join(self.base_dir, self._DATA_FILE), "w") as f:
            save_data = {
                "data": data,
                "timestamp": time.time(),
            }
            json.dump(save_data, f)

    def _write_html(self, html):
        with open(os.path.join(self.base_dir, self._HTML_FILE), "w") as f:
            f.write(html)

    def _set_card_metadata(self, card_id, card_type):
        self.card_id = card_id
        self.card_type = card_type
        with open(os.path.join(self.base_dir, self._METADATA_FILE), "w") as f:
            json.dump({"card_id": self.card_id, "card_type": self.card_type}, f)

    @gracefull_file_not_found
    def read_metadata(self):
        try:
            with open(os.path.join(self.base_dir, self._METADATA_FILE), "r") as f:
                metadata = json.load(f)
                return metadata["card_id"], metadata["card_type"]
        except json.JSONDecodeError:
            return None

    @gracefull_file_not_found
    def read_data(self):
        try:
            with open(os.path.join(self.base_dir, self._DATA_FILE), "r") as f:
                return json.load(f)
        except json.JSONDecodeError:
            return None

    @gracefull_file_not_found
    def read_html(self):
        with open(os.path.join(self.base_dir, self._HTML_FILE), "r") as f:
            return f.read()

    @gracefull_file_not_found
    def read_card_list(self):
        with open(os.path.join(self.parent_dir, self.LIST_METADATA), "r") as f:
            return json.load(f)

    def cleanup(self):
        shutil.rmtree(self.base_dir, ignore_errors=True)


def _eligible_for_refresh(update_timings, update_frequency):
    if update_timings is None:
        return True
    if time.time() - update_timings >= update_frequency:
        return True
    return False


def _update_card_cache(cache: CardCache, update_type: str, card: Card):
    if update_type == "data":
        data = card.get_data()
        cache._write_data(data)
    elif update_type == "html":
        card._html = None
        html = card.get()
        cache._write_html(html)
    else:
        raise Exception(f"Invalid update type {update_type}")
    # def update_cache(self, card_hash, update_type):


class PeriodicLogger:
    def __init__(self, logger, n_seconds=5, log_level=logging.INFO):
        self.logger = logger
        self.n_seconds = n_seconds
        self.log_level = log_level
        self.start_time = time.time()

    def log(self, message):
        if time.time() - self.start_time > self.n_seconds:
            self.logger.log(self.log_level, message)
            self.start_time = time.time()
            return True
        return False


class TaskCardCacheService:

    LIST_FREQUENCY_SECONDS = 5

    DATA_UPDATE_FREQUENCY = 0.2

    HTML_UPDATE_FREQUENCY = 2

    def __init__(
        self,
        task_pathspec,
        cache_path="./",
        uptime_seconds=600,
        list_frequency_seconds=5,
        data_update_frequency=0.2,
        html_update_frequency=2,
        max_no_card_wait_time=10
    ) -> None:
        self._task_pathspec = task_pathspec
        self._cache: Dict[str, CardCache] = {
            # card_hash: CardCache
        }
        self._cards: Dict[str, Card] = {
            # card_hash: Card
        }
        self._task = _get_task(task_pathspec)
        self._cache_path = cache_path
        self.logger = get_logger()
        self._uptime_seconds = uptime_seconds
        if self._task is None:
            raise MetaflowNotFound(f"Task with pathspec {task_pathspec} not found")

        self.LIST_FREQUENCY_SECONDS = list_frequency_seconds
        self.DATA_UPDATE_FREQUENCY = data_update_frequency
        self.HTML_UPDATE_FREQUENCY = html_update_frequency
        self._max_no_card_wait_time = max_no_card_wait_time

    @property
    def base_dir(self):
        return _task_dir_path(
            self._cache_path, CardCache.CACHE_DIR, self._task_pathspec
        )

    def load_all_cards(self):
        """
        Load all cards for the task and populates them inside a dictionary in memory.

        It returns a tuple containing :
        - status: bool: True if cards were found, False otherwise
        - unresolvable: bool: True if cards were completely unresolvable.
            This means that cards cannot be found because of some corruption
            in data/issues reaching to datastore/issues extracting metadata etc.
        """
        resolved_cards = self._get_cards_safely()
        _cards = resolved_cards.cards
        for card in _cards:
            if card.hash in self._cache:
                continue
            self._cache[card.hash] = CardCache(
                self._task_pathspec, card.hash, cache_path=self._cache_path, init=True
            )
            self._cache[card.hash]._set_card_metadata(card.id, card.type)
            self._cards[card.hash] = card

        status = False
        if len(_cards) > 0:
            status = True

        return status, resolved_cards.unresolvable

    def update_card_cache(self, card_hash, update_type):
        if card_hash not in self._cache:
            raise Exception(
                f"Card with hash {card_hash} not found for task {self._task_pathspec}"
            )
        cache = self._cache[card_hash]
        card = self._cards[card_hash]
        _update_card_cache(cache, update_type, card)

    def write_available_cards(self):
        _cardinfo = {}
        for chash in self._cards:
            _card = self._cards[chash]
            _cardinfo[chash] = {
                "id": _card.id,
                "type": _card.type,
            }
        with open(os.path.join(self.base_dir, CardCache.LIST_METADATA), "w") as f:
            json.dump(_cardinfo, f)

    def _get_cards_safely(self) -> ResolvedCards:
        try:
            _cards = get_cards(self._task, follow_resumed=False)
            if len(_cards) == 0:
                return ResolvedCards([], False,)
            return ResolvedCards([c for c in _cards], False,)
        except CardNotPresentException as e:
            self.logger.debug(f"Cards were not found for pathspec {self._task_pathspec}")
            # This means that the card is not present but can be polled for some time.
            return ResolvedCards([], False,)
        except AttributeError as e:
            # This means that accessing attributes of task is not possible
            self.logger.debug(f"Error while accessing task attributes for pathspec {self._task_pathspec} {e}")
            return ResolvedCards([], True,)
        except UnresolvableDatastoreException as e:
            self.logger.debug(f"Error while resolving datastore for card: {e}")
            return ResolvedCards([], True,)
        except Exception as e:
            self.logger.error(f"Unknown Error while extracting cards: {e}")
            return ResolvedCards([], True,)  # On other errors fail away too!

    def refresh_loop(self):
        timings = {"data": None, "html": None, "list": None}
        start_time = time.time()
        self.logger.info("Starting cache refresh loop for %s" % self._task_pathspec)
        cards_are_unresolvable = False
        _sleep_time = 0.25
        while True:
            if time.time() - start_time > self._uptime_seconds:  # exit condition
                break
            if _eligible_for_refresh(timings["list"], self.LIST_FREQUENCY_SECONDS):
                list_status, cards_are_unresolvable = self.load_all_cards()
                if list_status:
                    self.write_available_cards()

            cache_is_empty = len(self._cache) == 0
            crossed_no_card_wait_time = time.time() - start_time > self._max_no_card_wait_time
            if cache_is_empty and not crossed_no_card_wait_time:
                time.sleep(_sleep_time)
                continue
            elif cache_is_empty and crossed_no_card_wait_time:
                self.logger.info(f"Cache is empty for {self._task_pathspec} and no cards were found for {self._max_no_card_wait_time} seconds")
                break
            elif cache_is_empty and cards_are_unresolvable:
                self.logger.error(f"Cache is empty for {self._task_pathspec} and no cards were unresolvable")
                break

            for card_hash in self._cache:
                if _eligible_for_refresh(timings["html"], self.HTML_UPDATE_FREQUENCY):
                    self.update_card_cache(card_hash, "html")

                if _eligible_for_refresh(timings["data"], self.DATA_UPDATE_FREQUENCY):
                    self.update_card_cache(card_hash, "data")

            time.sleep(_sleep_time)


@click.group()
def cli():
    pass


@cli.command()
@click.argument("pathspec")
@click.option("--cache-path", default="./", help="Path to the cache")
@click.option("--uptime-seconds", default=600, help="Timeout for the cache service")
@click.option("--list-frequency", default=5, help="Frequency for the listing cards to populate the cache")
@click.option("--data-update-frequency", default=0.2, help="Frequency for the data update")
@click.option("--html-update-frequency", default=2, help="Frequency for the html update")
@click.option("--max-no-card-wait-time", default=10, help="Maximum time to wait a card to be present")
def task_updates(
    pathspec,
    cache_path,
    uptime_seconds,
    list_frequency,
    data_update_frequency,
    html_update_frequency,
    max_no_card_wait_time,
):
    cache_service = TaskCardCacheService(
        pathspec,
        cache_path=cache_path,
        uptime_seconds=uptime_seconds,
        list_frequency_seconds=list_frequency,
        data_update_frequency=data_update_frequency,
        html_update_frequency=html_update_frequency,
        max_no_card_wait_time=max_no_card_wait_time,
    )
    cache_service.refresh_loop()


if __name__ == "__main__":
    cli()
