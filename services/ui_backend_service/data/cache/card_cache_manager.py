import time
import os
import sys
from services.utils import logging
import uuid
from asyncio.subprocess import Process
import asyncio
from .card_cache_service import CardCache, safe_wipe_dir
import contextlib


_PARENT_DIR = os.path.dirname(__file__)
PATH_TO_CACHE_SERVICE = os.path.join(_PARENT_DIR, "card_cache_service.py")

DATA_UPDATE_POLLING_FREQUENCY = os.environ.get(
    "CARD_CACHE_DATA_UPDATE_POLLING_FREQUENCY", 0.3
)
CARD_UPDATE_POLLING_FREQUENCY = os.environ.get(
    "CARD_CACHE_CARD_UPDATE_POLLING_FREQUENCY", 2
)
CARD_LIST_POLLING_FREQUENCY = os.environ.get(
    "CARD_CACHE_CARD_LIST_POLLING_FREQUENCY", 2
)
CARD_CACHE_PROCESS_MAX_UPTIME = os.environ.get(
    "CARD_CACHE_PROCESS_MAX_UPTIME", 3 * 60  # 3 minutes
)
CARD_CACHE_PROCESS_NO_CARD_WAIT_TIME = os.environ.get(
    "CARD_CACHE_PROCESS_NO_CARD_WAIT_TIME", 20  # 20 seconds
)
DEFAULT_CACHE_STORAGE_PATH_ROOT = "/tmp"
CACHE_STORAGE_PATH_ROOT = os.environ.get(
    "CARD_CACHE_STORAGE_PATH_ROOT", DEFAULT_CACHE_STORAGE_PATH_ROOT
)
CACHE_SERVICE_LOG_STORAGE_ROOT = os.environ.get("CACHE_SERVICE_LOG_STORAGE_ROOT", None)

CARD_API_HTML_WAIT_TIME = float(os.environ.get("CARD_API_HTML_WAIT_TIME", 3))


async def _get_latest_return_code(process: Process):
    with contextlib.suppress(asyncio.TimeoutError):
        await asyncio.wait_for(process.wait(), 1e-6)
    return process.returncode


async def process_status(process: Process):
    return_code = await _get_latest_return_code(process)
    if return_code is None:
        return "running"
    if return_code == 0:
        return "completed"
    return "failed"


async def process_is_running(process: Process):
    status = await process_status(process)
    return status == "running"


class AsyncCardCacheProcessManager:

    processes = {
        # "<current-context>":{
        #  "processes": {
        #       "<procid>": {
        #           "proc": asyncio.subprocess.Process,
        #           "started": time.time()
        #       }
        #   },
        #  "write_directory": "<write_directory>"
        # }
    }

    def get_context_dict(self, context):
        _x = self.processes.get(context, None)
        if _x is None:
            return _x
        return _x.copy()

    @property
    def current_process_dict(self):
        return self.processes[self._current_context]["processes"]

    @property
    def current_write_directory(self):
        return self.processes[self._current_context]["write_directory"]

    def __init__(self, logger) -> None:
        self.context_lock = asyncio.Lock()
        self.logger = logger
        self._current_context = None
        self.update_context()

    async def set_new_context(self):
        async with self.context_lock:
            old_context = self._current_context
            self.update_context()
            return old_context

    async def remove_context(self, context):
        async with self.context_lock:
            if context in self.processes:
                del self.processes[context]

    def update_context(self):
        _ctx_dict, _ctx = self.create_context_dict()
        self.processes.update(_ctx_dict)
        self._current_context = _ctx

    def create_context_dict(self):
        _ctx = uuid.uuid4().hex[:8]
        return {
            _ctx: {
                "processes": {},
                "write_directory": os.path.join(CACHE_STORAGE_PATH_ROOT, _ctx),
            }
        }, _ctx

    def _register_process(self, procid, proc):
        self.current_process_dict[procid] = {
            "proc": proc,
            "started": time.time(),
        }
        asyncio.create_task(proc.wait())

    def _make_command(self, pathspec):
        return [
            str(i)
            for i in [
                sys.executable,
                PATH_TO_CACHE_SERVICE,
                "task-updates",
                pathspec,
                "--uptime-seconds",
                CARD_CACHE_PROCESS_MAX_UPTIME,
                "--list-frequency",
                CARD_LIST_POLLING_FREQUENCY,
                "--cache-path",
                self.current_write_directory,
                "--max-no-card-wait-time",
                CARD_CACHE_PROCESS_NO_CARD_WAIT_TIME,
            ]
        ]

    async def add(self, procid, pathspec, lock_timeout=0.5):
        # The lock helps to ensure that the processes only get added one at a time
        # This is important because the processes are added to a shared dictionary
        procid, status = procid, None
        _acquired_lock = False
        try:
            await asyncio.wait_for(self.context_lock.acquire(), timeout=lock_timeout)
            _acquired_lock = True
            running_proc = await self.is_running(procid)
            if running_proc:
                return procid, "running"
            proc, started_on = self.get(procid)
            if proc is not None:
                self.remove_current(procid, delete_item=True)

            cmd = self._make_command(pathspec)
            await self.spawn(procid, cmd)
            status = "started"
        except asyncio.TimeoutError:
            status = "add-timeout"
        except Exception as e:
            status = "add-exception"
        finally:
            if _acquired_lock and self.context_lock.locked():
                self.context_lock.release()
        return procid, status

    def get(self, procid):
        proc_dict = self.current_process_dict.get(procid, None)
        if proc_dict is not None:
            return proc_dict["proc"], proc_dict["started"]
        return None, None

    async def spawn(self, procid, cmd,):
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            shell=False,
        )
        self._register_process(procid, proc,)

    def remove_current(self, procid, delete_item=True):
        if procid not in self.current_process_dict:
            return
        if delete_item:
            del self.current_process_dict[procid]

    async def cleanup(self):
        old_context = await self.set_new_context()
        _ctx_dict = self.get_context_dict(old_context)
        if _ctx_dict is None:
            return []
        # Two things to remove (old Keys and old directories)
        wait_keys = []
        for pid in _ctx_dict["processes"]:
            status = await process_status(_ctx_dict["processes"][pid]["proc"])
            if status not in ["completed", "failed"]:
                wait_keys.append(pid)

        if len(wait_keys) > 0:
            # sleeping for MAX_PROCESS_TIME so that all the processes writing should have finished
            # AND any upstream request accessing the cache-dir should have been attenteded
            await asyncio.sleep(CARD_CACHE_PROCESS_MAX_UPTIME)

        _write_dir = _ctx_dict["write_directory"]
        safe_wipe_dir(_write_dir)  # TODO : Make this happen in a different thread.
        await self.remove_context(old_context)

    async def is_running(self, procid):
        if procid not in self.current_process_dict:
            return False
        return await process_is_running(self.current_process_dict[procid]["proc"])

    async def get_status(self, procid):
        if procid not in self.current_process_dict:
            return None
        return await process_status(self.current_process_dict[procid]["proc"])

    async def running_processes(self):
        return [
            procid
            for procid in self.current_process_dict
            if await self.is_running(procid)
        ]


class CardCacheManager:
    def __init__(self) -> None:
        self.logger = logging.getLogger("CardCacheManager")
        self._process_manager = AsyncCardCacheProcessManager(self.logger)
        self._manager_id = uuid.uuid4().hex
        self.logger.info("CardCacheManager initialized")

    def get_local_cache(self, pathspec, card_hash):
        cache = CardCache.load_from_disk(
            pathspec,
            card_hash,
            self._process_manager.current_write_directory,
        )
        return cache

    async def register(self, pathspec, lock_timeout=0.5):
        proc_id = pathspec
        is_running = await self._process_manager.is_running(proc_id)
        if is_running:
            return proc_id, "running"
        self.logger.info("Registering task [%s]" % (pathspec))
        _id, status = await self._process_manager.add(
            proc_id, pathspec, lock_timeout=lock_timeout
        )
        return _id, status

    async def get_status(self, pathspec):
        return await self._process_manager.get_status(pathspec)

    async def regular_cleanup_routine(self, interval=60 * 60 * 24 * 5):
        try:
            while True:
                await asyncio.sleep(interval)
                await self._process_manager.cleanup()
        except asyncio.CancelledError:
            self.logger.info("Process/Directory cleanup routine cancelled")


async def verify_process_has_crashed(cache_manager: CardCacheManager, pathspec):
    _status = await cache_manager.get_status(pathspec)
    if _status in ["failed", None]:
        return True
    return False



async def list_cards(cache_manager: CardCacheManager, pathspec, max_wait_time=3):
    await cache_manager.register(pathspec)
    _cache = cache_manager.get_local_cache(pathspec, None)
    start_time = time.time()
    while time.time() - start_time < max_wait_time:
        try:
            card_hashes = _cache.read_card_list()
            if card_hashes is not None:
                return card_hashes
        except Exception as e:
            logging.error(f"Error reading card list for {pathspec}: {e}")
        await asyncio.sleep(0.1)
    return None
