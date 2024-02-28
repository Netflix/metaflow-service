import time
from subprocess import Popen
import os
import sys
from services.utils import logging
import uuid
from asyncio.subprocess import Process
import asyncio
from .card_cache_service import CardCache, cleanup_non_running_caches
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
    "CARD_CACHE_PROCESS_NO_CARD_WAIT_TIME", 4  # 4 seconds
)
DEFAULT_CACHE_STORAGE_PATH = "/tmp"
CACHE_STORAGE_PATH = os.environ.get(
    "CARD_CACHE_STORAGE_PATH", DEFAULT_CACHE_STORAGE_PATH
)
CACHE_SERVICE_LOG_STORAGE_ROOT = os.environ.get("CACHE_SERVICE_LOG_STORAGE_ROOT", None)

CARD_API_HTML_WAIT_TIME = float(os.environ.get("CARD_API_HTML_WAIT_TIME", 5))


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


class AsyncProcessManager:

    processes = {
        # "procid": {
        #     "proc": asyncio.subprocess.Process,
        #     "started": time.time()
        # }
    }

    def __init__(self, logger) -> None:
        self.lock = asyncio.Lock()
        self.logger = logger

    def _register_process(self, procid, proc):
        self.processes[procid] = {
            "proc": proc,
            "started": time.time(),
        }

    async def add(self, procid, cmd, logs_file_path=CACHE_SERVICE_LOG_STORAGE_ROOT):
        running_proc = await self.is_running(procid)
        if running_proc:
            return procid, "running"
        # The lock helps to ensure that the processes only get added one at a time
        # This is important because the processes are added to a shared dictionary
        async with self.lock:
            proc, started_on = self.get(procid)
            if proc is not None:
                await self.remove(procid, delete_item=True)

            logs_file = None
            if logs_file_path is not None:
                logs_file = open(
                    os.path.join(
                        logs_file_path,
                        "card_cache_service_%s.log" % (procid),
                    ),
                    "w",
                )

            await self.spawn(procid, cmd, logs_file, logs_file)
            return procid, "started"

    def get(self, procid):
        proc_dict = self.processes.get(procid, None)
        if proc_dict is not None:
            return proc_dict["proc"], proc_dict["started"]
        return None, None

    async def spawn(self, procid, cmd, stdout_file, std_err_file=None):
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=stdout_file,
            stderr=std_err_file,
            shell=False,
        )
        self._register_process(procid, proc)

    async def remove(self, procid, delete_item=True):
        if procid not in self.processes:
            return
        self.logger.info("Removing process: %s" % procid)
        await self.processes[procid]["proc"].wait()
        self.logger.info("Process removed: %s" % procid)
        if self.processes[procid]["proc"].stdout is not None:
            self.processes[procid]["proc"].stdout.close()
        if delete_item:
            del self.processes[procid]

    async def cleanup(self):
        # The lock ensures that when the dictionary is being modified,
        # no other process can modify it at the same time.
        async with self.lock:
            removal_keys = []
            for procid in self.processes:
                running_proc = await self.is_running(procid)
                if running_proc:
                    continue
                removal_keys.append(procid)
                await self.remove(procid, delete_item=False)
            for procid in removal_keys:
                del self.processes[procid]
            return removal_keys

    async def is_running(self, procid):
        if procid not in self.processes:
            return False
        return await process_is_running(self.processes[procid]["proc"])

    async def get_status(self, procid):
        if procid not in self.processes:
            return None
        return await process_status(self.processes[procid]["proc"])

    async def running_processes(self):
        return [procid for procid in self.processes if await self.is_running(procid)]


class CardCacheManager:
    def __init__(self) -> None:
        self.logger = logging.getLogger("CardCacheManager")
        self._process_manager = AsyncProcessManager(self.logger)
        self._manager_id = uuid.uuid4().hex
        self.logger.info("CardCacheManager initialized")

    def _make_task_command(self, pathspec):
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
                "--data-update-frequency",
                DATA_UPDATE_POLLING_FREQUENCY,
                "--html-update-frequency",
                CARD_UPDATE_POLLING_FREQUENCY,
                "--cache-path",
                CACHE_STORAGE_PATH,
                "--max-no-card-wait-time",
                CARD_CACHE_PROCESS_NO_CARD_WAIT_TIME
            ]
        ]

    def get_local_cache(self, pathspec, card_hash):
        cache = CardCache.load_from_disk(
            pathspec, card_hash, CACHE_STORAGE_PATH,
        )
        return cache

    async def register(self, pathspec):
        proc_id = pathspec
        is_running = await self._process_manager.is_running(proc_id)
        if is_running:
            return proc_id, "running"

        cmd = self._make_task_command(pathspec)
        self.logger.info(
            "Registering task [%s]" % (pathspec)
        )
        _id, status = await self._process_manager.add(proc_id, cmd, logs_file_path=CACHE_SERVICE_LOG_STORAGE_ROOT)
        return _id, status

    async def get_status(self, pathspec):
        return await self._process_manager.get_status(pathspec)

    async def start_process_cleanup_routine(self, interval=60):
        try:
            while True:
                cleanup_keys = await self._process_manager.cleanup()  # Perform the cleanup
                if len(cleanup_keys) > 0:
                    self.logger.info(
                        "Cleaned up processes: %s" % ", ".join(cleanup_keys)
                    )
                await asyncio.sleep(interval)  # Wait for a specified interval before the next cleanup
        except asyncio.CancelledError:
            self.logger.info("Process cleanup routine cancelled")

    async def cleanup_disk_routine(self, interval=60 * 60 * 4):
        try:
            while True:
                await asyncio.sleep(interval)
                # The lock ensure that new processes are not getting created or
                # processes are not getting removed when the disk cleanup is happening.
                async with self._process_manager.lock:
                    running_proc_ids = await self._process_manager.running_processes()
                    cleanup_non_running_caches(CACHE_STORAGE_PATH, CardCache.CACHE_DIR, running_proc_ids)
        except asyncio.CancelledError:
            self.logger.info("Disk cleanup routine cancelled")


async def verify_process_has_crashed(cache_manager: CardCacheManager, pathspec):
    _status = await cache_manager.get_status(pathspec)
    if _status in ["failed", None]:
        return True
    return False


def _get_html_or_refresh(local_cache: CardCache):
    if local_cache.read_ready():
        _html = local_cache.read_html()
        if _html is not None:
            return {
                local_cache.card_hash: {
                    "html": _html,
                }
            }
    else:
        local_cache.refresh()
    return None


async def wait_until_card_is_ready(
    cache_manager: CardCacheManager, local_cache: CardCache, max_wait_time=3, frequency=0.1
):
    html = None
    start_time = time.time()
    await cache_manager.register(local_cache.pathspec)
    # At this point in the function the process should aleady be running
    while time.time() - start_time < max_wait_time:
        html = _get_html_or_refresh(local_cache)
        if html is not None:
            break
        process_failed = await verify_process_has_crashed(cache_manager, local_cache.pathspec)
        if process_failed:
            cache_manager.logger.error(
                f"Card {local_cache.card_hash} has crashed for {local_cache.pathspec}"
            )
            break
        await asyncio.sleep(frequency)
    return html  # We ONLY return None if the card is not found after max_wait_time; This is because cards may not be ready


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
