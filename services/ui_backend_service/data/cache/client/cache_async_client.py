from json.decoder import JSONDecodeError
import time
import asyncio
import json
from asyncio.subprocess import PIPE, STDOUT

from .cache_client import CacheClient, CacheServerUnreachable, CacheClientTimeout

from services.utils import logging

OP_WORKER_CREATE = 'worker_create'
OP_WORKER_TERMINATE = 'worker_terminate'

WAIT_FREQUENCY = 0.2
HEARTBEAT_FREQUENCY = 1


class CacheAsyncClient(CacheClient):
    _drain_lock = asyncio.Lock()
    _restart_requested = False

    async def start_server(self, cmdline, env):
        self.logger = logging.getLogger("CacheAsyncClient:{root}".format(root=self._root))

        self._proc = await asyncio.create_subprocess_exec(*cmdline,
                                                          env=env,
                                                          stdin=PIPE,
                                                          stdout=PIPE,
                                                          stderr=STDOUT,
                                                          limit=1024000)  # 1024KB

        asyncio.gather(
            self._heartbeat(),
            self.read_stdout()
        )

    async def _read_pipe(self, src):
        while self._is_alive:
            line = await src.readline()
            if not line:
                await asyncio.sleep(WAIT_FREQUENCY)
                break
            yield line.rstrip().decode("utf-8")

    async def read_stdout(self):
        async for line in self._read_pipe(self._proc.stdout):
            await self.read_message(line)

    async def read_message(self, line: str):
        try:
            message = json.loads(line)
            self.logger.info(message)
            if message['op'] == OP_WORKER_CREATE:
                self.pending_requests.add(message['stream_key'])
            elif message['op'] == OP_WORKER_TERMINATE:
                self.pending_requests.remove(message['stream_key'])

            self.logger.info("Pending stream keys: {}".format(
                list(self.pending_requests)))
        except JSONDecodeError as ex:
            self.logger.info("Message: {}".format(line))
        except Exception as ex:
            self.logger.exception(ex)

    async def check(self):
        ret = await self.Check()  # pylint: disable=no-member
        await ret.wait()
        ret.get()

    async def stop_server(self):
        if self._is_alive:
            self._is_alive = False
            self._proc.terminate()
            await self._proc.wait()

    async def send_request(self, blob):
        try:
            self._proc.stdin.write(blob)
            async with self._drain_lock:
                await asyncio.wait_for(
                    self._proc.stdin.drain(),
                    timeout=WAIT_FREQUENCY)
        except asyncio.TimeoutError:
            self.logger.warn("StreamWriter.drain timeout, request restart: {}".format(repr(self._proc.stdin)))
            # Drain timeout error indicates unrecoverable critical issue,
            # essentially the cache functionality remains broken after the first asyncio.TimeoutError.
            # Request restart from CacheStore so that normal operation can be resumed.
            self._restart_requested = True
        except ConnectionResetError:
            self._is_alive = False
            raise CacheServerUnreachable()

    async def wait_iter(self, it, timeout):
        end = time.time() + timeout
        for obj in it:
            if obj is None:
                await asyncio.sleep(WAIT_FREQUENCY)
                if not self._is_alive:
                    raise CacheServerUnreachable()
                elif time.time() > end:
                    raise CacheClientTimeout()
            else:
                yield obj

    async def wait(self, fun, timeout):
        def _repeat():
            while True:
                yield fun()

        async for obj in self.wait_iter(_repeat(), timeout):
            return obj

    async def request_and_return(self, reqs, ret):
        for req in reqs:
            await req
        return ret

    async def _heartbeat(self):
        while self._is_alive:
            try:
                await self.ping()
            except CacheServerUnreachable:
                self._is_alive = False
            await asyncio.sleep(HEARTBEAT_FREQUENCY)
