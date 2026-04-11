from json.decoder import JSONDecodeError
import time
import asyncio
import json
from asyncio.subprocess import PIPE, STDOUT

from .cache_client import CacheClient, CacheServerUnreachable, CacheClientTimeout

from services.utils import logging

OP_WORKER_CREATE = "worker_create"
OP_WORKER_TERMINATE = "worker_terminate"

WAIT_FREQUENCY = 0.2
HEARTBEAT_FREQUENCY = 1


class CacheAsyncClient(CacheClient):

    async def start_server(self, cmdline, env):
        self._drain_lock = asyncio.Lock()
        self._restart_requested = False
        self.logger = logging.getLogger(
            "CacheAsyncClient:{root}".format(root=self._root)
        )

        self._proc = await asyncio.create_subprocess_exec(
            *cmdline, env=env, stdin=PIPE, stdout=PIPE, stderr=STDOUT, limit=1024000
        )  # 1024KB

        self._heartbeat_task = asyncio.ensure_future(self._heartbeat())
        self._read_task = asyncio.ensure_future(self.read_stdout())

    async def _read_pipe(self, src):
        while self._is_alive:
            line = await src.readline()
            if not line:
                self.logger.warning(
                    "Cache server stdout pipe closed (subprocess likely exited). "
                    "proc.returncode=%s, _is_alive=%s",
                    self._proc.returncode, self._is_alive
                )
                await asyncio.sleep(WAIT_FREQUENCY)
                break
            yield line.rstrip().decode("utf-8")

    async def read_stdout(self):
        async for line in self._read_pipe(self._proc.stdout):
            await self.read_message(line)
        # Pipe closed without stop_server being called — trigger restart (DEF-B06-D1)
        if self._is_alive:
            self.logger.error(
                "read_stdout: stdout pipe closed unexpectedly while _is_alive=True. "
                "Setting _is_alive=False and requesting restart. "
                "proc.returncode=%s, pid=%s",
                self._proc.returncode, self._proc.pid
            )
            self._is_alive = False
            self._restart_requested = True

    async def read_message(self, line: str):
        try:
            # We check for isEnabledFor because some things may be very long to print
            # (in particularly pending_requests)
            message = json.loads(line)
            if self.logger.isEnabledFor(logging.INFO):
                self.logger.info(message)
            if message["op"] == OP_WORKER_CREATE:
                self.pending_requests.add(message["stream_key"])
            elif message["op"] == OP_WORKER_TERMINATE:
                self.pending_requests.discard(message["stream_key"])

            if self.logger.isEnabledFor(logging.INFO):
                self.logger.info(
                    "Pending stream keys: {}".format(len(list(self.pending_requests)))
                )
        except JSONDecodeError as ex:
            if self.logger.isEnabledFor(logging.INFO):
                self.logger.info("Message: {}".format(line))
        except Exception as ex:
            self.logger.exception(ex)

    async def check(self):
        ret = await self.Check()  # pylint: disable=no-member
        await ret.wait()
        ret.get()

    async def stop_server(self):
        self._is_alive = False
        # Cancel background tasks regardless of how _is_alive was cleared (M3)
        if hasattr(self, '_heartbeat_task'):
            self._heartbeat_task.cancel()
            self._read_task.cancel()
            await asyncio.gather(self._heartbeat_task, self._read_task, return_exceptions=True)
        # Always reap the subprocess to avoid zombie processes (M2)
        if hasattr(self, '_proc'):
            if self._proc.returncode is None:
                self._proc.terminate()
            self.logger.info("Waiting for cache server to terminate")
            await self._proc.wait()

    async def send_request(self, blob):
        if not self._is_alive:  # DEF-B02-D2: fast-path avoids misleading ERROR logs during shutdown
            raise CacheServerUnreachable(reason="cache server not alive")
        try:
            async with self._drain_lock:
                self._proc.stdin.write(blob)
                await asyncio.wait_for(self._proc.stdin.drain(), timeout=WAIT_FREQUENCY)
        except asyncio.TimeoutError:
            self.logger.warning(
                "StreamWriter.drain timeout, request restart. "
                "stdin=%s, proc.returncode=%s, pending_requests=%d",
                repr(self._proc.stdin), self._proc.returncode,
                len(self.pending_requests)
            )
            self._is_alive = False  # drain timeout is unrecoverable (m1)
            self._restart_requested = True
        except ConnectionError as e:  # catches BrokenPipeError and ConnectionResetError (M1)
            self.logger.error(
                "ConnectionError in send_request: cache subprocess likely crashed. "
                "proc.returncode=%s, pid=%s, pending_requests=%d, error=%s",
                self._proc.returncode, self._proc.pid,
                len(self.pending_requests), e
            )
            self._is_alive = False
            self._restart_requested = True
            raise CacheServerUnreachable(
                reason="ConnectionError during send_request (pid={}, returncode={})".format(
                    self._proc.pid, self._proc.returncode)
            )

    async def wait_iter(self, it, timeout):
        start = time.time()
        end = start + timeout
        for obj in it:
            if obj is None:
                await asyncio.sleep(WAIT_FREQUENCY)
                if not self._is_alive:
                    elapsed = time.time() - start
                    self.logger.error(
                        "CacheServerUnreachable in wait_iter: _is_alive=False after %.2fs. "
                        "proc.returncode=%s, pid=%s, _restart_requested=%s, "
                        "pending_requests=%d",
                        elapsed, self._proc.returncode, self._proc.pid,
                        self._restart_requested, len(self.pending_requests)
                    )
                    raise CacheServerUnreachable(
                        reason="server not alive during wait_iter after {:.2f}s "
                               "(pid={}, returncode={}, restart_requested={})".format(
                                   elapsed, self._proc.pid,
                                   self._proc.returncode, self._restart_requested)
                    )
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
                self.logger.error(
                    "Heartbeat ping failed: setting _is_alive=False. "
                    "proc.returncode=%s, pid=%s, pending_requests=%d",
                    self._proc.returncode, self._proc.pid,
                    len(self.pending_requests)
                )
                self._is_alive = False
            await asyncio.sleep(HEARTBEAT_FREQUENCY)
