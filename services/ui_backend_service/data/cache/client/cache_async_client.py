import time
import asyncio
import json
from subprocess import PIPE

from .cache_client import CacheClient, CacheServerUnreachable
from .cache_server import OP_WORKER_CREATE, OP_WORKER_TERMINATE

WAIT_FREQUENCY = 0.2
HEARTBEAT_FREQUENCY = 1


class CacheAsyncClient(CacheClient):

    async def start_server(self, cmdline, env):
        self._proc = await asyncio.create_subprocess_exec(*cmdline,
                                                          env=env,
                                                          stdin=PIPE,
                                                          stdout=PIPE,
                                                          stderr=PIPE)

        asyncio.gather(
            self._heartbeat(),
            self.read_stdout(),
            self.read_stderr()
        )

    async def _read_pipe(self, src):
        while True:
            line = await src.readline()
            if not line:
                await asyncio.sleep(WAIT_FREQUENCY)
                break
            yield line.rstrip().decode("utf-8")

    async def read_stdout(self):
        async for line in self._read_pipe(self._proc.stdout):
            await self.read_message(line)

    async def read_stderr(self):
        async for line in self._read_pipe(self._proc.stderr):
            await self.read_message(line)

    async def read_message(self, line: str):
        try:
            message = json.loads(line)
            if message['op'] == OP_WORKER_CREATE:
                self.pending_requests.add(message['stream_key'])
            elif message['op'] == OP_WORKER_TERMINATE:
                self.pending_requests.remove(message['stream_key'])
            else:
                return

            print("Pending stream keys: {}".format(
                list(self.pending_requests)), flush=True)
        except Exception:
            pass

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
            await self._proc.stdin.drain()
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
