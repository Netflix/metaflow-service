import time
import asyncio
import json
import os
import fcntl
from datetime import datetime
from asyncio.subprocess import PIPE, STDOUT

from .cache_client import CacheClient, CacheServerUnreachable
from .cache_server import OP_WORKER_CREATE, OP_WORKER_TERMINATE

WAIT_FREQUENCY = 0.2
HEARTBEAT_FREQUENCY = 1

os.environ["PYTHONASYNCIODEBUG"] = "1"


def get_object_methods(obj):
    return [method_name for method_name in dir(obj)
            if callable(getattr(obj, method_name))]


class CacheAsyncClient(CacheClient):
    _drain_lock = asyncio.Lock()

    async def start_server(self, cmdline, env):
        self._proc = await asyncio.create_subprocess_exec(*cmdline,
                                                          env=env,
                                                          stdin=PIPE,
                                                          stdout=PIPE,
                                                          stderr=STDOUT)

        def _make_fd_non_blocking(fd):
            fl = fcntl.fcntl(fd, fcntl.F_GETFL)
            fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)

        self.stdin_pipe = self._proc.stdin.get_extra_info('pipe')
        self.stdin_fileno = self.stdin_pipe.fileno()

        print("stdin {}".format(get_object_methods(self._proc.stdin)), flush=True)
        print("stdin_pipe {}".format(get_object_methods(self.stdin_pipe)), flush=True)
        print("stdin_fileno {}".format(get_object_methods(self.stdin_fileno)), flush=True)

        _make_fd_non_blocking(self.stdin_fileno)

        self._proc.stdin.transport.set_write_buffer_limits(0)

        asyncio.gather(
            self._heartbeat(),
            self.read_stdout()
        )

    async def _read_pipe(self, src):
        while self._is_alive:

            # try:
            #     async with self._drain_lock:
            #         await asyncio.wait_for(
            #             self._proc.stdin.drain(),
            #             timeout=WAIT_FREQUENCY)
            # except asyncio.TimeoutError:
            #     print("StreamWriter.drain timeout (_read_pipe)", flush=True)

            line = await src.readline()

            now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')
            print("[{}] stdout ({}) - {}".format(now, self._root, repr(self._proc.stdout)), flush=True)

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
            if message['op'] == OP_WORKER_CREATE:
                self.pending_requests.add(message['stream_key'])
            elif message['op'] == OP_WORKER_TERMINATE:
                self.pending_requests.remove(message['stream_key'])
            else:
                return

            print("Message {}".format(message), flush=True)

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
            async with self._drain_lock:
                await asyncio.wait_for(
                    self._proc.stdin.drain(),
                    timeout=5)
        except asyncio.TimeoutError:
            print("StreamWriter.drain timeout", flush=True)
        except ConnectionResetError:
            self._is_alive = False
            raise CacheServerUnreachable()
        finally:
            now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')
            print("[{}] stdin ({}) - {}".format(now, self._root, repr(self._proc.stdin)), flush=True)

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
