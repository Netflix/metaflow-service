from .base_row import BaseRow
import time
from services.data.db_utils import get_exposed_run_id


class RunRow(BaseRow):
    flow_id: str = None
    run_number: int = None
    run_id: str = None
    run: str = None
    user_name: str = None
    user: str = None
    status: str = None
    ts_epoch: int = 0
    finished_at: int = None
    duration: int = None

    def __init__(
        self,
        flow_id,
        user_name,
        user=None,
        run_number=None,
        run_id=None,
        run=None,
        status=None,
        ts_epoch=None,
        finished_at=None,
        duration=None,
        tags=None,
        system_tags=None,
        last_heartbeat_ts=None,
        **kwargs
    ):
        self.flow_id = flow_id
        self.user_name = user_name
        self.user = user
        self.run_number = run_number
        self.run_id = run_id
        self.run = run
        self.status = status
        self.tags = tags
        self.system_tags = system_tags
        if ts_epoch is None:
            ts_epoch = int(round(time.time() * 1000))

        self.ts_epoch = ts_epoch
        self.last_heartbeat_ts = last_heartbeat_ts
        self.finished_at = finished_at
        self.duration = duration
        self.last_heartbeat_ts = last_heartbeat_ts

    def serialize(self, expanded: bool = False):
        if expanded:
            return {
                "flow_id": self.flow_id,
                "run_number": self.run_number,
                "run_id": self.run_id,
                "user_name": self.user_name,
                "user": self.user,
                "run": self.run,
                "status": self.status,
                "ts_epoch": self.ts_epoch,
                "finished_at": self.finished_at,
                "duration": self.duration,
                "last_heartbeat_ts": self.last_heartbeat_ts,
                "tags": self.tags,
                "system_tags": self.system_tags
            }
        else:
            return {
                "flow_id": self.flow_id,
                "run_number": str(get_exposed_run_id(self.run_number, self.run_id)),
                "user_name": self.user_name,
                "status": self.status,
                "ts_epoch": self.ts_epoch,
                "finished_at": self.finished_at,
                "duration": self.duration,
                "last_heartbeat_ts": self.last_heartbeat_ts,
                "tags": self.tags,
                "system_tags": self.system_tags
            }
