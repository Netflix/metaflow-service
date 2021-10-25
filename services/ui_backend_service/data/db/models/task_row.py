from .base_row import BaseRow
import time
from services.data.db_utils import get_exposed_task_id, get_exposed_run_id


class TaskRow(BaseRow):
    flow_id: str = None
    run_number: int = None
    run_id: str = None
    step_name: str = None
    task_id: int = None
    task_name: str = None
    user_name: str = None
    status: str = None
    ts_epoch: int = 0
    started_at: int = None
    finished_at: int = None
    duration: int = None
    attempt_id: int = 0
    tags = None
    system_tags = None

    def __init__(
        self,
        flow_id,
        run_number,
        run_id,
        user_name,
        step_name,
        task_id=None,
        task_name=None,
        status=None,
        ts_epoch=None,
        started_at=None,
        finished_at=None,
        duration=None,
        attempt_id=0,
        tags=None,
        system_tags=None,
        last_heartbeat_ts=None,
        **kwargs
    ):
        self.flow_id = flow_id
        self.run_number = run_number
        self.run_id = run_id
        self.step_name = step_name
        self.task_id = task_id
        self.task_name = task_name

        self.user_name = user_name
        if ts_epoch is None:
            ts_epoch = int(round(time.time() * 1000))

        self.status = status
        self.ts_epoch = ts_epoch
        self.started_at = started_at
        self.finished_at = finished_at
        self.duration = duration
        self.attempt_id = attempt_id
        self.tags = tags
        self.system_tags = system_tags
        self.last_heartbeat_ts = last_heartbeat_ts

    def serialize(self, expanded: bool = False):
        if expanded:
            return {
                "flow_id": self.flow_id,
                "run_number": self.run_number,
                "run_id": self.run_id,
                "step_name": self.step_name,
                "task_id": self.task_id,
                "task_name": self.task_name,
                "user_name": self.user_name,
                "status": self.status,
                "ts_epoch": self.ts_epoch,
                "started_at": self.started_at,
                "finished_at": self.finished_at,
                "duration": self.duration,
                "attempt_id": self.attempt_id,
                "tags": self.tags,
                "system_tags": self.system_tags,
                "last_heartbeat_ts": self.last_heartbeat_ts
            }
        else:
            return {
                "flow_id": self.flow_id,
                "run_number": str(get_exposed_run_id(self.run_number, self.run_id)),
                "step_name": self.step_name,
                "task_id": str(get_exposed_task_id(self.task_id, self.task_name)),
                "user_name": self.user_name,
                "status": self.status,
                "ts_epoch": self.ts_epoch,
                "started_at": self.started_at,
                "finished_at": self.finished_at,
                "duration": self.duration,
                "attempt_id": self.attempt_id,
                "tags": self.tags,
                "system_tags": self.system_tags,
                "last_heartbeat_ts": self.last_heartbeat_ts
            }
