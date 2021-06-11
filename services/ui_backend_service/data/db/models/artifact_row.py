from services.data.db_utils import get_exposed_run_id, get_exposed_task_id
from .base_row import BaseRow
import time


class ArtifactRow(BaseRow):
    flow_id: str = None
    run_number: int = None
    run_id: str = None
    step_name: str = None
    task_id: int = None
    task_name: str = None
    name: str = None
    location: str = None
    sha: str = None
    type: str = None
    content_type: str = None
    user_name: str = None
    attempt_id: int = 0
    ts_epoch: int = 0

    def __init__(
        self,
        flow_id,
        run_number,
        run_id,
        step_name,
        task_id,
        task_name,
        name,
        location,
        ds_type,
        sha,
        type,
        content_type,
        user_name,
        attempt_id,
        ts_epoch=None,
        tags=None,
        system_tags=None,
        **kwargs
    ):
        self.flow_id = flow_id
        self.run_number = run_number
        self.run_id = run_id
        self.step_name = step_name
        self.task_id = task_id
        self.task_name = task_name
        self.name = name
        self.location = location
        self.ds_type = ds_type
        self.sha = sha
        self.type = type
        self.content_type = content_type
        self.user_name = user_name
        self.attempt_id = attempt_id
        if ts_epoch is None:
            ts_epoch = int(round(time.time() * 1000))

        self.ts_epoch = ts_epoch
        self.tags = tags
        self.system_tags = system_tags

    def serialize(self, expanded: bool = False):
        if expanded:
            return {
                "flow_id": self.flow_id,
                "run_number": self.run_number,
                "run_id": self.run_id,
                "step_name": self.step_name,
                "task_id": self.task_id,
                "task_name": self.task_name,
                "name": self.name,
                "location": self.location,
                "ds_type": self.ds_type,
                "sha": self.sha,
                "type": self.type,
                "content_type": self.content_type,
                "user_name": self.user_name,
                "attempt_id": self.attempt_id,
                "ts_epoch": self.ts_epoch,
                "tags": self.tags,
                "system_tags": self.system_tags,
            }
        else:
            return {
                "flow_id": self.flow_id,
                "run_number": str(get_exposed_run_id(self.run_number, self.run_id)),
                "step_name": self.step_name,
                "task_id": str(get_exposed_task_id(self.task_id, self.task_name)),
                "name": self.name,
                "location": self.location,
                "ds_type": self.ds_type,
                "sha": self.sha,
                "type": self.type,
                "content_type": self.content_type,
                "user_name": self.user_name,
                "attempt_id": self.attempt_id,
                "ts_epoch": self.ts_epoch,
                "tags": self.tags,
                "system_tags": self.system_tags,
            }
