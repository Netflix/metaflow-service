from .base_row import BaseRow
from services.data.db_utils import get_exposed_run_id, get_exposed_task_id
import time


class MetadataRow(BaseRow):
    flow_id: str = None
    run_number: int = None
    run_id: str = None
    step_name: str = None
    task_id: int = None
    task_name: str = None
    id: int = None  # autoincrement
    field_name: str = None
    value: dict = None
    type: str = None
    user_name: str = None
    ts_epoch: int = 0
    tags = None
    system_tags = None

    def __init__(
        self,
        flow_id,
        run_number,
        run_id,
        step_name,
        task_id,
        task_name,
        id,
        field_name,
        value,
        type,
        user_name,
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
        self.field_name = field_name
        self.value = value
        self.type = type
        self.user_name = user_name
        if ts_epoch is None:
            ts_epoch = int(round(time.time() * 1000))

        self.ts_epoch = ts_epoch
        self.id = id
        self.tags = tags
        self.system_tags = system_tags

    def serialize(self, expanded: bool = False):
        if expanded:
            return {
                "id": self.id,
                "flow_id": self.flow_id,
                "run_number": self.run_number,
                "run_id": self.run_id,
                "step_name": self.step_name,
                "task_id": self.task_id,
                "task_name": self.task_name,
                "field_name": self.field_name,
                "value": self.value,
                "type": self.type,
                "user_name": self.user_name,
                "ts_epoch": self.ts_epoch,
                "tags": self.tags,
                "system_tags": self.system_tags,
            }
        else:
            return {
                "id": self.id,
                "flow_id": self.flow_id,
                "run_number": str(get_exposed_run_id(self.run_number, self.run_id)),
                "step_name": self.step_name,
                "task_id": str(get_exposed_task_id(self.task_id, self.task_name)),
                "field_name": self.field_name,
                "value": self.value,
                "type": self.type,
                "user_name": self.user_name,
                "ts_epoch": self.ts_epoch,
                "tags": self.tags,
                "system_tags": self.system_tags,
            }
