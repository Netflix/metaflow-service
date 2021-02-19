from .base_row import BaseRow
import time
from services.data.db_utils import get_exposed_run_id


class StepRow(BaseRow):
    flow_id: str = None
    run_number: int = None
    run_id: str = None
    step_name: str = None
    user_name: str = None
    ts_epoch: int = 0
    tags = None
    system_tags = None

    def __init__(
        self,
        flow_id,
        run_number,
        run_id,
        user_name,
        step_name,
        ts_epoch=None,
        tags=None,
        system_tags=None,
        **kwargs
    ):
        self.flow_id = flow_id
        self.run_number = run_number

        if run_id is None:
            run_id = str(run_number)
        self.run_id = run_id

        self.step_name = step_name
        self.user_name = user_name
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
                "user_name": self.user_name,
                "ts_epoch": self.ts_epoch,
                "tags": self.tags,
                "system_tags": self.system_tags,
            }
        else:
            return {
                "flow_id": self.flow_id,
                "run_number": str(get_exposed_run_id(self.run_number, self.run_id)),
                "step_name": self.step_name,
                "user_name": self.user_name,
                "ts_epoch": self.ts_epoch,
                "tags": self.tags,
                "system_tags": self.system_tags,
            }
