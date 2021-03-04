from .base_row import BaseRow
import time


class FlowRow(BaseRow):
    flow_id: str = None
    user_name: str = None
    ts_epoch: int = 0

    def __init__(self, flow_id, user_name, ts_epoch=None, tags=None, system_tags=None, **kwargs):
        self.flow_id = flow_id
        self.user_name = user_name
        if ts_epoch is None:
            ts_epoch = int(round(time.time() * 1000))
        self.ts_epoch = ts_epoch
        self.tags = tags
        self.system_tags = system_tags

    def serialize(self, expanded: bool = False):
        return {
            "flow_id": self.flow_id,
            "user_name": self.user_name,
            "ts_epoch": self.ts_epoch,
            "tags": self.tags,
            "system_tags": self.system_tags,
        }
