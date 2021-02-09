import time
from services.data.db_utils import get_exposed_run_id, get_exposed_task_id
from services.data.models import FlowRow, StepRow, TaskRow, ArtifactRow, MetadataRow
from services.data.models import RunRow


class __RunRow(RunRow):

    def __init__(
        self,
        flow_id,
        user_name,
        real_user=None,
        run_number=None,
        run_id=None,
        status=None,
        ts_epoch=None,
        finished_at=None,
        duration=None,
        tags=None,
        system_tags=None,
        last_heartbeat_ts=None,
        **kwargs
    ):
        super().__init__(
            flow_id,
            user_name,
            real_user=None,
            run_number=None,
            run_id=None,
            status=None,
            ts_epoch=None,
            finished_at=None,
            duration=None,
            tags=None,
            system_tags=None,
            last_heartbeat_ts=None,
            **kwargs
        )

    def serialize(self, expanded: bool = False):
        if expanded:
            return {
                "flow_id": self.flow_id,
                "run_number": self.run_number,
                "run_id": self.run_id,
                "user_name": self.user_name,
                "real_user": self.real_user,
                "status": self.status,
                "ts_epoch": self.ts_epoch,
                "finished_at": self.finished_at,
                "duration": self.duration,
                "last_heartbeat_ts": self.last_heartbeat_ts,
                "tags": self.tags,
                "system_tags": self.system_tags,
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
                "system_tags": self.system_tags,
            }
