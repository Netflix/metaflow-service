import time
from .db_utils import get_exposed_run_id, get_exposed_task_id


class FlowRow(object):
    flow_id: str = None
    user_name: str = None
    ts_epoch: int = 0

    def __init__(self, flow_id, user_name, ts_epoch=None, tags=None, system_tags=None):
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


class RunRow(object):
    flow_id: str = None
    run_number: int = None
    run_id: str = None
    user_name: str = None
    ts_epoch: int = 0

    def __init__(
        self,
        flow_id,
        user_name,
        run_number=None,
        run_id=None,
        ts_epoch=None,
        tags=None,
        system_tags=None,
        last_heartbeat_ts=None,
    ):
        self.flow_id = flow_id
        self.user_name = user_name
        self.run_number = run_number
        self.run_id = run_id
        self.tags = tags
        self.system_tags = system_tags
        if ts_epoch is None:
            ts_epoch = int(round(time.time() * 1000))

        self.ts_epoch = ts_epoch
        self.last_heartbeat_ts = last_heartbeat_ts

    def serialize(self, expanded: bool = False):
        if expanded:
            return {
                "flow_id": self.flow_id,
                "run_number": self.run_number,
                "run_id": self.run_id,
                "user_name": self.user_name,
                "ts_epoch": self.ts_epoch,
                "tags": self.tags,
                "system_tags": self.system_tags,
                "last_heartbeat_ts": self.last_heartbeat_ts
            }
        else:
            return {
                "flow_id": self.flow_id,
                "run_number": get_exposed_run_id(self.run_number, self.run_id),
                "user_name": self.user_name,
                "ts_epoch": self.ts_epoch,
                "tags": self.tags,
                "system_tags": self.system_tags,
                "last_heartbeat_ts": self.last_heartbeat_ts
            }


class StepRow(object):
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
                "run_number": get_exposed_run_id(self.run_number, self.run_id),
                "step_name": self.step_name,
                "user_name": self.user_name,
                "ts_epoch": self.ts_epoch,
                "tags": self.tags,
                "system_tags": self.system_tags,
            }


class TaskRow(object):
    flow_id: str = None
    run_number: int = None
    run_id: str = None
    step_name: str = None
    task_id: int = None
    task_name: str = None
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
        task_id=None,
        task_name=None,
        ts_epoch=None,
        tags=None,
        system_tags=None,
        last_heartbeat_ts=None,
        metadata_field_name=None,  # Unused but required for serialization
        metadata_value=None,  # Unused but required for serialization
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

        self.ts_epoch = ts_epoch
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
                "ts_epoch": self.ts_epoch,
                "tags": self.tags,
                "system_tags": self.system_tags,
                "last_heartbeat_ts": self.last_heartbeat_ts
            }
        else:
            return {
                "flow_id": self.flow_id,
                "run_number": get_exposed_run_id(self.run_number, self.run_id),
                "step_name": self.step_name,
                "task_id": get_exposed_task_id(self.task_id, self.task_name),
                "user_name": self.user_name,
                "ts_epoch": self.ts_epoch,
                "tags": self.tags,
                "system_tags": self.system_tags,
                "last_heartbeat_ts": self.last_heartbeat_ts
            }


class MetadataRow(object):
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
        return {
            "id": self.id,
            "flow_id": self.flow_id,
            "run_number": get_exposed_run_id(self.run_number, self.run_id),
            "step_name": self.step_name,
            "task_id": get_exposed_task_id(self.task_id, self.task_name),
            "field_name": self.field_name,
            "value": self.value,
            "type": self.type,
            "user_name": self.user_name,
            "ts_epoch": self.ts_epoch,
            "tags": self.tags,
            "system_tags": self.system_tags,
        }


class ArtifactRow(object):
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
        return {
            "flow_id": self.flow_id,
            "run_number": get_exposed_run_id(self.run_number, self.run_id),
            "step_name": self.step_name,
            "task_id": get_exposed_task_id(self.task_id, self.task_name),
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
