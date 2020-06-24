import time


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

    def serialize(self):
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
    user_name: str = None
    ts_epoch: int = 0

    def __init__(
        self,
        flow_id,
        user_name,
        run_number=None,
        ts_epoch=None,
        tags=None,
        system_tags=None,
    ):
        self.flow_id = flow_id
        self.user_name = user_name
        self.run_number = run_number
        self.tags = tags
        self.system_tags = system_tags
        if ts_epoch is None:
            ts_epoch = int(round(time.time() * 1000))

        self.ts_epoch = ts_epoch

    def serialize(self):
        return {
            "flow_id": self.flow_id,
            "run_number": self.run_number,
            "user_name": self.user_name,
            "ts_epoch": self.ts_epoch,
            "tags": self.tags,
            "system_tags": self.system_tags,
        }


class StepRow(object):
    flow_id: str = None
    run_number: int = None
    step_name: str = None
    user_name: str = None
    ts_epoch: int = 0
    tags = None
    system_tags = None

    def __init__(
        self,
        flow_id,
        run_number,
        user_name,
        step_name,
        ts_epoch=None,
        tags=None,
        system_tags=None,
    ):
        self.flow_id = flow_id
        self.run_number = run_number
        self.step_name = step_name
        self.user_name = user_name
        if ts_epoch is None:
            ts_epoch = int(round(time.time() * 1000))

        self.ts_epoch = ts_epoch
        self.tags = tags
        self.system_tags = system_tags

    def serialize(self):
        return {
            "flow_id": self.flow_id,
            "run_number": self.run_number,
            "step_name": self.step_name,
            "user_name": self.user_name,
            "ts_epoch": self.ts_epoch,
            "tags": self.tags,
            "system_tags": self.system_tags,
        }


class TaskRow(object):
    flow_id: str = None
    run_number: int = None
    step_name: str = None
    task_id: int = None
    user_name: str = None
    ts_epoch: int = 0
    tags = None
    system_tags = None

    def __init__(
        self,
        flow_id,
        run_number,
        user_name,
        step_name,
        task_id=None,
        ts_epoch=None,
        tags=None,
        system_tags=None,
    ):
        self.flow_id = flow_id
        self.run_number = run_number
        self.step_name = step_name
        self.task_id = task_id
        self.user_name = user_name
        if ts_epoch is None:
            ts_epoch = int(round(time.time() * 1000))

        self.ts_epoch = ts_epoch
        self.tags = tags
        self.system_tags = system_tags

    def serialize(self):
        return {
            "flow_id": self.flow_id,
            "run_number": self.run_number,
            "step_name": self.step_name,
            "task_id": self.task_id,
            "user_name": self.user_name,
            "ts_epoch": self.ts_epoch,
            "tags": self.tags,
            "system_tags": self.system_tags,
        }


class MetadataRow(object):
    flow_id: str = None
    run_number: int = None
    step_name: str = None
    task_id: int = None
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
        step_name,
        task_id,
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
        self.step_name = step_name
        self.task_id = task_id
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

    def serialize(self):
        return {
            "id": self.id,
            "flow_id": self.flow_id,
            "run_number": self.run_number,
            "step_name": self.step_name,
            "task_id": self.task_id,
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
    step_name: str = None
    task_id: int = None
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
        step_name,
        task_id,
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
        self.step_name = step_name
        self.task_id = task_id
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

    def serialize(self):
        return {
            "flow_id": self.flow_id,
            "run_number": self.run_number,
            "step_name": self.step_name,
            "task_id": self.task_id,
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
