
import pytest
from services.metadata_service.api.task import _has_heartbeat_capable_version_tag


expectations = [
    ([], False),
    (["2.2.12"], False),
    (["metaflow_version:0.5"], False),
    (["metaflow_version:1.22.1"], True),
    (["metaflow_version:1.2.1"], True),
    (["metaflow_version:2.0.0"], False),
    (["metaflow_version:2.0.5"], False),
    (["metaflow_version:2.2.11"], False),
    (["metaflow_version:2.2.12"], True),
    (["metaflow_version:2.2.12+ab1234"], True),
    (["metaflow_version:2.3"], True),
    (["metaflow_version:2.3.1"], True),
    (["metaflow_version:2.4.1"], True),
]


@pytest.mark.parametrize("system_tags, expected_boolean", expectations)
async def test_has_heartbeat_capable_version_tag(system_tags, expected_boolean):
  _result_bool = _has_heartbeat_capable_version_tag(system_tags)

  assert expected_boolean == _result_bool
