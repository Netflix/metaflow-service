import pytest

from services.ui_backend_service.data.refiner.task_refiner import TaskRefiner

pytestmark = [pytest.mark.unit_tests]


def _refiner():
    # Refinery.__init__ only stores the cache; refine_record's foreach path never touches it,
    # so a None cache is sufficient for exercising the unpack logic in isolation.
    return TaskRefiner(cache=None)


def _record():
    return {
        'flow_id': 'F', 'run_number': 1, 'run_id': '1',
        'step_name': 'train', 'task_id': '42', 'task_name': '42',
        'attempt_id': 0, 'status': 'completed',
    }


# Regression for the staging crash-loop (2026-06-04): Metaflow's ForeachFrame grew a 5th
# `value` field (metaflow.tuple_util: step, var, num_splits, index, value). The refiner used to
# unpack `value[0]` into exactly 4 vars, raising "too many values to unpack (expected 4)" for
# every foreach task written by a recent client — which crash-looped the async refiner and
# exhausted the downstream connection pool, degrading the whole UI.
async def test_refine_record_handles_5_element_foreach_frame():
    refiner = _refiner()
    record = _record()
    # 5-element frame: (step, var, num_splits, index, value)
    values = {'_foreach_stack': [('train', 'x', 10, 7, 'the-value')]}

    out = await refiner.refine_record(record, values)

    assert out['foreach_label'] == '42[7]'  # index is positional element [3]


async def test_refine_record_handles_legacy_4_element_foreach_frame():
    refiner = _refiner()
    record = _record()
    # Legacy 4-element frame: (step, var, num_splits, index)
    values = {'_foreach_stack': [('train', 'x', 10, 3)]}

    out = await refiner.refine_record(record, values)

    assert out['foreach_label'] == '42[3]'


async def test_refine_record_tolerates_6_plus_element_frame():
    refiner = _refiner()
    record = _record()
    # Future-proofing: any frame with >= 4 fields must not raise; index stays at [3].
    values = {'_foreach_stack': [('train', 'x', 10, 2, 'v', 'extra')]}

    out = await refiner.refine_record(record, values)

    assert out['foreach_label'] == '42[2]'


async def test_refine_record_skips_short_foreach_frame():
    refiner = _refiner()
    record = _record()
    # A malformed/short frame (< 4 fields) is skipped, not crashed on.
    values = {'_foreach_stack': [('train', 'x', 10)]}

    out = await refiner.refine_record(record, values)

    assert 'foreach_label' not in out


async def test_refine_record_no_foreach_stack_is_noop():
    refiner = _refiner()
    record = _record()

    out = await refiner.refine_record(record, {})

    assert 'foreach_label' not in out
