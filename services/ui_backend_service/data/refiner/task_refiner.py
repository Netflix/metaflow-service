from .refinery import Refinery


class TaskRefiner(Refinery):
    """
    Refiner class for postprocessing Task rows.

    Uses Metaflow Client API to refine Task's actual status from Metaflow Service and Datastore.

    Parameters
    -----------
    cache : AsyncCacheClient
        An instance of a cache that implements the GetTask action.
    """

    def __init__(self, cache):
        super().__init__(cache=cache)

    def _action(self):
        return self.cache_store.cache.GetTask

    def _record_to_action_input(self, record):
        # Prefer run_id over run_number
        # Prefer task_name over task_id
        return "{flow_id}/{run_id}/{step_name}/{task_name}/{attempt_id}".format(
            flow_id=record['flow_id'],
            run_id=record.get('run_id') or record['run_number'],
            step_name=record['step_name'],
            task_name=record.get('task_name') or record['task_id'],
            attempt_id=record['attempt_id'])

    async def refine_record(self, record, values):
        if record['status'] == 'unknown' and values.get('_task_ok') is not None:
            value = values['_task_ok']
            if value is False:
                record['status'] = 'failed'
            elif value is True:
                record['status'] = 'completed'

        if values.get('_foreach_stack'):
            value = values['_foreach_stack']
            if len(value) > 0 and len(value[0]) >= 4:
                _, _name, _, _index = value[0]
                record['foreach_label'] = "{}[{}]".format(record['task_id'], _index)

        return record
