from .refinery import Refinery


class ParameterRefiner(Refinery):
    """
    Refiner class for postprocessing Run parameters.

    Uses Metaflow Client API to refine Run parameters from Metaflow Datastore.

    Parameters
    -----------
    cache : AsyncCacheClient
        An instance of a cache that implements the GetArtifacts action.
    """

    def __init__(self, cache):
        super().__init__(cache=cache)

    def _action(self):
        return self.cache_store.cache.GetParameters

    def _record_to_action_input(self, record):
        return "{flow_id}/{run_number}".format(**record)

    async def refine_record(self, record, values):
        return {k: {'value': v} for k, v in values.items()}
