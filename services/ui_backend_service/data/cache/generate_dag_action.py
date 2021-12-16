import hashlib
import json

from .client import CacheAction
from .utils import streamed_errors, DAGParsingFailed, DAGUnsupportedFlowLanguage

from .custom_flowgraph import FlowGraph

from metaflow import Run, Step, DataArtifact, namespace
from metaflow.exception import MetaflowNotFound
namespace(None)  # Always use global namespace by default


class GenerateDag(CacheAction):
    """
    Generates a DAG for a given Run.

    Parameters
    ----------
    flow_id : str
        The flow id that this codepackage belongs to.
        Required for finding the correct class inside the parser logic.
    run_number : str
        Run number to construct rest of the pathspec

    Returns
    --------
    List or None
        example:
        [
        boolean,
        {
            "step_name": {
            'type': string,
            'box_next': boolean,
            'box_ends': string,
            'next': list,
            'doc': string
            },
            ...
        }
        ]
        First field conveys whether dag generation was successful.
        Second field contains the actual DAG.
    """

    @classmethod
    def format_request(cls, flow_id, run_number, invalidate_cache=False):
        msg = {
            'flow_id': flow_id,
            'run_number': run_number
        }
        key_identifier = "{}/{}".format(flow_id, run_number)
        result_key = 'dag:result:%s' % hashlib.sha1((key_identifier).encode('utf-8')).hexdigest()
        stream_key = 'dag:stream:%s' % hashlib.sha1((key_identifier).encode('utf-8')).hexdigest()

        return msg,\
            [result_key],\
            stream_key,\
            [stream_key],\
            invalidate_cache

    @classmethod
    def response(cls, keys_objs):
        '''
        Returns the generated DAG result
        '''
        return [json.loads(val) for key, val in keys_objs.items() if key.startswith('dag:result')][0]

    @classmethod
    def stream_response(cls, it):
        for msg in it:
            yield msg

    @classmethod
    def execute(cls,
                message=None,
                keys=None,
                existing_keys={},
                stream_output=None,
                invalidate_cache=False,
                **kwargs):
        results = {}
        flow_id = message['flow_id']
        run_number = message['run_number']

        result_key = [key for key in keys if key.startswith('dag:result')][0]

        with streamed_errors(stream_output):
            # TODO: Clean up the internal _graph_info accessing.
            # Having to jump through a lot of hoops currently due to internal artifacts
            # not being accessible directly.
            run = Run("{}/{}".format(flow_id, run_number))
            param_step = Step("{}/_parameters".format(run.pathspec))
            try:
                dag = DataArtifact("{}/_graph_info".format(param_step.task.pathspec)).data
            except MetaflowNotFound:
                dag = generate_dag(flow_id, run)

            results[result_key] = json.dumps(dag)

        return results

# Utilities


def generate_dag(flow_id, run: Run):
    try:
        source = run.code.flowspec
        # Initialize a FlowGraph object
        graph = FlowGraph(source=source, name=flow_id)
        # Build the DAG based on the DAGNodes given by the FlowGraph for the found FlowSpec class.
        steps_info, steps_structure = graph.output_steps()
        graph_info = {
            "steps_info": steps_info,
            "steps_structure": steps_structure,
            "doc": graph.doc
        }

        return graph_info
    except Exception as ex:
        if ex.__class__.__name__ == 'KeyError' and "python" in str(ex):
            raise DAGUnsupportedFlowLanguage(
                'DAG parsing is not supported for the language used in this Flow.'
            ) from None
        else:
            raise DAGParsingFailed(f"DAG Parsing failed: {str(ex)}")
