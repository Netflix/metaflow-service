import hashlib
import json

from .client import CacheAction
from services.utils import get_traceback_str

from .custom_flowgraph import FlowGraph

from metaflow import Run


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

        def stream_error(err, id, traceback=None):
            return stream_output({"type": "error", "message": err, "id": id, "traceback": traceback})

        try:
            run = Run("{}/{}".format(flow_id, run_number))
            results[result_key] = json.dumps(generate_dag(flow_id, run.code.flowspec))
        except Exception as ex:
            if ex.__class__.__name__ == 'KeyError' and "filename 'python3' not found" in str(ex):
                stream_error(
                    'Parsing DAG graph is not supported for the language used in this Flow.',
                    'dag-unsupported-flow-language')
            else:
                stream_error(str(ex), ex.__class__.__name__, get_traceback_str())

        return results

# Utilities


def generate_dag(flow_id, source):
    # Initialize a FlowGraph object
    graph = FlowGraph(source=source, name=flow_id)
    # Build the DAG based on the DAGNodes given by the FlowGraph for the found FlowSpec class.
    dag = {}
    for node in graph:
        dag[node.name] = {
            'type': node.type,
            'box_next': node.type not in ('linear', 'join'),
            'box_ends': node.matching_join,
            'next': node.out_funcs,
            'doc': node.doc
        }
    return dag
