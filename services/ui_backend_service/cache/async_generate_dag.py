import json
import io
from tarfile import TarFile
from .custom_flowgraph import FlowGraph  # TODO: change to metaflow.graph when the AST-only PR is merged.

from .utils import get_codepackage
from . import cached

from services.utils import logging

logger = logging.getLogger("GenerateDag")


@cached(alias="default", key_builder=lambda func, session, flow_name, loc: "dag:{}/{}".format(flow_name, loc))
async def get_dag(boto_session, flow_name, location):
    '''
        Generates a DAG for a given codepackage tarball location and Flow name.

        Returns
        --------
        [
            {
                "step_name": {
                'type': string,
                'box_next': boolean,
                'box_ends': string,
                'next': list
                },
                ...
            }
        ]
        '''
    results = {}

    # get codepackage from S3
    async with boto_session.create_client('s3') as s3_client:
        codetar = await get_codepackage(s3_client, location)
        try:
            results = generate_dag(flow_name, codetar)
        except UnsupportedFlowLanguage:
            raise
        except Exception as ex:
            logger.exception("Exception processing dag")
            raise GenerateDAGFailed from ex

    return results

# Utilities


def generate_dag(flow_id, tarball_bytes):
    # extract the sourcecode from the tarball
    with TarFile.open(fileobj=io.BytesIO(tarball_bytes)) as f:
        info_json = f.extractfile('INFO').read().decode('utf-8')
        info = json.loads(info_json)
        # Break if language is not supported.
        if "use_r" in info and info["use_r"]:
            # TODO: get rid of this error once Flows written with R can be successfully parsed.
            # at the time of writing, the sourcecode is unable to be located in the tarball for R flows.
            raise UnsupportedFlowLanguage
        script_name = info['script']
        sourcecode = f.extractfile(script_name).read().decode('utf-8')

    # Initialize a FlowGraph object
    graph = FlowGraph(source=sourcecode, name=flow_id)
    # Build the DAG based on the DAGNodes given by the FlowGraph for the found FlowSpec class.
    dag = {}
    for node in graph:
        dag[node.name] = {
            'type': node.type,
            'box_next': node.type not in ('linear', 'join'),
            'box_ends': node.matching_join,
            'next': node.out_funcs
        }
    return dag


class UnsupportedFlowLanguage(Exception):
    def __init__(self):
        self.message = "Parsing DAG graph is not supported for the language used in this Flow."
        self.id = "dag-unsupported-flow-language"

    def __str__(self):
        return self.message


class GenerateDAGFailed(Exception):
    def __init__(self):
        self.message = "Failed to process DAG"
        self.id = "failed-to-process-dag"

    def __str__(self):
        return self.message
