import json
from tarfile import TarFile

from .custom_flowgraph import FlowGraph  # TODO: change to metaflow.graph when the AST-only PR is merged.

from .utils import get_codepackage
import aiobotocore
import io
import os
from . import cached

from services.utils import logging

logger = logging.getLogger("GenerateDag")


@cached(alias="default")
async def get_dag(flow_name, location):
    '''
        Generates a DAG for a given codepackage tarball location and Flow name.

        Returns
        --------
        [
        boolean,
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
        First field conveys whether dag generation was successful.
        Second field contains the actual DAG.
        '''
    results = {}

    # get codepackage from S3
    session = aiobotocore.get_session()
    async with session.create_client('s3') as s3_client:
        try:
            codetar = await get_codepackage(s3_client, location)
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
