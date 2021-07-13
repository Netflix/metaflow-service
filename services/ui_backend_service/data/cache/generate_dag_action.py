import hashlib
import json
import os
import boto3
from tarfile import TarFile

from .client import CacheAction
from services.utils import get_traceback_str

from .custom_flowgraph import FlowGraph
from .utils import (CacheS3AccessDenied, CacheS3CredentialsMissing,
                    CacheS3Exception, CacheS3NotFound,
                    CacheS3URLException, get_s3_obj)


class GenerateDag(CacheAction):
    """
    Generates a DAG for a given codepackage tarball location and Flow name.

    Parameters
    ----------
    flow_id : str
        The flow id that this codepackage belongs to.
        Required for finding the correct class inside the parser logic.
    codepackage_location : str
        the S3 location for the codepackage to be fetched.

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
    def format_request(cls, flow_id, codepackage_location, invalidate_cache=False):
        msg = {
            'location': codepackage_location,
            'flow_id': flow_id
        }
        result_key = 'dag:result:%s' % hashlib.sha1((flow_id + codepackage_location).encode('utf-8')).hexdigest()
        stream_key = 'dag:stream:%s' % hashlib.sha1((flow_id + codepackage_location).encode('utf-8')).hexdigest()

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
        location = message['location']
        flow_name = message['flow_id']

        result_key = [key for key in keys if key.startswith('dag:result')][0]

        def stream_error(err, id, traceback=None):
            return stream_output({"type": "error", "message": err, "id": id, "traceback": traceback})

        # get codepackage from S3
        s3 = boto3.client("s3")
        try:
            codetar = get_s3_obj(s3, location)
            results[result_key] = json.dumps(generate_dag(flow_name, codetar.name))
        except CacheS3AccessDenied as ex:
            stream_error(str(ex), "s3-access-denied")
        except CacheS3NotFound as ex:
            stream_error(str(ex), "s3-not-found")
        except CacheS3URLException as ex:
            stream_error(str(ex), "s3-bad-url")
        except CacheS3CredentialsMissing as ex:
            stream_error(str(ex), "s3-missing-credentials")
        except CacheS3Exception as ex:
            stream_error(str(ex), "s3-generic-error")
        except UnsupportedFlowLanguage as ex:
            stream_error(str(ex), "dag-unsupported-flow-language")
        except Exception as ex:
            stream_error(str(ex), "dag-processing-error", get_traceback_str())

        return results

# Utilities


def generate_dag(flow_id, tarball_path):
    # extract the sourcecode from the tarball
    with TarFile.open(tarball_path) as f:
        info_json = f.extractfile('INFO').read().decode('utf-8')
        info = json.loads(info_json)
        # Break if language is not supported.
        if "use_r" in info and info["use_r"]:
            raise UnsupportedFlowLanguage
        # script name contains the full path passed to the python executable,
        # trim this down to the python filename
        script_name = os.path.basename(info['script'])
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
            'next': node.out_funcs,
            'doc': node.doc
        }
    return dag


class UnsupportedFlowLanguage(Exception):
    def __str__(self):
        return "Parsing DAG graph is not supported for the language used in this Flow."
