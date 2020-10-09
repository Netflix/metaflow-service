import hashlib
import json
from tarfile import TarFile

from metaflow.client.cache import CacheAction
from metaflow import FlowSpec
from .custom_flowgraph import FlowGraph # TODO: change to metaflow.graph when the AST-only PR is merged.

from .utils import NoRetryS3
from .utils import MetaflowS3CredentialsMissing, MetaflowS3AccessDenied, MetaflowS3Exception, MetaflowS3NotFound, MetaflowS3URLException
class GenerateDag(CacheAction):
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

    @classmethod
    def format_request(cls, flow_id, codepackage_location):
        msg = {
            'location': codepackage_location,
            'flow_id': flow_id
        }
        result_key = 'dag:result:%s' % hashlib.sha1((flow_id+codepackage_location).encode('utf-8')).hexdigest()
        stream_key = 'dag:stream:%s' % hashlib.sha1((flow_id+codepackage_location).encode('utf-8')).hexdigest()

        return msg,\
               [result_key],\
               stream_key,\
               [stream_key]


    @classmethod
    def response(cls, keys_objs):
        '''
        Returns the generated DAG result
        '''
        return [ json.loads(val) for key, val in keys_objs.items() if key.startswith('dag:result') ][0]

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
                **kwargs):

        results = {}
        location = message['location']
        flow_name = message['flow_id']

        result_key = [ key for key in keys if key.startswith('dag:result')][0]
        stream_error = lambda err, id: stream_output({"type": "error", "message": err, "id": id})

        # get codepackage from S3
        with NoRetryS3() as s3:
          try:
            codetar = s3.get(location)
            results[result_key] = json.dumps(generate_dag(flow_name, codetar.path))
          except MetaflowS3AccessDenied as ex:
            stream_error(str(ex), "s3-access-denied")
          except MetaflowS3NotFound as ex:
            stream_error(str(ex), "s3-not-found")
          except MetaflowS3URLException as ex:
            stream_error(str(ex), "s3-bad-url")
          except MetaflowS3CredentialsMissing as ex:
            stream_error(str(ex), "s3-missing-credentials")
          except MetaflowS3Exception as ex:
            stream_error(str(ex), "s3-generic-error")
          except UnsupportedFlowLanguage as ex:
            stream_error(str(ex), "dag-unsupported-flow-language")
          except Exception as ex:
            stream_error(str(ex), "dag-processing-error")

        return results

# Utilities

def generate_dag(flow_id, tarball_path):
  # extract the sourcecode from the tarball
  with TarFile.open(tarball_path) as f:
    info_json = f.extractfile('INFO').read().decode('utf-8')
    info = json.loads(info_json)
    # Break if language is not supported.
    if info["use_r"]:
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
      'box_next': node.has_tail_next,
      'box_ends': node.matching_join,
      'next': node.out_funcs
    }
  return dag

class UnsupportedFlowLanguage(Exception):
  def __str__(self):
    return "Parsing DAG graph is not supported for the language used in this Flow."
