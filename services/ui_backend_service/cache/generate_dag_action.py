import hashlib
import json
from tarfile import TarFile
import inspect
import tempfile
import os
import sys

from metaflow.client.cache import CacheAction
from metaflow import S3, FlowSpec
from .custom_flowgraph import FlowGraph # TODO: change to metaflow.graph when the AST-only PR is merged.

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

        return msg,\
               [result_key],\
               None,\
               []


    @classmethod
    def response(cls, keys_objs):
        '''
        Returns the generated DAG result
        '''
        return [ json.loads(val) for key, val in keys_objs.items() if key.startswith('dag:result') ][0]

    @classmethod
    def stream_response(cls, it):
        for msg in it:
            if msg is None:
                yield msg
            else:
                yield {'event': msg}

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

        # get codepackage from S3
        with S3() as s3:
          try:
            codetar = s3.get(location)
            results[result_key] = json.dumps([True, generate_dag(flow_name, codetar.path)])
          except Exception as ex:
            results[result_key] = json.dumps([False, "failed to generate dag: %s" % ex])

        return results

# Utilities

def generate_dag(flow_id, tarball_path):
  # extract the sourcecode from the tarball
  with TarFile.open(tarball_path) as f:
    info = f.extractfile('INFO').read().decode('utf-8')
    script_name = json.loads(info)['script']
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
