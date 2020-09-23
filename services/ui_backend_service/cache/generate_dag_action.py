import hashlib
import json
from tarfile import TarFile
import inspect
import tempfile
import os
import sys

from metaflow.client.cache import CacheAction
from metaflow import S3, FlowSpec
from metaflow.graph import FlowGraph

class GenerateDag(CacheAction):
    '''
    Generates a DAG for a given codepackage tarball location.

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
    def format_request(cls, codepackage_location):
        msg = {
            'location': codepackage_location
        }
        result_key = 'dag:result:%s' % hashlib.sha1(codepackage_location.encode('utf-8')).hexdigest()

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

        result_key = [ key for key in keys if key.startswith('dag:result')][0]

        # expression to use for printing progress during stream.
        stream_print = lambda line: stream_output({"progress": line})

        # get codepackage from S3
        with S3() as s3:
          try:
            codetar = s3.get(location)
            results[result_key] = json.dumps([True, generate_dag(codetar.path)])
          except Exception as ex:
            results[result_key] = json.dumps([False, "failed to generate dag: %s" % ex])

        return results

# Utilities

def generate_dag(tarball_path):
  # extract the sourcecode from the tarball
  with TarFile.open(tarball_path) as f:
    info = f.extractfile('INFO').read().decode('utf-8')
    script_name = json.loads(info)['script']
    sourcecode = f.extractfile(script_name).read().decode('utf-8')
  
  # Initialize a FlowGraph object
  graph = flowgraph_from_source(sourcecode)
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

def flowgraph_from_source(sourcestring):
  # First we need to initialize the codepackage content as a module.
  # ***IMPORTANT***
  # TODO: This is potentially risky. Assume that we are in control of the code,
  # as it has ended up in our S3 bucket? Otherwise it might be a vector for arbitrary code execution.
  # ***IMPORTANT***
  tmpfile = tempfile.NamedTemporaryFile(suffix='.py', delete=True)
  try:
    tmpfile.write(sourcestring.encode('utf8'))
    tmpfile.flush()
    tmpmodule_path, tmpmodule_file_name = os.path.split(tmpfile.name)
    tmpmodule_name = tmpmodule_file_name[:-3]
    sys.path.append(tmpmodule_path)
    source_module = __import__(tmpmodule_name)
    # After the module has been initialized, we look for any class that subclasses FlowSpec, and pass this
    # to the FlowGraph to get a graph parsed.
    flowspec_subclasses = [ cls for cls in dir(source_module)
                                if inspect.isclass(getattr(source_module, cls))
                                if issubclass(getattr(source_module, cls), FlowSpec)
                                if not getattr(source_module, cls) == FlowSpec
                          ]
    main_flow_class = getattr(source_module, flowspec_subclasses[0])
    graph = FlowGraph(main_flow_class)
  finally:
    tmpfile.close()
  return graph
