import pytest
from services.ui_backend_service.data.cache.custom_flowgraph import FlowGraph

pytestmark = [pytest.mark.unit_tests]


def test_flowgraph_ast_parsing():
  flow_source = """
from metaflow import FlowSpec, step, Parameter, retry

class DAGTest(FlowSpec):
    count = Parameter('count',
                      help="Amount of tasks to generate for each foreach step",
                      default=10)

    @step
    def start(self):
        self.next(self.regular_step)

    @step
    def regular_step(self):
        "Just a regular step that splits into two"
        self.foo = "bar"
        self.next(
            self.prepare_foreach,
            self.prepare_foreach2,
        )

    @step
    def prepare_foreach(self):
        "Generate a list of things to process in the first foreach"
        self.things = list(range(0, self.count))
        self.next(self.process_foreach, foreach='things')

    @step
    def prepare_foreach2(self):
        "Generate a list of things to process in the second foreach"
        self.things = list(range(0, self.count))
        self.next(self.process_foreach2, foreach='things')

    @step
    def process_foreach(self):
        print("Hello {}".format(self.input))
        self.next(self.join)

    @retry(times=2)
    @step
    def process_foreach2(self):
        "Process second foreach and retry in case of failures"
        print("Hello {}".format(self.input))
        self.next(self.join2)

    @step
    def join(self, inputs):
        self.next(self.ultimate_join)

    @step
    def join2(self, inputs):
        self.next(self.after_join)

    @step
    def after_join(self):
        print("Something after join2")
        self.next(self.ultimate_join)

    @step
    def ultimate_join(self, inputs):
        "Join both process path results"
        self.next(self.end)

    @step
    def end(self):
        print("done")


if __name__ == '__main__':
    DAGTest()
  """

  expected_graph = {
      "steps": {
          "start": {
              "name": "start",
              "type": "start",
              "line": 10,
              "doc": "",
              "decorators": [],
              "next": [
                  "regular_step"
              ]
          },
          "regular_step": {
              "name": "regular_step",
              "type": "split-static",
              "line": 14,
              "doc": "Just a regular step that splits into two",
              "decorators": [],
              "next": [
                  "prepare_foreach",
                  "prepare_foreach2"
              ],
              "matching_join": "ultimate_join"
          },
          "prepare_foreach": {
              "name": "prepare_foreach",
              "type": "split-foreach",
              "line": 23,
              "doc": "Generate a list of things to process in the first foreach",
              "decorators": [],
              "next": [
                  "process_foreach"
              ],
              "foreach_artifact": "things",
              "matching_join": "join"
          },
          "process_foreach": {
              "name": "process_foreach",
              "type": "linear",
              "line": 35,
              "doc": "",
              "decorators": [],
              "next": [
                  "join"
              ]
          },
          "join": {
              "name": "join",
              "type": "join",
              "line": 47,
              "doc": "",
              "decorators": [],
              "next": [
                  "ultimate_join"
              ]
          },
          "prepare_foreach2": {
              "name": "prepare_foreach2",
              "type": "split-foreach",
              "line": 29,
              "doc": "Generate a list of things to process in the second foreach",
              "decorators": [],
              "next": [
                  "process_foreach2"
              ],
              "foreach_artifact": "things",
              "matching_join": "join2"
          },
          "process_foreach2": {
              "name": "process_foreach2",
              "type": "linear",
              "line": 41,
              "doc": "Process second foreach and retry in case of failures",
              "decorators": [
                  {
                      "name": "retry",
                      "attributes": {
                          "times": 2,
                          "minutes_between_retries": "2"
                      },
                      "statically_defined": True
                  }
              ],
              "next": [
                  "join2"
              ]
          },
          "join2": {
              "name": "join2",
              "type": "join",
              "line": 51,
              "doc": "",
              "decorators": [],
              "next": [
                  "after_join"
              ]
          },
          "after_join": {
              "name": "after_join",
              "type": "linear",
              "line": 55,
              "doc": "",
              "decorators": [],
              "next": [
                  "ultimate_join"
              ]
          },
          "ultimate_join": {
              "name": "ultimate_join",
              "type": "join",
              "line": 60,
              "doc": "Join both process path results",
              "decorators": [],
              "next": [
                  "end"
              ]
          },
          "end": {
              "name": "end",
              "type": "end",
              "line": 65,
              "doc": "",
              "decorators": [],
              "next": []
          }
      },
      "graph_structure": [
          "start",
          "regular_step",
          [
              [
                  "prepare_foreach",
                  [
                      [
                          "process_foreach"
                      ]
                  ],
                  "join"
              ],
              [
                  "prepare_foreach2",
                  [
                      [
                          "process_foreach2"
                      ]
                  ],
                  "join2",
                  "after_join"
              ]
          ],
          "ultimate_join",
          "end"
      ],
      "doc": ""
  }
  graph = FlowGraph(flow_source, "DAGTest")
  steps_info, graph_structure = graph.output_steps()
  graph_info = {
      "steps": steps_info,
      "graph_structure": graph_structure,
      "doc": graph.doc
  }

  assert graph_info == expected_graph
