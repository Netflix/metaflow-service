import ast
from metaflow.graph import deindent_docstring, DAGNode, FlowGraph as _FlowGraph


class StepVisitor(ast.NodeVisitor):

    def __init__(self, nodes):
        self.nodes = nodes
        super(StepVisitor, self).__init__()

    def visit_FunctionDef(self, node):
        decos = [d.func.id if isinstance(d, ast.Call) else d.id
                 for d in node.decorator_list]
        if 'step' in decos:
            doc = ast.get_docstring(node)
            self.nodes[node.name] = DAGNode(node, decos, doc if doc else '')


class FlowGraph(_FlowGraph):
    # NOTE: This implementation relies on passing in the name of the FlowSpec class
    # to be parsed from the sourcecode.
    def __init__(self, source, name):
        self.name = name

        self.nodes = self._create_nodes(source)
        self._traverse_graph()
        self._postprocess()

    def _create_nodes(self, source):
        def _flow(n):
            if isinstance(n, ast.ClassDef):
                return n.name == self.name

        # NOTE: Can possibly fail if filter returns multiple results,
        # but this would mean there are duplicate class names.
        [root] = list(filter(_flow, ast.parse(source).body))
        self.name = root.name
        doc = ast.get_docstring(root)
        self.doc = deindent_docstring(doc) if doc else ''
        nodes = {}
        StepVisitor(nodes).visit(root)
        return nodes

    def output_steps(self):

        steps_info = {}
        graph_structure = []

        def node_to_type(node):
            "convert internal node type to a more descriptive one for API consumers."
            if node.type in ["linear", "start", "end", "join"]:
                return node.type
            elif node.type == "split":
                return "split-static"
            elif node.type == "foreach":
                if node.parallel_foreach:
                    return "split-parallel"
                return "split-foreach"
            return "unknown"  # Should never happen

        def node_to_dict(name, node):
            return {
                "name": name,
                "type": node_to_type(node),
                "line": node.func_lineno,
                "doc": node.doc,
                "next": node.out_funcs,
                "foreach_artifact": node.foreach_param,
            }

        def populate_block(start_name, end_name):
            cur_name = start_name
            resulting_list = []
            while cur_name != end_name:
                cur_node = self.nodes[cur_name]
                node_dict = node_to_dict(cur_name, cur_node)

                steps_info[cur_name] = node_dict
                resulting_list.append(cur_name)

                if cur_node.type not in ("start", "linear", "join"):
                    # We need to look at the different branches for this
                    resulting_list.append(
                        [
                            populate_block(s, cur_node.matching_join)
                            for s in cur_node.out_funcs
                        ]
                    )
                    cur_name = cur_node.matching_join
                else:
                    cur_name = cur_node.out_funcs[0]
            return resulting_list

        graph_structure = populate_block("start", "end")

        steps_info["end"] = node_to_dict("end", self.nodes["end"])
        graph_structure.append("end")

        return steps_info, graph_structure
