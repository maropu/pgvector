#!/usr/bin/env python3

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the 'License'); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import json


def convert_to_graphviz_format(nodes, edges, layout):
    dot_str = 'graph [\n'
    dot_str += '  layout = {}\n'.format(layout)
    dot_str += '];\n'

    for level, nodes_in_level in nodes.items():
        if level > 0:
            dot_str += f'subgraph cluster_level_{level} {{\n'
            dot_str += f'  label = "Level#{level}";\n'
            dot_str += f'  bgcolor = "grey90";\n'

        for page, values in nodes_in_level.items():
            for offset, distance, tpe in values:
                dot_str += f'subgraph cluster_{page} {{\n'
                dot_str += f'  label = "Block#{page}";\n'

                if level > 0:
                    dot_str += f'  bgcolor = "white";\n'

                if tpe == 1:
                    dot_str += f'  "{page}/{offset}" [label="entryPoint\\n(d={distance})", penwidth=3, color=red];\n'
                elif tpe == 2:
                    dot_str += f'  "{page}/{offset}" [label="{page}/{offset}\\n(d={distance})", penwidth=3, color=blue];\n'
                else:
                    dot_str += f'  "{page}/{offset}"[label="{page}/{offset}\\n(d={distance})"];\n'

            dot_str += '}\n'

        if level > 0:
            dot_str += '}\n'

    for src_page, src_offset, dst_page, dst_offset in edges:
        dot_str += f'"{src_page}/{src_offset}" -> "{dst_page}/{dst_offset}";\n'

    return 'digraph G {\n' + dot_str + '}\n'


def print_as_graphviz_format(filename, layout):
    graph = None

    try:
        with open('hnsw_graph.json', 'r') as f:
          graph = json.load(f)
    except Exception as e:
        raise Exception(f'Cannot open {filename}: {e}')

    nodes = {}
    edges = []

    # Variables to compute an entry point and terminal nodes
    dst_set = set()
    src_set = set()

    for edge in graph['edges']:
        src_page, src_offset, dst_page, dst_offset = edge['src_blkno'], edge['src_offno'], edge['dst_blkno'], edge['dst_offno']
        edges.append((src_page, src_offset, dst_page, dst_offset))
        src_set.add((src_page, src_offset))
        dst_set.add((dst_page, dst_offset))

    assert len(src_set.difference(dst_set)) == 1

    entry_point = list(src_set.difference(dst_set))[0]
    terminal_nodes = dst_set.difference(src_set)

    for node in graph['nodes']:
        page, offset, level, distance = node['blkno'], node['offno'], node['level'], node['distance']
        if level not in nodes:
            nodes[level] = {}
        if page not in nodes[level]:
            nodes[level][page] = []

        # tpe=1 is an etnry point and tpe=2 is a terminal node; otherwise, tpe=0
        tpe = 0
        if (page, offset) == entry_point:
            tpe = 1
        elif (page, offset) in terminal_nodes:
            tpe = 2

        nodes[level][page].append((offset, distance, tpe))

    print(convert_to_graphviz_format(nodes, edges, layout))


if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('filename', type=str)
    parser.add_argument('--layout', type=str, default='dot')
    args = parser.parse_args()

    print_as_graphviz_format(args.filename, args.layout)
