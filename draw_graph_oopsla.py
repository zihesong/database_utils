# elle jepson画图
import copy
from genericpath import exists
from gettext import find
import networkx as nx
import matplotlib.pyplot as plt
import matplotlib as mpl

class DiGraph:
    def __init__(self):
        self.adj_map = {}
        self.visited = []
        self.trace = []
        self.boo_cycle = False
        self.cycle_list = []
        self.map = {}

    def add_edge(self, from_node, to_node):
        if from_node in self.adj_map:
            self.adj_map[from_node].add(to_node)
        else:
            self.adj_map[from_node] = {to_node}
    
    def add_wr_info(self, from_node, to_node, key, value):
        from_info = {'type':'wr', 'action': 'w', 'node':to_node, 'key':key, 'value':value}
        to_info = {'type':'wr', 'action': 'r', 'node':from_node, 'key':key, 'value':value}
        if from_node in self.map:
            self.map[from_node].append(from_info)
        else:
            self.map[from_node] = [from_info]
        if to_node in self.map:
            self.map[to_node].append(to_info)
        else:
            self.map[to_node] = [to_info]

    def add_ww_info(self, from_node, to_node, rel_txn):
        from_info = {'type':'ww', 'node':to_node, 'co_node': rel_txn}
        to_info = {'type':'ww', 'node':from_node, 'co_node': rel_txn}
        if from_node in self.map:
            self.map[from_node].append(from_info)
        else:
            self.map[from_node] = [from_info]
        if to_node in self.map:
            self.map[to_node].append(to_info)
        else:
            self.map[to_node] = [to_info]

    def add_vertex(self, new_node):
        if new_node not in self.adj_map:
            self.adj_map[new_node] = set()

    def has_edge(self, from_node, to_node):
        if from_node in self.adj_map and to_node in self.adj_map[from_node]:
            return True
        else:
            return False

    def has_cycle(self):
        for key in list(self.adj_map.keys()):
            reachable = set()
            if self.dfs_util_reach(key, key, reachable):
                print("reach key is: " + str(key))
                return True
        return False

    def find_cycle(self,start_node):
        if start_node in self.visited:
            if start_node in self.trace:
                self.boo_cycle = True
                trace_index = self.trace.index(start_node)
                for i in range(trace_index, len(self.trace)):
                    print(str(self.trace[i]) + ' ', end='')
                    self.cycle_list.append(self.trace[i])
                print('\n', end='')
                return
            return
        self.visited.append(start_node)
        self.trace.append(start_node)

        if start_node != '' :
            for node in self.adj_map[start_node]:
                if node in self.adj_map:
                    self.find_cycle(node)
        self.trace.pop()

    def dfs_util_reach(self, s, u, reachable):
        if u in self.adj_map:
            for node in self.adj_map[u]:
                if node == s:
                    return True
                elif node in reachable:
                    continue
                else:
                    reachable.add(node)
                    if self.dfs_util_reach(s, node, reachable):
                        return True
        return False

    def dfs_util_all(self, u, reachable):
        if u in self.adj_map:
            for node in self.adj_map[u]:
                if node in reachable:
                    continue
                reachable.add(node)
                self.dfs_util_all(node, reachable)

    def take_closure(self):
        clone_map = self.adj_map.copy()
        for node in self.adj_map:
            reachable = set()
            self.dfs_util_all(node, reachable)
            clone_map[node] = reachable
        self.adj_map = clone_map

    def union_with(self, g):
        for key, value in g.adj_map.items():
            if key in self.adj_map:
                self.adj_map[key] = self.adj_map[key].union(value)
            else:
                self.adj_map[key] = value
    
    def union_ww(self,g):
        for node, txns in g.map.items():
            for txn in txns:
                if node in self.map:
                    self.map[node].append(txn)
                else:
                    self.map[node] = [txn]


class OopslaAtomicHistoryPO:
    def __init__(self, ops):
        self.so = DiGraph()
        self.vis = DiGraph()
        self.wr_rel = {}
        self.txns = {}
        client_in_so = {}
        self.r_nodes = {}
        self.w_nodes = {}
        current_tra = []
        # Add ops in the type an array of dicts: [{'op_type': 'w', 'var': 1, 'val': 1, 'client_id': 1, 'tra_id': 1}, ...]
        for i in range(len(ops)):
            op_dict = self.get_op(ops[i])
            #for the last op in each transaction
            if i == len(ops) - 1 or self.get_op(ops[i + 1])['tra_id'] != op_dict['tra_id']:
                if op_dict['client_id'] in client_in_so:
                    #add edge between transactions from same client to self.so
                    self.so.add_edge(client_in_so[op_dict['client_id']], op_dict['tra_id']) 
                client_in_so[op_dict['client_id']] = op_dict['tra_id']
                current_tra.append(op_dict)
                
                for op in current_tra:
                    if op['op_type'] == 'w':
                        # if write, if key dont have graph create one and add tra_in as vertex in wr_rel
                        if op['var'] in self.wr_rel:
                            self.wr_rel[op['var']].add_vertex(op_dict['tra_id'])
                        else:
                            graph = DiGraph()
                            graph.add_vertex(op_dict['tra_id'])
                            self.wr_rel[op['var']] = graph

                        # find the corresponding read op and add edge in wl_rel
                        if op['var'] in self.r_nodes:
                            for tra in self.r_nodes[op['var']]:
                                # r_nodes[op['var']] record the txn_id that read on var
                                if tra != op_dict['tra_id']:
                                    for node in self.txns[tra]:
                                        if node['val'] == op['val'] and node['var'] == op['var'] and node[
                                            'op_type'] == 'r':
                                            self.wr_rel[op['var']].add_edge(op_dict['tra_id'], tra)
                                            self.wr_rel[op['var']].add_wr_info(op_dict['tra_id'], tra, op['var'], op['val'])
                                            break
                        if op['var'] not in self.w_nodes:
                            self.w_nodes[op['var']] = set()
                        # add the tra_id into w_node[op['var']]
                        self.w_nodes[op['var']].add(op_dict['tra_id'])
                    else:
                        if op['var'] in self.wr_rel:
                            # if read, find the corresponding write and add edge in wr_rel
                            has_wr = False
                            for key, t_set in self.wr_rel[op['var']].adj_map.items():
                                if key != op_dict['tra_id']:
                                    for node in self.txns[key]:
                                        if node['val'] == op['val'] and node['var'] == op['var'] and node[
                                            'op_type'] == 'w':
                                            t_set.add(op_dict['tra_id'])
                                            self.wr_rel[op['var']].add_wr_info(key, op_dict['tra_id'], op['var'], op['val'])
                                            has_wr = True
                                            break
                                    if has_wr:
                                        break

                        if op['var'] not in self.r_nodes:
                            self.r_nodes[op['var']] = set()
                        # add the tra_id into r_node[op['var']]
                        self.r_nodes[op['var']].add(op_dict['tra_id'])
                if op_dict['tra_id'] not in self.txns:
                    self.txns[op_dict['tra_id']] = []
                # add current txn into self.txns
                self.txns[op_dict['tra_id']].extend(current_tra.copy())
                current_tra.clear()
            else:
                current_tra.append(op_dict)
        self.vis = copy.deepcopy(self.so)
        self.so.take_closure()

    def get_op(self, op):
        op = op.strip('\n')
        arr = op[2:-1].split(',')
        return {
            'op_type': op[0],
            'var': arr[0],
            'val': arr[1],
            'client_id': int(arr[2]),
            'tra_id': int(arr[3]),
        }


    def get_wr(self):
        # combine all wr_rel of different var into one graph
        wr = DiGraph()
        for key, digraph in self.wr_rel.items():
            wr.union_with(digraph)
            wr.union_ww(digraph)
        return wr

    def vis_includes(self, g):
        self.vis.union_with(g)

    def vis_is_trans(self):
        self.vis.take_closure()

    def casual_ww(self):
        ww = {}
        for x, wr_x in self.wr_rel.items():
            ww_x = DiGraph()
            for t1, t3s in wr_x.adj_map.items():
                for t2 in list(wr_x.adj_map):
                    if t1 != t2:
                        has_edge = False
                        # if self.vis.has_edge(t2, t1):
                        #     has_edge = True
                        # else:
                        for t3 in t3s:
                            if self.vis.has_edge(t2, t3):
                                has_edge = True
                                break
                        if has_edge:
                            ww_x.add_edge(t2, t1)
                            ww_x.add_ww_info(t2, t1, t3)
            ww[x] = ww_x
        return ww

    def check_read_zero(self):
        for key, t_set in self.vis.adj_map.items():
            w_vars = set()
            for node in self.txns[key]:
                if node['op_type'] == 'w':
                    w_vars.add(node['var'])
            for t in t_set:
                for t_node in self.txns[t]:
                    if t_node['op_type'] == 'r' and t_node['val'] == '0' and t_node['var'] in w_vars:
                        return True
        return False


if __name__ == '__main__':
    for i in range(1):
        folder_name = "output/"+str(i)+"/result.txt"
        with open(folder_name) as in_file:
            raw_ops = in_file.readlines()

        causal_hist = OopslaAtomicHistoryPO(raw_ops)
        so_hist = copy.deepcopy(causal_hist.vis)

        wr_hist = causal_hist.get_wr()
        causal_hist.vis_includes(wr_hist)

        ww = causal_hist.casual_ww()
        ww_hist = DiGraph()
        for key, digraph in ww.items():
            ww_hist.union_with(digraph)
            ww_hist.union_ww(digraph)
        causal_hist.vis_includes(ww_hist)
        causal_hist.vis.find_cycle(0)

        node_list = []
        graph = nx.DiGraph()
        for node in causal_hist.vis.cycle_list:
            if node not in node_list:
                node_list.append(node)
        graph.add_nodes_from(node_list)
        node_list = copy.deepcopy(node_list)

        node_labels = {}

        for node in node_list:    
            
            if node in ww_hist.adj_map:
                for adj_node in ww_hist.adj_map[node]:
                    if adj_node in node_list:
                        graph.add_edge(node,adj_node, rel='ww')
                        for op in ww_hist.map[node]:
                            if op['node'] == adj_node:
                                if op['co_node'] not in node_list:
                                    node_list.append(op['co_node'])
                                    graph.add_node(op['co_node'])
            
        for node in graph.nodes:

            if node in so_hist.adj_map:
                for adj_node in so_hist.adj_map[node]:
                    if adj_node in graph.nodes:
                        graph.add_edge(node,adj_node, rel='so')
            
            if node in wr_hist.adj_map:
                for adj_node in wr_hist.adj_map[node]:
                    if adj_node in graph.nodes:
                        if (node,adj_node) not in graph.edges:
                            graph.add_edge(node,adj_node, rel='wr')
                        else:
                            graph[node][adj_node]['rel']=graph[node][adj_node]['rel'] + ' & wr'
                        
                        if node in wr_hist.map:
                            tmp_desc = []
                            tmp_adj_desc = []
                            for op in wr_hist.map[node]:
                                if op['node'] == adj_node:
                                    if node not in node_labels:
                                        node_labels[node] = str(node) + '\n'
                                    if op['key'] not in tmp_desc:
                                        tmp_desc.append(op['key'])
                                    if adj_node not in node_labels:
                                        node_labels[adj_node] = str(adj_node) + '\n'
                                    if op['key'] not in tmp_adj_desc:
                                        tmp_adj_desc.append(op['key'])

                            if tmp_desc is not None:
                                node_labels[node] = node_labels[node] + 'w('
                                for i in range(len(tmp_desc)-1):
                                    node_labels[node] = node_labels[node] + tmp_desc[i] + ','
                                node_labels[node] = node_labels[node] + tmp_desc[len(tmp_desc)-1] +') - ' + str(adj_node) + '\n'
                                node_labels[adj_node] = node_labels[adj_node] + 'r('
                                for j in range(len(tmp_adj_desc)-1):
                                    node_labels[adj_node] = node_labels[adj_node] + tmp_adj_desc[j] + ','
                                node_labels[adj_node] = node_labels[adj_node] + tmp_adj_desc[len(tmp_adj_desc)-1] +') - ' + str(node) + '\n'      

        pos=nx.shell_layout(graph)
        nodes = dict(graph.degree)
        low, *_, high = sorted(nodes.values())
        norm = mpl.colors.Normalize(vmin=low, vmax=high, clip=True)
        mapper = mpl.cm.ScalarMappable(norm=norm, cmap=mpl.cm.coolwarm)
        nx.draw(graph, pos, with_labels = False, nodelist = nodes, node_color = [mapper.to_rgba(i) for i in nodes.values()], node_size=8000, cmap=plt.cm.Dark2, edge_cmap=plt.cm.coolwarm)
        nx.draw_networkx_labels(graph, pos, labels=node_labels, font_color = 'black', font_size = 8)
        nx.draw_networkx_edge_labels(graph, pos, font_size=8, font_color="blue")
        plt.show()
