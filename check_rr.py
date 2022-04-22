'''
The format of each operation is: read/write(variable, value, client_id, transaction_id), denote as r/w(var, val, cid, tid)
Property One:
    For each Read operation r(var, val, cid_1, tid_1), then the related Write operation w(var, val, cid_2, tid_2) must be a committed operation, in other words, tid_2 must be a committed transaction.
Property Two:
    For each Read operation r(var, val_2, cid, tid), if there is a Write operation w(var, val_1, cid, tid) in the same transaction happened before this Read operation, then val_1 = val_2. Specifically, if there are multiple Write operations before the Read operation, the Read operation should return the value from the last Write operation.
Property Three:
    For each Read operation r(var, val, cid_1, tid_1), if the related Write operation w(var, val, cid_2, tid_2) is coming from a different transation tid_2, then the Write operation w(var, val, cid_2, tid_2) must be the last Write operation to var in trasaction tid_2.
Property Four:
    For multiple Read operations in the same transaction, r(var, val_1, cid, tid) and r(var, val_2, cid, tid), if there is no Write operation to var between them, then val_1 = val_2.
'''

import copy

class DiGraph:
    def __init__(self):
        self.adj_map = {}

    def add_edge(self, from_node, to_node):
        if from_node in self.adj_map:
            self.adj_map[from_node].add(to_node)
        else:
            self.adj_map[from_node] = {to_node}

    def add_vertex(self, new_node):
        if new_node not in self.adj_map:
            self.adj_map[new_node] = set()


class RRChecker:
    def __init__(self, ops):
        self.wr_rel = {}
        self.txns = {}
        self.r_nodes = {}
        self.w_nodes = {}
        current_tra = []
        
        # Add ops in the type an array of dicts: [{'op_type': 'w', 'var': 1, 'val': 1, 'client_id': 1, 'tra_id': 1}, ...]
        for i in range(len(ops)):
            op_dict = self.get_op(ops[i])
            #for the last op in each transaction
            if i == len(ops) - 1 or self.get_op(ops[i + 1])['tra_id'] != op_dict['tra_id']:
                current_tra.append(op_dict)
                
                for op in current_tra:
                    if op['op_type'] == 'w':
                        # if write, if var dont have graph create one and add tra_id as vertex in wr_rel
                        if op['var'] in self.wr_rel:
                            self.wr_rel[op['var']].add_vertex(op_dict['tra_id'])
                        else:
                            graph = DiGraph()
                            graph.add_vertex(op_dict['tra_id'])
                            self.wr_rel[op['var']] = graph

                        # find the corresponding read op and add edge in wl_rel
                        if op['var'] in self.r_nodes:
                            for txn in self.r_nodes[op['var']]:
                                # r_nodes[op['var']] record the txn_id that read on var
                                if txn != op_dict['tra_id']:
                                    for node in self.txns[txn]:
                                        if node['val'] == op['val'] and node['var'] == op['var'] and node[
                                            'op_type'] == 'r':
                                            self.wr_rel[op['var']].add_edge(op_dict['tra_id'], txn)
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

    def check_repeatable_read(self,i):
        pro1 = []
        pro2 = []
        pro3 = []
        pro4 = []
        for var,r_t_set in self.r_nodes.items():
            for r_tra_id in r_t_set:
                bef_r = False
                bef_w = False
                for r_op in self.txns[r_tra_id]:
                    if r_op['var'] == var and r_op['op_type'] == 'r':
                        if bef_w == True:
                            if r_op['val'] != w_bef_op['val']:
                                e_txn = {'var': r_op['var'], 'old_val': w_bef_op['val'], 'new_val': r_op['val'], 'tra_id': r_op['tra_id']}
                                pro2.append(e_txn)
                                file = open('output/'+str(i)+'/pro2.txt','a');
                                file.write('( var: ' + str(e_txn['var']) + ', old_val: ' + str(e_txn['old_val']) + ', new_val: ' + str(e_txn['new_val']) + ', tra_id: ' + str(e_txn['tra_id']) + ')\n')
                                file.close();
                        else:
                            if bef_r == True:
                                if r_op['val'] != r_bef_op['val']:
                                    e_txn = {'var': r_op['var'], 'old_val': r_bef_op['val'], 'new_val': r_op['val'], 'tra_id': r_op['tra_id']}
                                    pro4.append(e_txn)
                                    file = open('output/'+str(i)+'/pro4.txt','a');
                                    file.write('( var: ' + str(e_txn['var']) + ', old_val: ' + str(e_txn['old_val']) + ', new_val: ' + str(e_txn['new_val']) + ', tra_id: ' + str(e_txn['tra_id']) + ')\n')
                                    file.close();
                            else:
                                find_write = False
                                for w_t, r_set in self.wr_rel[r_op['var']].adj_map.items():
                                    if r_op['tra_id'] in r_set:
                                        last_write = False
                                        for w_op in reversed(self.txns[w_t]):
                                            if r_op['var'] == w_op['var'] and r_op['val'] == w_op['val'] and w_op['op_type'] == 'w':
                                                find_write = True
                                                if last_write == True:
                                                    pro3.append(e_txn)
                                                    e_txn = {'var': r_op['var'], 'val': r_op['val'], 'w_tra_id': w_op['tra_id'], 'r_tra_id': r_op['tra_id']}
                                                    file = open('output/'+str(i)+'/pro3.txt','a');
                                                    file.write('( var: ' + str(e_txn['var']) + ', val: ' + str(e_txn['val']) + ', w_tra_id: ' + str(e_txn['w_tra_id']) + ', r_tra_id: ' + str(e_txn['r_tra_id']) + ')\n')
                                                    file.close();
                                                break
                                            if r_op['var'] == w_op['var'] and w_op['op_type'] == 'w':
                                                last_write = True
                                    if find_write == True:
                                        break
                                if find_write == False and r_op['val'] != str(0):
                                    e_txn = {'var': r_op['var'], 'val': r_op['val'], 'tra_id': r_op['tra_id']}
                                    pro1.append(e_txn)
                                    file = open('output/'+str(i)+'/pro1.txt','a');
                                    file.write('( var: ' + str(e_txn['var']) + ', val: ' + str(e_txn['val']) + ', tra_id: '+ str(e_txn['tra_id']) + ')\n')
                                    file.close();
                        bef_r = True
                        r_bef_op = r_op
                        bef_w = False 
                    elif r_op['var'] == var and r_op['op_type'] == 'w':
                        bef_w = True
                        w_bef_op = r_op    
        return pro1,pro2, pro3, pro4      


if __name__ == '__main__':
    for i in range(1):
        folder_name = "output/"+str(i)+"/result.txt"
        with open(folder_name) as in_file:
            raw_ops = in_file.readlines()

        causal_hist = RRChecker(raw_ops)
        pro1, pro2, pro3, pro4 = causal_hist.check_repeatable_read(i)
        if len(pro2) != 0:
            print('find data violate Property Two in trace: ' + str(i))
        if len(pro3) != 0:
            print('find data violate Property Three in trace: ' + str(i))
        if len(pro4) != 0:
            print('find data violate Property Four in trace: ' + str(i))
