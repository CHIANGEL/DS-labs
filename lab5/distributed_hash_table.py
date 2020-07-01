# Modified from the internet

import pprint
import hashlib
 
class DHT(object):
    def __init__(self, nodes=None, n_number=3):
        """
        :param nodes:           所有的节点
        :param n_number:        一个节点对应多少个虚拟节点
        :return:
        """
        self._n_number = n_number   #每一个节点对应多少个虚拟节点，这里默认是32个
        self._node_dict = {}        #用于将虚拟节点的hash值与node的对应关系
        self._sort_list = []        #用于存放所有的虚拟节点的hash值，这里需要保持排序
        if nodes:
            for node in nodes:
                self.add_node(node)
 
    def add_node(self, node):
        """
        添加node，首先要根据虚拟节点的数目，创建所有的虚拟节点，并将其与对应的node对应起来
        当然还需要将虚拟节点的hash值放到排序的里面
        这里在添加了节点之后，需要保持虚拟节点hash值的顺序
        :param node:
        :return:
        """
        for i in range(self._n_number):
            node_str = "{}{}".format(node, i)
            key = self._gen_key(node_str)
            self._node_dict[key] = node
            self._sort_list.append(key)
        self._sort_list.sort()
 
    def remove_node(self, node):
        """
        这里一个节点的退出，需要将这个节点的所有的虚拟节点都删除
        :param node:
        :return:
        """
        for i in range(self._n_number):
            node_str = "{}{}".format(node, i)
            key = self._gen_key(node_str)
            del self._node_dict[key]
            self._sort_list.remove(key)
 
    def get_node(self, key_str):
        """
        返回这个字符串应该对应的node，这里先求出字符串的hash值，然后找到第一个小于等于的虚拟节点，然后返回node
        如果hash值大于所有的节点，那么用第一个虚拟节点
        :param :
        :return:
        """
        if self._sort_list:
            key = self._gen_key(key_str)
            #print("DHT: key {}".format(key))
            for node_key in self._sort_list:
                #print("{}: {}".format("node_key", node_key))
                if key <= node_key:
                    #print("{}: {}".format("int(self._node_dict[node_key])", int(self._node_dict[node_key])))
                    return node_key, int(self._node_dict[node_key])
            #print("{}: {}".format("self._sort_list[0]", self._sort_list[0]))
            #print("{}: {}".format("int(self._node_dict[self._sort_list[0]])", int(self._node_dict[self._sort_list[0]])))
            return self._sort_list[0], int(self._node_dict[self._sort_list[0]])
        else:
            return None, None
    
    def get_next_node(self, current_vnode):
        cur_vnode_idx = self._sort_list.index(current_vnode)
        next_vnode_idx = (cur_vnode_idx + 1) % len(self._sort_list)
        return self._sort_list[next_vnode_idx], int(self._node_dict[self._sort_list[next_vnode_idx]])
 
    @staticmethod
    def _gen_key(key_str):
        """
        通过key，返回当前key的hash值，这里采用md5
        :param key:
        :return:
        """
        md5_str = hashlib.md5(key_str.encode(encoding='UTF-8')).hexdigest()
        return md5_str

if __name__ == "__main__":
    test = DHT(["0", "1", "2"], 2)
    pprint.pprint(test._sort_list)
    pprint.pprint(test._node_dict)
    print(test.get_node("DHT1"))
    print(test.get_node("DHT2"))
    print(test.get_node("DgasdfHT3"))
    print(test.get_next_node(test.get_node("DgasdfHT3")[0]))
    print(test._gen_key("DgasdfHT3"))