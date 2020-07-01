import xmlrpc.client, xmlrpc.server
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn
from kazoo.client import KazooClient, KazooState
from kazoo.protocol.states import EventType
import kazoo.exceptions
import subprocess
import pprint
import random
import socket
import errno
import time
import json

class ThreadXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass

def zk_state_listener(state):
    if state == KazooState.LOST:
        print("INFO: Warning: kazoo LOST")
    elif state == KazooState.SUSPENDED:
        print("INFO: Warning: kazoo SUSPENDED")
    else:
        print("INFO: Warning: kazoo CONNECTED")

master_host = ""
master_port = -1
group_infos = {}

zk_host = "localhost"
zk_port = 2181
zk = KazooClient(hosts=(zk_host + ":" + str(zk_port)))
zk.start()
zk.add_listener(zk_state_listener)

CHECK_INTERVAL = 0.1
PERSISTENCE_SUCCESS = 0
PERSISTENCE_ERROR = -1
DUMP_SUCCESS = 0
DUMP_ERROR = -1

random.seed(time.time())

def get_servers(event=None):
    global zk
    global group_infos
    servers = zk.get_children('/GroupMember', watch=get_servers)
    servers = [item for item in servers if "Master" not in item]
    print("INFO: Watch event caught in /GroupMember! Start updating server infos")
    new_group_infos = {}
    for server in servers:
        group_id = int(server[:server.find("-")])
        server_id = int(server[server.find("-") + 1:])
        if group_id not in new_group_infos:
            new_group_infos[group_id] = []
        data = zk.get('/GroupMember/{}'.format(server))[0]
        if data:
            server_info = eval(data.decode())
            new_group_infos[group_id].append(server_info)
    group_infos = new_group_infos
    print("INFO: Get available server infos:")
    pprint.pprint(group_infos)

def master_setup():
    global master_host
    global master_port
    global zk
    global group_infos
    print("INFO: Start getting master infos")
    data = zk.get('/GroupMember/{}'.format("MasterHost"))[0]
    if data:
        master_host = data.decode()
    else:
        print("ERROR: Fail to get master port from zookeeper!")
        raise
    data = zk.get('/GroupMember/{}'.format("MasterPort"))[0]
    if data:
        master_port = int(data.decode())
    else:
        print("ERROR: Fail to get master port from zookeeper!")
        raise
    print("INFO: Get master addr: {}:{}".format(master_host, master_port))

class masterRPC:
    def hash(self, key):
        global zk
        global group_infos
        hash_value =  abs(hash(key)) % len(group_infos)
        group_ids = list(group_infos.keys())
        group_ids.sort()
        return group_ids[hash_value]

    def read_redirect(self, key):
        global zk
        global group_infos
        target_group_id = self.hash(key)
        # Load balance for READ operation on active servers in target group
        weight_data = {}
        for ServerId, ServerInfo in enumerate(group_infos[target_group_id]):
            weight_data[ServerId] = ServerInfo["weight"]
        total_weight = sum(weight_data.values())
        tmp = random.uniform(0, total_weight)
        curr_sum = 0
        target_server_id = None
        for key in weight_data.keys():
            curr_sum += weight_data[key]
            if tmp <= curr_sum:
                target_server_id = key
                break
        if target_server_id == None:
            target_server_id = 0
        ServerInfo = group_infos[target_group_id][target_server_id]
        print("INFO: redirect client request to server {}-{} on {}:{}".format(target_group_id, target_server_id, ServerInfo['host'], ServerInfo["port"]))
        return ("http://" + ServerInfo['host']  + ":" +  str(ServerInfo["port"]))

    def write_redirect(self, key):
        global zk
        global group_infos
        target_group_id = self.hash(key)
        # Load balance for READ operation on active servers in target group
        weight_data = {}
        for ServerId, ServerInfo in enumerate(group_infos[target_group_id]):
            weight_data[ServerId] = ServerInfo["weight"]
        total_weight = sum(weight_data.values())
        tmp = random.uniform(0, total_weight)
        curr_sum = 0
        target_server_id = None
        for key in weight_data.keys():
            curr_sum += weight_data[key]
            if tmp <= curr_sum:
                target_server_id = key
                break
        if target_server_id == None:
            target_server_id = 0
        ServerInfo = group_infos[target_group_id][target_server_id]
        print("INFO: redirect client request to server {}-{} on {}:{}".format(target_group_id, target_server_id, ServerInfo['host'], ServerInfo["port"]))
        return ("http://" + ServerInfo['host']  + ":" +  str(ServerInfo["port"]))

    def get(self, key):
        global zk
        global group_infos
        target_server = self.read_redirect(key)
        return target_server

    def put(self, key):
        global zk
        global group_infos
        target_server = self.write_redirect(key)
        return target_server

    def delete(self, key):
        global zk
        global group_infos
        target_server = self.write_redirect(key)
        return target_server

    def make_persistence(self):
        global zk
        global group_infos
        print("INFO: sending dump command to currently active server")
        try:
            for group_id in group_infos:
                group_info = group_infos[group_id]
                for ServerId, ServerInfo in enumerate(group_info):
                    print("INFO: Dumping server {}-{}".format(group_info, ServerId))
                    serverClient = xmlrpc.client.ServerProxy("http://{}:{}".format(ServerInfo['host'], ServerInfo["port"]))
                    ret = serverClient.dump()
                    if ret == DUMP_ERROR:
                        raise
        except Exception as e:
            print("INFO: ERROR when dumping server: {}".format(e))
            return PERSISTENCE_ERROR
        return PERSISTENCE_SUCCESS
    
    def ping(self):
        return 0

def main():
    global master_host
    global master_port
    global zk
    global group_infos
    print("INFO: I am the primary master now!")
    master_setup()
    get_servers()
    with ThreadXMLRPCServer((master_host, master_port)) as server:
        server.register_multicall_functions()
        server.register_instance(masterRPC())
        print("INFO: Master booted on http://{}:{}".format(master_host, master_port))
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            zk.stop()
            print("\nKeyboard interrupt received, exiting.")
            exit(0)

if __name__ == "__main__":
    print_loop_flag = True
    while True:
        try:
            zk.create("/GroupMember/MasterExists", "master".encode(), ephemeral=True)
            break
        except:
            pass
        if print_loop_flag:
            print("INFO: Another master is on work. Looping as backups")
            print_loop_flag = False
        time.sleep(0.1)
    zk.get_children("/GroupMember", watch=get_servers)
    main()