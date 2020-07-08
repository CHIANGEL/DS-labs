import xmlrpc.client, xmlrpc.server
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn
from kazoo.client import KazooClient, KazooState
from kazoo.protocol.states import EventType
import kazoo.exceptions
from distributed_hash_table import DHT
from lottery_algorithm import lottery
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
hash_table = None

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
ADJUST_ERROR = -1
ADJUST_SUCCESS = 0

random.seed(time.time())

def get_servers(event=None):
    global zk
    global hash_table
    global group_infos
    servers = zk.get_children('/GroupMember', watch=get_servers)
    servers = [item for item in servers if "Master" not in item]
    #print("INFO: Watch event caught in /GroupMember! Start updating server infos")
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
    #print("INFO: Get available new server infos:")
    pprint.pprint(new_group_infos)
    GroupNode = [str(group_id) for group_id in new_group_infos]
    hash_table = DHT(GroupNode)
    #print("INFO: Distributed hash table established:")
    pprint.pprint(hash_table._sort_list)
    pprint.pprint(hash_table._node_dict)
    set_new_group_infos = set([group_id for group_id in new_group_infos])
    set_group_infos = set([group_id for group_id in group_infos])
    if event == None:
        # Master Init. Just update group_infos
        #print("INFO: Master init")
        system_write_lock = zk.WriteLock("/SystemLock")
        #print("INFO: Acquiring system write lock")
        system_write_lock.acquire()
        #print("INFO: System write lock acquired")
        for group_id in new_group_infos:
            server_info = new_group_infos[group_id][0]
            #print("INFO: Data transfer operations on Server {}:{}".format(server_info['host'], server_info["port"]))
            serverClient = xmlrpc.client.ServerProxy("http://{}:{}".format(server_info['host'], server_info["port"]))
            ret = serverClient.adjust()
            if ret == ADJUST_ERROR:
                raise
        group_infos = new_group_infos
        system_write_lock.release()
        #print("INFO: System write lock released")
    elif set_group_infos.issubset(set_new_group_infos) == False:
        # Some group totally crush! Can not provide service any more!
        #print(set_group_infos)
        #print(set_new_group_infos)
        #print("ERROR: Some group totally crush! Can not provide service any more!")
        zk.create("/ServiceStop")
        exit(0)
    elif len(set_group_infos) < len(set_new_group_infos):
        # New groups are added. Need to block out to transfer data
        #print("INFO: New groups are added. Need to block out to transfer data")
        system_write_lock = zk.WriteLock("/SystemLock")
        #print("INFO: Acquiring system write lock")
        system_write_lock.acquire()
        #print("INFO: System write lock acquired")
        for group_id in new_group_infos:
            server_info = new_group_infos[group_id][0]
            #print("INFO: Data transfer operations on Server {}:{}".format(server_info['host'], server_info["port"]))
            serverClient = xmlrpc.client.ServerProxy("http://{}:{}".format(server_info['host'], server_info["port"]))
            ret = serverClient.adjust()
            if ret == ADJUST_ERROR:
                raise
        group_infos = new_group_infos
        system_write_lock.release()
        #print("INFO: System write lock released")
    else:
        # Seek for new standbys
        #print("INFO: Seek for new standbys")
        system_write_lock = zk.WriteLock("/SystemLock")
        #print("INFO: Acquiring system write lock")
        system_write_lock.acquire()
        #print("INFO: System write lock acquired")
        for group_id in new_group_infos:
            server_infos = new_group_infos[group_id]
            if len(new_group_infos[group_id]) > len(group_infos[group_id]):
                # New standby, need to sync data
                source_server = group_infos[group_id][0]
                #print("INFO: Sync data source from Server {}:{}".format(source_server['host'], source_server["port"]))
                serverClient = xmlrpc.client.ServerProxy("http://{}:{}".format(source_server['host'], source_server["port"]))
                for server_info in server_infos:
                    if server_info not in group_infos[group_id]:
                        serverClient.sync_send("http://{}:{}".format(server_info['host'], server_info["port"]))
        group_infos = new_group_infos
        system_write_lock.release()
        #print("INFO: System write lock released")

def master_setup():
    global master_host
    global master_port
    global zk
    global group_infos
    #print("INFO: Start getting master infos")
    data = zk.get('/GroupMember/{}'.format("MasterHost"))[0]
    if data:
        master_host = data.decode()
    else:
        #print("ERROR: Fail to get master port from zookeeper!")
        raise
    data = zk.get('/GroupMember/{}'.format("MasterPort"))[0]
    if data:
        master_port = int(data.decode())
    else:
        #print("ERROR: Fail to get master port from zookeeper!")
        raise
    #print("INFO: Get master addr: {}:{}".format(master_host, master_port))

class masterRPC:
    def hash(self, key):
        global hash_table
        target_vnode, target_group_id = hash_table.get_node(key)
        return int(target_group_id)

    def read_redirect(self, key):
        global zk
        global group_infos
        target_group_id = self.hash(key)
        target_server_id = lottery(group_infos, target_group_id)
        ServerInfo = group_infos[target_group_id][target_server_id]
        #print("INFO: redirect client request to server {}-{} on {}:{}".format(target_group_id, target_server_id, ServerInfo['host'], ServerInfo["port"]))
        return ("http://" + ServerInfo['host']  + ":" +  str(ServerInfo["port"]))

    def write_redirect(self, key):
        global zk
        global group_infos
        target_group_id = self.hash(key)
        target_server_id = lottery(group_infos, target_group_id)
        ServerInfo = group_infos[target_group_id][target_server_id]
        #print("INFO: redirect client request to server {}-{} on {}:{}".format(target_group_id, target_server_id, ServerInfo['host'], ServerInfo["port"]))
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
        #print("INFO: sending dump command to currently active server")
        try:
            for group_id in group_infos:
                group_info = group_infos[group_id]
                for ServerId, ServerInfo in enumerate(group_info):
                    #print("INFO: Dumping server {}-{}".format(group_info, ServerId))
                    serverClient = xmlrpc.client.ServerProxy("http://{}:{}".format(ServerInfo['host'], ServerInfo["port"]))
                    ret = serverClient.dump()
                    if ret == DUMP_ERROR:
                        raise
        except Exception as e:
            #print("INFO: ERROR when dumping server: {}".format(e))
            return PERSISTENCE_ERROR
        return PERSISTENCE_SUCCESS
    
    def ping(self):
        return 0

def main():
    global master_host
    global master_port
    global zk
    global group_infos
    #print("INFO: I am the primary master now!")
    master_setup()
    get_servers()
    with ThreadXMLRPCServer((master_host, master_port)) as server:
        server.register_multicall_functions()
        server.register_instance(masterRPC())
        #print("INFO: Master booted on http://{}:{}".format(master_host, master_port))
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            zk.stop()
            #print("\nKeyboard interrupt received, exiting.")
            exit(0)

if __name__ == "__main__":
    print_loop_flag = True
    while True:
        try:
            zk.create("/Master/MasterExists", "master".encode(), ephemeral=True, makepath=True)
            break
        except:
            pass
        if print_loop_flag:
            #print("INFO: Another master is on work. Looping as backups")
            print_loop_flag = False
        time.sleep(0.1)
    if zk.exists("/ServiceStop"):
        #print("ERROR: Some group totally crush! Can not provide service any more!")
        exit(0)
    zk.get_children("/GroupMember", watch=get_servers)
    main()