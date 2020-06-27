import xmlrpc.client, xmlrpc.server
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn
from kazoo.client import KazooClient, KazooState
from kazoo.protocol.states import EventType
import subprocess
import socket
import errno
import time

class ThreadXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass

def zk_state_listener(state):
    if state == KazooState.LOST:
        print("MASTER: Warning: kazoo LOST")
    elif state == KazooState.SUSPENDED:
        print("MASTER: Warning: kazoo SUSPENDED")
    else:
        print("MASTER: Warning: kazoo CONNECTED")

GroupNum = 2
server_process = [
    [None, None],
    [None, None]
]
GroupInfos = [
    [
        {"host":"localhost", "port":9000, "state":"inactive"},
        {"host":"localhost", "port":9001, "state":"inactive"},
    ],
    [
        {"host":"localhost", "port":9100, "state":"inactive"},
        {"host":"localhost", "port":9101, "state":"inactive"},
    ],
]
assert GroupNum == len(GroupInfos)
primary_servers = [0 for _ in range(GroupNum)]
master_host = "localhost"
master_port = 8000
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

class masterRPC:
    def hash(self, key):
        return abs(hash(key)) % GroupNum

    def redirect(self, key):
        target_group_id = self.hash(key)
        target_server_id = primary_servers[target_group_id]
        ServerInfo = GroupInfos[target_group_id][target_server_id]
        print("MASTER: redirect client request to server {}-{} on {}:{}".format(target_group_id, target_server_id, ServerInfo['host'], ServerInfo["port"]))
        return ("http://" + ServerInfo['host']  + ":" +  str(ServerInfo["port"]))

    def get(self, key):
        target_server = self.redirect(key)
        return target_server

    def put(self, key):
        target_server = self.redirect(key)
        return target_server

    def delete(self, key):
        target_server = self.redirect(key)
        return target_server

    def make_persistence(self):
        print("MASTER: sending dump command to currently active server")
        try:
            for GroupId, GroupInfo in enumerate(GroupInfos):
                for ServerId, ServerInfo in enumerate(GroupInfo):
                    if GroupInfos[GroupId][ServerId]['state'] == "active":
                        print("MASTER: Dumping server {}-{}".format(GroupId, ServerId))
                        serverClient = xmlrpc.client.ServerProxy("http://" + ServerInfo['host']  + ":" +  str(ServerInfo["port"]))
                        ret = serverClient.dump()
                        if ret == DUMP_ERROR:
                            raise
        except Exception as e:
            print("MASTER: ERROR when dumping server: {}".format(e))
            return PERSISTENCE_ERROR
        return PERSISTENCE_SUCCESS
    
    def ping(self):
        return 0

def updateGroupPeer(GroupId, GroupInfo):
    print("MASTER: start updating peer info of group {}".format(GroupId))
    for ServerId, ServerInfo in enumerate(GroupInfo):
        if GroupInfos[GroupId][ServerId]['state'] == "active":
            peer_info = (GroupInfo[:ServerId] + GroupInfo[ServerId + 1:])
            serverClient = xmlrpc.client.ServerProxy("http://" + ServerInfo['host']  + ":" +  str(ServerInfo["port"]))
            serverClient.update_peer(str(peer_info))

def bootServer(GroupId, GroupInfo, ServerId, ServerInfo):
    print("MASTER: Booting Server {}-{}".format(GroupId, ServerId))
    try:
        command = ["python", "server.py", 
                    "--host", ServerInfo['host'], 
                    "--port", str(ServerInfo["port"]),
                    "--GroupId", str(GroupId),
                    "--ServerId", str(ServerId)]
        server_process[GroupId][ServerId] = subprocess.Popen(command)
    except Exception as e:
        print("MASTER: ERROR when booting server {}-{}: {}".format(GroupId, ServerId, e))
        exit()
    boot_time = 0
    tmpClient = xmlrpc.client.ServerProxy("http://" + ServerInfo['host']  + ":" +  str(ServerInfo["port"]))
    while True:
        time.sleep(CHECK_INTERVAL)
        boot_time += CHECK_INTERVAL
        try:
            if tmpClient.ping() == 0:
                print("MASTER: Capture ping from server {}-{} in {} seconds".format(GroupId, ServerId, boot_time))
                GroupInfos[GroupId][ServerId]["state"] = "active"
                break
        except socket.error as err:
            if err.errno != errno.ECONNREFUSED:
                raise err

def server_crash_handler(event):
    if event.type == EventType.CHILD:
        pathStr = event.path
        crash_group_id = int(pathStr[pathStr.find("-") + 1:])
        print("MASTER: Server crash in group {}".format(crash_group_id))
        for ServerId, ServerInfo in enumerate(GroupInfos[crash_group_id]):
            GroupInfos[crash_group_id][ServerId]['state'] = "inactive"
        children = zk.get_children(pathStr)
        for idx, node_name in enumerate(children):
            node = zk.get("{}/{}".format(pathStr, node_name))
            alive_server_id = int(node[0].decode())
            GroupInfos[crash_group_id][alive_server_id]['state'] = "active"
            primary_servers[crash_group_id] = alive_server_id
            print("MASTER: Server {}-{} still alive".format(crash_group_id, alive_server_id))
        updateGroupPeer(crash_group_id, GroupInfos[crash_group_id])

if __name__ == "__main__":
    with ThreadXMLRPCServer((master_host, master_port)) as server:
        for GroupId, GroupInfo in enumerate(GroupInfos):
            for ServerId, ServerInfo in enumerate(GroupInfo):
                bootServer(GroupId, GroupInfo, ServerId, ServerInfo)
            updateGroupPeer(GroupId, GroupInfo)
            zk.get_children("/GroupMember/Group-{}".format(GroupId), watch=server_crash_handler)
        server.register_multicall_functions()
        server.register_instance(masterRPC())
        print("MASTER: Master booted on http://{}:{}".format(master_host, master_port))
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            zk.stop()
            print("\nKeyboard interrupt received, exiting.")
            exit(0)
