import xmlrpc.client, xmlrpc.server
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn
from kazoo.client import KazooClient, KazooState
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
GroupInfos = [
    [
        {"host":"localhost", "port":9000, "state":"inactive", "proc":None},
        {"host":"localhost", "port":9001, "state":"inactive", "proc":None},
    ],
    [
        {"host":"localhost", "port":9100, "state":"inactive", "proc":None},
        {"host":"localhost", "port":9101, "state":"inactive", "proc":None},
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
ReadLocks = {}
WriteLocks = {}
LockNum = 1000
LockIds = [False for _ in range(LockNum)]
CHECK_INTERVAL = 0.1

def generate_lock_id():
    for i in range(LockNum):
        if LockIds[i] == False:
            LockIds[i] = True
            return i
    return -1

class masterRPC:
    def hash(self, key):
        return abs(hash(key)) % GroupNum

    def redirect(self, key):
        target_group_id = self.hash(key)
        target_server_id = primary_servers[target_group_id]
        ServerInfo = GroupInfos[target_group_id][target_server_id]
        print("MASTER: redirect client request to server {}-{} on {}:{}".format(target_group_id, target_server_id, ServerInfo['host'], ServerInfo["port"]))
        return ("http://" + ServerInfo['host']  + ":" +  str(ServerInfo["port"]))
    
    def acquire_read_lock(self, key):
        lock_id = generate_lock_id()
        if lock_id == -1:
            return -1
        ReadLocks[lock_id] = zk.ReadLock("/lock/" + key, "READ:" + key)
        print("MASTER: start acquiring lock")
        ret = ReadLocks[lock_id].acquire()
        print("MASTER: ret->{}".format(ret))
        if ret == True:
            return lock_id
        else:
            return -1

    def release_read_lock(self, lock_id):
        print("MASTER: releasing write lock: {}".format(lock_id))
        ReadLocks[lock_id].release()
        del ReadLocks[lock_id]
        LockIds[lock_id] = False
        return True
    
    def acquire_write_lock(self, key):
        lock_id = generate_lock_id()
        if lock_id == -1:
            return -1
        WriteLocks[lock_id] = zk.WriteLock("/lock/" + key, "WRITE:" + key)
        if WriteLocks[lock_id].acquire() == True:
            return lock_id
        else:
            return -1

    def release_write_lock(self, lock_id):
        WriteLocks[lock_id].release()
        del WriteLocks[lock_id]
        LockIds[lock_id] = False
        return True

    def get(self, key):
        target_server = self.redirect(key)
        return target_server

    def put(self, key):
        target_server = self.redirect(key)
        return target_server

    def delete(self, key):
        target_server = self.redirect(key)
        return target_server
    
    def ping(self):
        return 0

def bootServer():
    for GroupId, GroupInfo in enumerate(GroupInfos):
        for ServerId, ServerInfo in enumerate(GroupInfo):
            print("MASTER: Booting Server {}-{}".format(GroupId, ServerId))
            try:
                command = ["python", "server.py", 
                            "--host", ServerInfo['host'], 
                            "--port", str(ServerInfo["port"]),
                            "--GroupId", str(GroupId),
                            "--ServerId", str(ServerId)]
                GroupInfos[GroupId][ServerId]["proc"] = subprocess.Popen(command)
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
                        print("MASTER: Capture pings from server {}-{} in {} seconds".format(GroupId, ServerId, boot_time))
                        break
                except socket.error as err:
                    pass
                    '''if err.errno != errno.ECONNREFUSED:
                        raise err'''

if __name__ == "__main__":
    with ThreadXMLRPCServer((master_host, master_port)) as server:
        bootServer()
        server.register_multicall_functions()
        server.register_instance(masterRPC())
        print("MASTER: Master booted on http://{}:{}".format(master_host, master_port))
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            zk.stop()
            print("\nKeyboard interrupt received, exiting.")
            exit(0)
