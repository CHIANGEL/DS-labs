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
        print("LOCK_SERVER: Warning: kazoo LOST")
    elif state == KazooState.SUSPENDED:
        print("LOCK_SERVER: Warning: kazoo SUSPENDED")
    else:
        print("LOCK_SERVER: Warning: kazoo CONNECTED")

lock_server_host = "localhost"
lock_server_port = 8001
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

class LockServerRPC:
    def acquire_read_lock(self, key):
        lock_id = generate_lock_id()
        if lock_id == -1:
            return -1
        ReadLocks[lock_id] = zk.ReadLock("/lock/" + key, "READ:" + key)
        print("LOCK_SERVER: start acquiring read lock")
        ret = ReadLocks[lock_id].acquire()
        if ret == True:
            return lock_id
        else:
            return -1

    def release_read_lock(self, lock_id):
        print("LOCK_SERVER: releasing read lock: {}".format(lock_id))
        ReadLocks[lock_id].release()
        del ReadLocks[lock_id]
        LockIds[lock_id] = False
        return True
    
    def acquire_write_lock(self, key):
        lock_id = generate_lock_id()
        if lock_id == -1:
            return -1
        WriteLocks[lock_id] = zk.WriteLock("/lock/" + key, "WRITE:" + key)
        print("LOCK_SERVER: start acquiring write lock")
        if WriteLocks[lock_id].acquire() == True:
            return lock_id
        else:
            return -1

    def release_write_lock(self, lock_id):
        print("LOCK_SERVER: releasing write lock: {}".format(lock_id))
        WriteLocks[lock_id].release()
        del WriteLocks[lock_id]
        LockIds[lock_id] = False
        return True

    def ping(self):
        return 0

if __name__ == "__main__":
    with ThreadXMLRPCServer((lock_server_host, lock_server_port)) as server:
        server.register_multicall_functions()
        server.register_instance(LockServerRPC())
        print("LOCK_SERVER: Lock server booted on http://{}:{}".format(lock_server_host, lock_server_port))
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            zk.stop()
            print("\nKeyboard interrupt received, exiting.")
            exit(0)
