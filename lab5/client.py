from kazoo.client import KazooClient, KazooState
import xmlrpc.client
import time
import sys

GET_ERROR = -1
PUT_SUCCESS = 0
PUT_ERROR = -1
DELETE_SUCCCESS = 0
DELETE_ERROR = -1
PERSISTENCE_SUCCESS = 0
PERSISTENCE_ERROR = -1

def zk_state_listener(state):
    if state == KazooState.LOST:
        print("INFO: Warning: kazoo LOST")
    elif state == KazooState.SUSPENDED:
        print("INFO: Warning: kazoo SUSPENDED")
    else:
        print("INFO: Warning: kazoo CONNECTED")

zk_host = "localhost"
zk_port = 2181
zk = KazooClient(hosts=(zk_host + ":" + str(zk_port)))
zk.start()
zk.add_listener(zk_state_listener)

master_host = ""
master_port = -1
    
def get(arg, masterClient):
    global zk
    key = arg[1]
    print("> Acquiring System-level ReadLock...")
    system_read_lock = zk.ReadLock("/SystemLock")
    ret = system_read_lock.acquire()
    if ret == True:
        print("> System-level ReadLock acquired")
    else:
        print("> Fail to acquire System-level ReadLock")
        return
    print("> Acquiring ReadLock...")
    read_lock = zk.ReadLock("/lock/{}".format(key))
    ret = read_lock.acquire()
    if ret == True:
        print("> ReadLock acquired")
    else:
        print("> Fail to acquire ReadLock")
        return
    target_server = masterClient.get(key)
    print("> Redirected to server: {}".format(target_server))
    serverClient = xmlrpc.client.ServerProxy(target_server)
    try:
        if serverClient.ping() != 0:
            print("> ERROR: can not capture pings from target server!")
            return
        ret = serverClient.get(key)
    except:
        print("> Target server has crashed! Please retry!")
        return
    if ret == GET_ERROR:
        print("> FAIL")
    else:
        print("> SUCCESS: {}".format(ret))
    print("> Releasing ReadLock...")
    read_lock.release()
    print("> ReadLock released")
    print("> Releasing System-level ReadLock...")
    system_read_lock.release()
    print("> System-level ReadLock released")

def put(arg, masterClient):
    global zk
    key = arg[1]
    value = arg[2]
    print("> Acquiring System-level ReadLock...")
    system_read_lock = zk.ReadLock("/SystemLock")
    ret = system_read_lock.acquire()
    if ret == True:
        print("> System-level ReadLock acquired")
    else:
        print("> Fail to acquire System-level ReadLock")
        return
    print("> Acquiring WriteLock...")
    write_lock = zk.WriteLock("/lock/{}".format(key))
    ret = write_lock.acquire()
    if ret == True:
        print("> WriteLock acquired")
    else:
        print("> Fail to acquire WriteLock")
        return
    target_server = masterClient.put(key)
    print("> Redirected to server: {}".format(target_server))
    serverClient = xmlrpc.client.ServerProxy(target_server)
    try:
        if serverClient.ping() != 0:
            print("> ERROR: can not capture pings from target server!")
            return
        ret = serverClient.put(key, value)
    except:
        print("> Target server has crashed! Please retry!")
        return
    if ret == PUT_SUCCESS:
        print("> SUCCESS")
    else:
        print("> FAIL")
    print("> Releasing WriteLock...")
    write_lock.release()
    print("> WriteLock released")
    print("> Releasing System-level ReadLock...")
    system_read_lock.release()
    print("> System-level ReadLock released")

def delete(arg, masterClient):
    global zk
    key = arg[1]
    print("> Acquiring System-level ReadLock...")
    system_read_lock = zk.ReadLock("/SystemLock")
    ret = system_read_lock.acquire()
    if ret == True:
        print("> System-level ReadLock acquired")
    else:
        print("> Fail to acquire System-level ReadLock")
        return
    print("> Acquiring WriteLock...")
    write_lock = zk.WriteLock("/lock/{}".format(key))
    ret = write_lock.acquire()
    if ret == True:
        print("> WriteLock acquired")
    else:
        print("> Fail to acquire WriteLock")
        return
    target_server = masterClient.delete(key)
    print("> Redirected to server: {}".format(target_server))
    serverClient = xmlrpc.client.ServerProxy(target_server)
    try:
        if serverClient.ping() != 0:
            print("> ERROR: can not capture pings from target server!")
            return
        ret = serverClient.delete(key)
    except:
        print("> FAIL: Target server has crashed! Please retry!")
        return
    if ret == DELETE_SUCCCESS:
        print("> SUCCESS")
    else:
        print("> FAIL")
    print("> Releasing WriteLock...")
    write_lock.release()
    print("> WriteLock released")
    print("> Releasing System-level ReadLock...")
    system_read_lock.release()
    print("> System-level ReadLock released")

def make_persistence(arg, masterClient):
    global zk
    print("> Acquiring System-level WriteLock...")
    system_write_lock = zk.WriteLock("/SystemLock")
    ret = system_write_lock.acquire()
    if ret == True:
        print("> System-level WriteLock acquired")
    else:
        print("> Fail to acquire System-level WriteLock")
        return
    print("> Making data persistent...")
    ret = masterClient.make_persistence()
    if ret == 0:
        print("> Success!")
    else:
        print("> Fail to make data persistent. Please retry.")
    print("> Releasing System-level WriteLock...")
    system_write_lock.release()
    print("> System-level WriteLock released")

command2func = {
    'get' : get,
    'put' : put,
    'delete' : delete,
    'make_persistence': make_persistence,
}

def CheckArgs(arg):
    if arg[0] not in command2func:
        print("> ERROR: unsupported command: {}".format(arg[0]))
        return 0
    if arg[0] == "get" and len(arg) != 2:
        print("> ERROR: expect {} arguments but receive {} arguments".format(1, len(arg) - 1))
        return 0
    if arg[0] == "put" and len(arg) != 3:
        print("> ERROR: expect {} arguments but receive {} arguments".format(2, len(arg) - 1))
        return 0
    if arg[0] == "delete" and len(arg) != 2:
        print("> ERROR: expect {} arguments but receive {} arguments".format(1, len(arg) - 1))
        return 0
    if arg[0] == "make_persistence" and len(arg) != 1:
        print("> ERROR: expect {} arguments but receive {} arguments".format(0, len(arg) - 1))
        return 0
    return 1

def get_master_infos():
    global master_host
    global master_port
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

if __name__ == "__main__":
    get_master_infos()
    masterClient = xmlrpc.client.ServerProxy(("http://" + master_host + ":" + str(master_port)))
    if masterClient.ping() != 0:
        print("ERROR: can not capture pings from master!")
        exit()
    
    start = time.time()
    commandCount = 0
    while True:
        input = sys.stdin.readline().strip('\n')
        if input.startswith("#"):
            break
        arg = input.split(' ')
        if CheckArgs(arg) == 0:
            continue
        func = command2func[arg[0]]
        func(arg, masterClient)
        commandCount += 1

    zk.stop()
    allTime = time.time() - start
    print("Excute {} commands".format(commandCount))
    print("Time to execute all commands: {}".format(allTime))
    print('throughput is %f requests per second' % (commandCount / allTime))