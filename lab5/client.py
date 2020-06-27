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

master_host = "localhost"
master_port = 8000
lock_server_host = "localhost"
lock_server_port = 8001
    
def get(arg, masterClient, lockClient):
    key = arg[1]
    print("> Acquiring System-level ReadLock...")
    system_lock_id = lockClient.acquire_system_read_lock()
    if system_lock_id == -1:
        print("> Fail to acquire System-level ReadLock")
        return
    else:
        print("> System-level ReadLock acquired")
    print("> Acquiring ReadLock...")
    lock_id = lockClient.acquire_read_lock(key)
    if lock_id == -1:
        print("> Fail to acquire ReadLock")
        return
    else:
        print("> ReadLock acquired")
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
    ret = lockClient.release_read_lock(lock_id)
    print("> ReadLock released")
    print("> Releasing System-level ReadLock...")
    ret = lockClient.release_system_read_lock(system_lock_id)
    print("> System-level ReadLock released")

def put(arg, masterClient, lockClient):
    key = arg[1]
    value = arg[2]
    print("> Acquiring System-level ReadLock...")
    system_lock_id = lockClient.acquire_system_read_lock()
    if system_lock_id == -1:
        print("> Fail to acquire System-level ReadLock")
        return
    else:
        print("> System-level ReadLock acquired")
    print("> Acquiring WriteLock...")
    lock_id = lockClient.acquire_write_lock(key)
    if lock_id == -1:
        print("> Fail to acquire WriteLock")
        return
    else:
        print("> WriteLock acquired")
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
    ret = lockClient.release_write_lock(lock_id)
    print("> WriteLock released")
    print("> Releasing System-level ReadLock...")
    ret = lockClient.release_system_read_lock(system_lock_id)
    print("> System-level ReadLock released")

def delete(arg, masterClient, lockClient):
    key = arg[1]
    print("> Acquiring System-level ReadLock...")
    system_lock_id = lockClient.acquire_system_read_lock()
    if system_lock_id == -1:
        print("> Fail to acquire System-level ReadLock")
        return
    else:
        print("> System-level ReadLock acquired")
    print("> Acquiring WriteLock...")
    lock_id = lockClient.acquire_write_lock(key)
    if lock_id == -1:
        print("> Fail to acquire WriteLock")
        return
    else:
        print("> WriteLock acquired")
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
    ret = lockClient.release_write_lock(lock_id)
    print("> WriteLock released")
    print("> Releasing System-level ReadLock...")
    ret = lockClient.release_system_read_lock(system_lock_id)
    print("> System-level ReadLock released")

def acquire_read_lock(arg, masterClient, lockClient):
    key = arg[1]
    print("> Acquiring ReadLock...")
    lock_id = lockClient.acquire_read_lock(key)
    if lock_id == -1:
        print("> Fail to acquire ReadLock")
    else:
        print("> ReadLock acquired: {}".format(lock_id))

def release_read_lock(arg, masterClient, lockClient):
    lock_id = arg[1]
    print("> Releasing ReadLock...")
    ret = lockClient.release_read_lock(lock_id)
    print("> ReadLock released")

def acquire_write_lock(arg, masterClient, lockClient):
    key = arg[1]
    print("> Acquiring WriteLock...")
    lock_id = lockClient.acquireacquire_write_lock(key)
    if lock_id == -1:
        print("> Fail to acquire WriteLock: {}".format(lock_id))
    else:
        print("> WriteLock acquired")

def release_write_lock(arg, masterClient, lockClient):
    lock_id = arg[1]
    print("> Releasing WriteLock...")
    ret = lockClient.release_write_lock(lock_id)
    print("> WriteLock released")

def make_persistence(arg, masterClient, lockClient):
    print("> Acquiring System-level WriteLock...")
    system_lock_id = lockClient.acquire_system_write_lock()
    if system_lock_id == -1:
        print("> Fail to acquire System-level WriteLock")
        return
    else:
        print("> System-level WriteLock acquired")
    print("> Making data persistent...")
    ret = masterClient.make_persistence()
    if ret == 0:
        print("> Success!")
    else:
        print("> Fail to make data persistent. Please retry.")
    print("> Releasing System-level WriteLock...")
    ret = lockClient.release_system_write_lock(system_lock_id)
    print("> System-level WriteLock released")

command2func = {
    'get' : get,
    'put' : put,
    'delete' : delete,
    'acquire_read_lock': acquire_read_lock,
    'release_read_lock': release_read_lock,
    'acquire_write_lock': acquire_write_lock,
    'release_write_lock': release_write_lock,
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

if __name__ == "__main__":
    masterClient = xmlrpc.client.ServerProxy(("http://" + master_host + ":" + str(master_port)))
    if masterClient.ping() != 0:
        print("ERROR: can not capture pings from master!")
        exit()
    lockClient = xmlrpc.client.ServerProxy(("http://" + lock_server_host + ":" + str(lock_server_port)))
    if lockClient.ping() != 0:
        print("ERROR: can not capture pings from lock server!")
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
        func(arg, masterClient, lockClient)
        commandCount += 1

    allTime = time.time() - start
    print("Excute {} commands".format(commandCount))
    print("Time to execute all commands: {}".format(allTime))
    print('throughput is %f requests per second' % (commandCount / allTime))