import xmlrpc.client
import time
import sys

GET_ERROR = -1
PUT_ERROR = -1
PUT_SUCCESS = 0
DELETE_ERROR = -1
DELETE_SUCCCESS = 0

master_host = "localhost"
master_port = 8000
    
def get(arg, masterClient):
    key = arg[1]
    print("> Acquiring ReadLock...")
    lock_id = masterClient.acquire_read_lock(key)
    if lock_id == -1:
        print("> Fail to acquire ReadLock")
        return
    else:
        print("> ReadLock acquired")
    target_server = masterClient.get(key)
    print("> Redirected to server: {}".format(target_server))
    serverClient = xmlrpc.client.ServerProxy(target_server)
    if serverClient.ping() != 0:
        print("> ERROR: can not capture pings from target server!")
        return
    ret = serverClient.get(key)
    if ret == GET_ERROR:
        print("> FAIL")
    else:
        print("> SUCCESS: {}".format(ret))
    print("> Releasing ReadLock...")
    ret = masterClient.release_read_lock(lock_id)
    print("> ReadLock released")

def put(arg, masterClient):
    key = arg[1]
    value = arg[2]
    print("> Acquiring WriteLock...")
    lock_id = masterClient.acquire_write_lock(key)
    if lock_id == -1:
        print("> Fail to acquire WriteLock")
        return
    else:
        print("> WriteLock acquired")
    target_server = masterClient.put(key)
    print("> Redirected to server: {}".format(target_server))
    serverClient = xmlrpc.client.ServerProxy(target_server)
    if serverClient.ping() != 0:
        print("> ERROR: can not capture pings from target server!")
        return
    ret = serverClient.put(key, value)
    if ret == PUT_SUCCESS:
        print("> SUCCESS")
    else:
        print("> FAIL")
    print("> Releasing WriteLock...")
    ret = masterClient.release_write_lock(lock_id)
    print("> WriteLock released")

def delete(arg, masterClient):
    key = arg[1]
    print("> Acquiring WriteLock...")
    lock_id = masterClient.acquire_write_lock(key)
    if lock_id == -1:
        print("> Fail to acquire WriteLock")
        return
    else:
        print("> WriteLock acquired")
    target_server = masterClient.delete(key)
    print("> Redirected to server: {}".format(target_server))
    serverClient = xmlrpc.client.ServerProxy(target_server)
    if serverClient.ping() != 0:
        print("> ERROR: can not capture pings from target server!")
        return
    ret = serverClient.delete(key)
    if ret == DELETE_SUCCCESS:
        print("> SUCCESS")
    else:
        print("> FAIL")
    print("> Releasing WriteLock...")
    ret = masterClient.release_write_lock(lock_id)
    print("> WriteLock released")

def acquire_read_lock(arg, masterClient):
    key = arg[1]
    print("> Acquiring ReadLock...")
    lock_id = masterClient.acquire_read_lock(key)
    if lock_id == -1:
        print("> Fail to acquire ReadLock")
    else:
        print("> ReadLock acquired: {}".format(lock_id))

def release_read_lock(arg, masterClient):
    lock_id = arg[1]
    print("> Releasing ReadLock...")
    ret = masterClient.release_read_lock(lock_id)
    print("> ReadLock released")

def acquire_write_lock(arg, masterClient):
    key = arg[1]
    print("> Acquiring WriteLock...")
    lock_id = masterClient.acquireacquire_write_lock(key)
    if lock_id == -1:
        print("> Fail to acquire WriteLock: {}".format(lock_id))
    else:
        print("> WriteLock acquired")

def release_write_lock(arg, masterClient):
    lock_id = arg[1]
    print("> Releasing WriteLock...")
    ret = masterClient.release_write_lock(lock_id)
    print("> WriteLock released")

command2func = {
    'get' : get,
    'put' : put,
    'delete' : delete,
    'acquire_read_lock': acquire_read_lock,
    'release_read_lock': release_read_lock,
    'acquire_write_lock': acquire_write_lock,
    'release_write_lock': release_write_lock,
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
    return 1

if __name__ == "__main__":
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

    allTime = time.time() - start
    print("Excute {} commands".format(commandCount))
    print("Time to execute all commands: {}".format(allTime))
    print('throughput is %f requests per second' % (commandCount / allTime))