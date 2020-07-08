import xmlrpc.server, xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn
from optparse import OptionParser
from kazoo.client import KazooClient, KazooState
from model import Model
from distributed_hash_table import DHT
from lottery_algorithm import lottery
import pprint

class ThreadXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass

def zk_state_listener(state):
    if state == KazooState.LOST:
        print("MASTER: Warning: kazoo LOST")
    elif state == KazooState.SUSPENDED:
        print("MASTER: Warning: kazoo SUSPENDED")
    else:
        print("MASTER: Warning: kazoo CONNECTED")

GET_ERROR = -1
PUT_SUCCESS = 0
PUT_ERROR = -1
PUT_PROP_SUCCESS = 0
PUT_PROP_ERROR = -1
DELETE_SUCCESS = 0
DELETE_ERROR = -1
DELETE_PROP_SUCCESS = 0
DELETE_PROP_ERROR = -1
DUMP_SUCCESS = 0
DUMP_ERROR = -1

zk_host = "127.0.0.1"
zk_port = 2181
zk = KazooClient(hosts=(zk_host + ":" + str(zk_port)))
zk.start()
zk.add_listener(zk_state_listener)

host = ""
port = -1
GroupId = -1
ServerId = -1
peer_infos = []
group_infos = {}
model = None
hash_table = None

class serverRPC:
    def get(self, key):
        try:
            return model.get(key)
        except Exception as e:
            #print("SERVER: GET ERROR - {}".format(str(e)))
            return GET_ERROR
    
    def put(self, key, value):
        try:
            #print("SERVER: Start put operation on primary server")
            model.put(key, value)
        except Exception as e:
            #print("SERVER: PUT ERROR - {}".format(str(e)))
            return PUT_ERROR
        try:
            #print("SERVER: start put propagate to peer server: {}".format(peer_infos))
            for peer_id, peer_info in enumerate(peer_infos):
                #print("SERVER: Put propagate to peer server {}:{}".format(peer_info['host'], peer_info["port"]))
                serverClient = xmlrpc.client.ServerProxy("http://{}:{}".format(peer_info['host'], peer_info["port"]))
                ret = serverClient.put_propagate(key, value)
                if ret == PUT_PROP_ERROR:
                    raise
            return PUT_SUCCESS
        except Exception as e:
            #print("SERVER: PUT ERROR - {}".format(str(e)))
            # 这里crash，如果是primary写成功但是standby没有成功，那要不要把primary的值回滚？
            return PUT_ERROR
    
    def put_propagate(self, key, value):
        try:
            #print("SERVER: Standby server {}-{} receive put propagation".format(GroupId, ServerId))
            model.put(key, value)
            return PUT_PROP_SUCCESS
        except Exception as e:
            #print("SERVER: PUT PROP ERROR - {}".format(str(e)))
            return PUT_PROP_ERROR

    def delete(self, key):
        if key not in model.file_dict.data:
            return DELETE_SUCCESS
        try:
            #print("SERVER: Start delete operation on primary server")
            model.delete(key)
        except Exception as e:
            #print("SERVER: DELETE ERROR - {}".format(str(e)))
            return DELETE_ERROR
        try:
            #print("SERVER: start delete propagate to peer server: {}".format(peer_infos))
            for peer_id, peer_info in enumerate(peer_infos):
                #print("SERVER: Delete propagate to peer server {}:{}".format(peer_info['host'], peer_info["port"]))
                serverClient = xmlrpc.client.ServerProxy("http://{}:{}".format(peer_info['host'], peer_info["port"]))
                ret = serverClient.delete_propagate(key)
                if ret == DELETE_PROP_ERROR:
                    raise
            return DELETE_SUCCESS
        except Exception as e:
            #print("SERVER: DELETE ERROR - {}".format(str(e)))
            # 这里crash，说明是primary删除成功但是standby没有成功，那要不要把primary的值回滚？
            return DELETE_ERROR
    
    def delete_propagate(self, key):
        try:
            #print("SERVER: Standby server {}-{} receive delete propagation".format(GroupId, ServerId))
            model.delete(key)
            return DELETE_PROP_SUCCESS
        except Exception as e:
            #print("SERVER: DELETE PROP ERROR - {}".format(str(e)))
            return DELETE_PROP_ERROR

    def dump(self):
        try:
            #print("SERVER: Dumping server {}-{}".format(GroupId, ServerId))
            model.dump()
            return DUMP_SUCCESS
        except Exception as e:
            #print("SERVER: DUMP ERROR - {}".format(str(e)))
            return DUMP_ERROR

    def adjust(self):
        global zk
        global hash_table
        global GroupId
        global group_infos
        for key in list(model.file_dict.data.keys()):
            target_vnode, target_group_id = hash_table.get_node(key)
            if int(target_group_id) != int(GroupId):
                target_server_id = lottery(group_infos, target_group_id)
                ServerInfo = group_infos[target_group_id][target_server_id]
                #print("Transfer key {} to server {}-{} on {}:{}".format(key, target_group_id, target_server_id, ServerInfo['host'], ServerInfo["port"]))
                serverClient = xmlrpc.client.ServerProxy("http://{}:{}".format(ServerInfo['host'], ServerInfo["port"]))
                serverClient.put(key, model.file_dict.data[key])
                self.delete(key)
        return True

    def sync_send(self, target_server):
        serverClient = xmlrpc.client.ServerProxy(target_server)
        serverClient.sync_recv(str(model.file_dict.data))
        return True

    def sync_recv(self, dict_str):
        model.file_dict.data = eval(dict_str)
        return True

    def ping(self):
        return 0

def register_zookeeper(ServerInfo):
    global zk
    zk.ensure_path("/GroupMember")
    value = str(ServerInfo)
    try:
        zk_path = zk.create("/GroupMember/{}-{}".format(ServerInfo.GroupId, ServerInfo.ServerId), value.encode(), ephemeral=True, makepath=True)
        #print("SERVER: server {}-{} register on zk {}".format(GroupId, ServerId, zk_path))
    except:
        print("ERROR: server {}-{} already exists!".format(GroupId, ServerId, zk_path))

def get_peers(event=None):
    global zk
    global peer_infos
    global group_infos
    global hash_table
    global GroupId
    global ServerId
    # Update group_infos
    servers = zk.get_children('/GroupMember', watch=get_peers)
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
    group_infos = new_group_infos
    #print("INFO: Get available server infos:")
    pprint.pprint(group_infos)
    GroupNode = [str(group_id) for group_id in group_infos]
    hash_table = DHT(GroupNode)
    #print("INFO: Distributed hash table established:")
    #pprint.pprint(hash_table._node_dict)
    # Update peer_infos
    servers = zk.get_children('/GroupMember', watch=get_peers)
    servers = [item for item in servers if "Master" not in item]
    #print("INFO: Start updating peer infos")
    new_peer_infos = []
    for server in servers:
        group_id = int(server[:server.find("-")])
        server_id = int(server[server.find("-") + 1:])
        if group_id == GroupId and server_id != ServerId:
            data = zk.get('/GroupMember/{}'.format(server))[0]
            if data:
                server_info = eval(data.decode())
                new_peer_infos.append(server_info)
    peer_infos = new_peer_infos
    #print("INFO: Get available peer infos:")
    pprint.pprint(new_peer_infos)

if __name__ == "__main__":
    parser = OptionParser(
        usage="The storage server instance, should be called by master.py."
    )
    parser.add_option(
        "--host",
        metavar="host",
        type="string",
        help="server host")
    parser.add_option(
        "--port",
        metavar="port",
        type="int",
        help="server port")
    parser.add_option(
        "--GroupId",
        metavar="GroupId",
        type="int",
        help="server GroupId")
    parser.add_option(
        "--ServerId",
        metavar="ServerId",
        type="int",
        help="server ServerId in the group")
    parser.add_option(
        "--weight",
        metavar="weight",
        type="int",
        help="weight for load balance")
    (options, args) = parser.parse_args()
    GroupId = options.GroupId
    ServerId = options.ServerId
    host = options.host
    port = options.port
    model = Model(GroupId, ServerId)
    #print('SERVER: {}'.format(options))
    zk.get_children("/GroupMember", watch=get_peers)
    with ThreadXMLRPCServer((options.host, options.port)) as server:
        server.register_multicall_functions()
        server.register_instance(serverRPC())
        #print("SERVER: Server {}-{} booted on http://{}:{}".format(options.GroupId, options.ServerId, options.host, options.port))
        register_zookeeper(options)
        get_peers()
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            zk.stop()
            #print("\nKeyboard interrupt received, exiting.")
            exit(0)
