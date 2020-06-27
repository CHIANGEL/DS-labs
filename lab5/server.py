import xmlrpc.server, xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn
from optparse import OptionParser
from kazoo.client import KazooClient, KazooState
from model import Model

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
PeerInfos = []
model = None

class serverRPC:
    def get(self, key):
        try:
            return model.get(key)
        except Exception as e:
            print("SERVER: GET ERROR - {}".format(str(e)))
            return GET_ERROR
    
    def put(self, key, value):
        try:
            print("SERVER: Start put operation on primary server")
            model.put(key, value)
        except Exception as e:
            print("SERVER: PUT ERROR - {}".format(str(e)))
            return PUT_ERROR
        try:
            print("SERVER: start put propagate to peer server: {}".format(PeerInfos))
            for peer_id, peer_info in enumerate(PeerInfos):
                if peer_info["state"] == "active":
                    print("SERVER: Put propagate to peer server {}:{}".format(peer_info['host'], peer_info["port"]))
                    serverClient = xmlrpc.client.ServerProxy("http://" + peer_info['host']  + ":" +  str(peer_info["port"]))
                    ret = serverClient.put_propagate(key, value)
                    if ret == PUT_PROP_ERROR:
                        raise
            return PUT_SUCCESS
        except Exception as e:
            print("SERVER: PUT ERROR - {}".format(str(e)))
            # 这里crash，如果是primary写成功但是standby没有成功，那要不要把primary的值回滚？
            return PUT_ERROR
    
    def put_propagate(self, key, value):
        try:
            print("SERVER: Standby server {}-{} receive put propagation".format(GroupId, ServerId))
            model.put(key, value)
            return PUT_PROP_SUCCESS
        except Exception as e:
            print("SERVER: PUT PROP ERROR - {}".format(str(e)))
            return PUT_PROP_ERROR

    def delete(self, key):
        try:
            print("SERVER: Start delete operation on primary server")
            model.delete(key)
        except Exception as e:
            print("SERVER: DELETE ERROR - {}".format(str(e)))
            return DELETE_ERROR
        try:
            print("SERVER: start delete propagate to peer server: {}".format(PeerInfos))
            for peer_id, peer_info in enumerate(PeerInfos):
                if peer_info["state"] == "active":
                    print("SERVER: Delete propagate to peer server {}:{}".format(peer_info['host'], peer_info["port"]))
                    serverClient = xmlrpc.client.ServerProxy("http://" + peer_info['host']  + ":" +  str(peer_info["port"]))
                    ret = serverClient.delete_propagate(key)
                    if ret == DELETE_PROP_ERROR:
                        raise
            return DELETE_SUCCESS
        except Exception as e:
            print("SERVER: DELETE ERROR - {}".format(str(e)))
            # 这里crash，如果是primary删除成功但是standby没有成功，那要不要把primary的值回滚？
            return DELETE_ERROR
    
    def delete_propagate(self, key):
        try:
            print("SERVER: Standby server {}-{} receive delete propagation".format(GroupId, ServerId))
            model.delete(key)
            return DELETE_PROP_SUCCESS
        except Exception as e:
            print("SERVER: DELETE PROP ERROR - {}".format(str(e)))
            return DELETE_PROP_ERROR
        
    def update_peer(self, peer_infos):
        global PeerInfos
        PeerInfos = eval(peer_infos)
        print("SERVER: peer info of server {}-{}: {}".format(GroupId, ServerId, PeerInfos))
        return True

    def dump(self):
        try:
            print("SERVER: Dumping server {}-{}".format(GroupId, ServerId))
            model.dump()
            return DUMP_SUCCESS
        except Exception as e:
            print("SERVER: DUMP ERROR - {}".format(str(e)))
            return DUMP_ERROR
    
    def ping(self):
        return 0

def register_zookeeper(GroupId, ServerId):
    zk.ensure_path("/GroupMember")
    value = str(ServerId)
    zk_path = zk.create("/GroupMember/Group-{}/Server".format(GroupId), value.encode(), ephemeral=True, sequence=True, makepath=True)
    print("SERVER: server {}-{} register on zk {}".format(GroupId, ServerId, zk_path))

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
    (options, args) = parser.parse_args()
    GroupId = options.GroupId
    ServerId = options.ServerId
    host = options.host
    port = options.port
    model = Model(GroupId, ServerId)
    with ThreadXMLRPCServer((options.host, options.port)) as server:
        server.register_multicall_functions()
        server.register_instance(serverRPC())
        print("SERVER: Server {}-{} booted on http://{}:{}".format(options.GroupId, options.ServerId, options.host, options.port))
        register_zookeeper(options.GroupId, options.ServerId)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            zk.stop()
            print("\nKeyboard interrupt received, exiting.")
            exit(0)
