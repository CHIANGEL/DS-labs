import xmlrpc.server, xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn
from optparse import OptionParser
from kazoo.client import KazooClient
from model import Model

class ThreadXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass

GET_ERROR = -1
PUT_ERROR = -1
PUT_SUCCESS = 0
DELETE_ERROR = -1
DELETE_SUCCCESS = 0

zk_host = "127.0.0.1"
zk_port = 2181
model = Model()

class serverRPC:
    def get(self, key):
        try:
            return model.get(key)
        except Exception as e:
            print("SERVER: GET ERROR - {}".format(str(e)))
            return GET_ERROR
    
    def put(self, key, value):
        try:
            model.put(key, value)
            return PUT_SUCCESS
        except Exception as e:
            print("SERVER: PUT ERROR - {}".format(str(e)))
            return PUT_ERROR

    def delete(self, key):
        try:
            model.delete(key)
            return DELETE_SUCCCESS
        except Exception as e:
            print("SERVER: DELETE ERROR - {}".format(str(e)))
            return DELETE_ERROR
    
    def ping(self):
        return 0

def register_zookeeper(GroupId, ServerId):
    zk = KazooClient(hosts=(zk_host + ":" + str(zk_port)))
    zk.start()
    zk.stop()

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
    with ThreadXMLRPCServer((options.host, options.port)) as server:
        server.register_multicall_functions()
        server.register_instance(serverRPC())
        print("SERVER: Server {}-{} booted on http://{}:{}".format(options.GroupId, options.ServerId, options.host, options.port))
        register_zookeeper(options.GroupId, options.ServerId)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("\nKeyboard interrupt received, exiting.")
            exit(0)
