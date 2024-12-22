from app import parser, utils, io
from threading import Timer
import socket, os
class Server:
    DEFAULT_PORT = 6379
    EMPTY_RDB_FILE = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
    PROPAGATE_LIST = ['SET', 'DEL']
    COMMANDS = ['PING', 'ECHO', 'SET', 'GET', 'REPLCONF', 'PSYNC', 'KEYS', 'INFO', 'CONFIG', 'WAIT']

    def __init__(self, **kwargs):
        self.master = True
        self.socket = None
        self.master_socket = None

        self._master_port = -1
        self._master_hostname = None
        self._cache = {}
        self._port = Server.DEFAULT_PORT
        self._rdb_snapshot = None
        self._connections = []
        self._parsed_bytes = -1
        self._handshake_done = False
        self._parse_args(**kwargs)

    def _parse_args(self, **kwargs):
        for key, val in kwargs.items():
            print(key,val)
            self._cache[key] = val
        if self._cache['dir'] and self._cache['dbfilename']:
            self._get_db_image()
        if self._cache['port']:
            self._port = int(self._cache['port'])

        self.socket = socket.create_server(("localhost", self._port), reuse_port=True, backlog=5)
        if self._cache['replicaof']:
            self.master = False
            master_info = self._cache['replicaof'].split()
            self._master_hostname = str(master_info[0])
            self._master_port = int(master_info[1])
            self.master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.master_socket.connect((self._master_hostname, self._master_port))
            self._handshake_slave()

    def _handshake_master(self):
        print("----HANDSHAKE FROM MASTER----")
        #TODO: add state control for handshake

    def _handshake_slave(self):
        print("----HANDSHAKE FROM SLAVE----")
        # PING
        self.master_socket.send("*1\r\n$4\r\nPING\r\n".encode())
        resp = self.master_socket.recv(1024)
        print(resp)
        # REPLCONF 1
        self.master_socket.send(
                f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${len(str(self._port))}\r\n{self._port}\r\n".encode()
            )
        resp = self.master_socket.recv(1024)
        print(resp)
        # REPLCONF 2
        self.master_socket.send(f"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".encode())
        resp = self.master_socket.recv(1024)
        print(resp)
        # PSYNC
        self.master_socket.send(f"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".encode())
        resp = self.master_socket.recv(1024)
        self._handshake_done = True
        print(resp)
        self._parse_data(self.master_socket, resp)

        if self.master_socket not in self._connections:
            self._connections.append(self.master_socket)
        return

    def _get_db_image(self):
        rdb_path = os.path.join(self._cache['dir'], self._cache['dbfilename'])
        print(f"[_get_db_image] {rdb_path}")
        self._rdb_snapshot = io.RDB(rdb_path)

    def _delete_key(self, *args):
        key = ''.join(args)
        print(f"[_delete_key] key={key}")
        if key in self._cache:
            del self._cache[key]

    def _execute_ping(self, client):
        if client is not self.master_socket:
            client.send(b"+PONG\r\n")
        if client is self.master_socket and not self._handshake_done:
            client.send(b"+PONG\r\n")

    def _execute_replconf(self, client, cmd):
        if cmd[1].lower() == "capa" or cmd[1].lower() == "listening-port":
            client.send(b"+OK\r\n")
        elif cmd[1].upper() == "GETACK":
            if self._parsed_bytes == -1:
                self._parsed_bytes = 0
            client.send(
                f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(self._parsed_bytes))}\r\n{str(self._parsed_bytes)}\r\n" \
                .encode()
                )

    def _execute_psync(self, client):
        repl_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        client.send(f"+FULLRESYNC {repl_id} 0\r\n".encode())
        rdb_file = bytes.fromhex(Server.EMPTY_RDB_FILE)
        client.send(f"${len(rdb_file)}\r\n".encode() + rdb_file)
        self._handshake_done = True
        # Avoid duplicated handshake
        if client not in self._connections:
            self._connections.append(client)

    def _execute_echo(self, client, cmd):
        if len(cmd) != 2:
            client.send(b"$-1\r\n")
            return
        print(f"[_execute_echo] cmd = {cmd} msg={cmd[1]}\n")
        msg = cmd[1]
        resp = f"${len(msg)}\r\n{msg}\r\n"
        client.send(resp.encode())

    def _execute_get(self, client, cmd):
        if len(cmd) != 2:
            client.send(b"$-1\r\n")
            return
        key = cmd[1]
        v = ""
        if key in self._cache:
            v = self._cache[key]
        else:
            if self._rdb_snapshot:
                k, v, expiry = self._rdb_snapshot.get_val(key)
                if expiry < 0:
                    client.send(b"$-1\r\n")
                    return
                elif expiry > 0:
                    t = Timer(expiry, self._delete_key, k)
                    t.start()
                    print(f"[_execute_get] expiry = {expiry}\n")
                self._cache[k] = v

        print(f"[_execute_get] cmd = {cmd} key={cmd[1]}\n")
        print(self._cache.items())
        if not v:
            client.send(b"$-1\r\n")
            return
        resp = f"${len(v)}\r\n{v}\r\n"
        client.send(resp.encode())

    def _execute_set(self, client, cmd):
        if len(cmd) > 5:
            client.send(b"$-1\r\n")
            return
        key, val = cmd[1], cmd[2]
        print(f"[_execute_set] cmd = {cmd} key={cmd[1]} val={cmd[2]}\n")
        print(self._cache['dir'], self._cache['dbfilename'])
        if not self._rdb_snapshot:
            print("Saving to cache")
            self._cache[key] = val
        else:
            self._rdb_snapshot.set_val(key, val)
         # With expiry
        if len(cmd) == 5:
            expiry = int(cmd[4])
            t = Timer(utils._ms_to_s(expiry), self._delete_key, key)
            t.start()
            print(f"[_execute_set] expiry = {expiry}\n")
        print(self._cache.items())
        if client is not self.master_socket:
            client.send( b"+OK\r\n")

    def _execute_config(self, client, cmd):
        op = cmd[1]
        if op == 'GET':
            key = cmd[2]
            print(f"[_execute_config] op={op} key={key}")
            if key not in self._cache:
                client.send(b"$-1\r\n")
                return
            resp = f"*2\r\n${len(key)}\r\n{key}\r\n${len(self._cache[key])}\r\n{self._cache[key]}\r\n"
            client.send(resp.encode())

    def _execute_keys(self, client, cmd):
        key = cmd[1]
        print(f"[_execute_keys] key = {key}")
        print(self._cache['dir'], self._cache['dbfilename'])

        if key == '*':
            keys = self._rdb_snapshot.get_all()
            resp = f"*{len(keys)}\r\n"
            if len(keys) > 0:
                for k in keys:
                    r = f"${len(k)}\r\n{k}\r\n"
                    resp += r
            client.send(resp.encode())

        else:
            pass

    def _execute_info(self, client, cmd):
        section = cmd[1]
        if not self.master:
            role = "role:slave"
            client.send(f"${len(role)}\r\n{role}\r\n".encode())
        else:
            role = "role:master"
            master_replid = "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
            master_repl_offset = "master_repl_offset:0"
            resp_len = len(role) + len(master_replid) + len(master_repl_offset) + 4
            resp = f"${resp_len}\r\n{role}\r\n{master_repl_offset}\r\n{master_replid}\r\n"
            client.send(resp.encode())

    def _execute_wait(self, client, cmd):
        num_waits = int(cmd[1])
        if num_waits == 0:
            client.send(":0\r\n".encode())
            return
        print(self._connections)
        wait_time = int(cmd[2])
        client.send(f":{len(self._connections)}\r\n".encode())


    def _execute_cmd(self, client, cmd):
        print(f"[_execute_cmd] cmd={cmd}\n")
        if cmd[0] == 'PING':
            self._execute_ping(client)
        elif cmd[0] == 'REPLCONF':
            self._execute_replconf(client, cmd)
        elif cmd[0] == 'PSYNC':
            self._execute_psync(client)
        if cmd[0] == 'ECHO':
            self._execute_echo(client, cmd)
        elif cmd[0] == 'SET':
            self._execute_set(client, cmd)
        elif cmd[0] == 'GET':
            self._execute_get(client, cmd)
        elif cmd[0] == 'CONFIG':
            self._execute_config(client, cmd)
        elif cmd[0] == 'KEYS':
            self._execute_keys(client, cmd)
        elif cmd[0] == 'INFO':
            self._execute_info(client, cmd)
        elif cmd[0] == 'WAIT':
            self._execute_wait(client, cmd)
        # else:
        #     client.send(b"$-1\r\n")

    def _broadcast(self, data):
        print("[Broadcasting]", data)
        for c in self._connections:
            c.send(data)

    def _split_cmd(self, parsed):
        cmds = []
        cmd = None
        args = []
        for item in parsed:
            if item in Server.COMMANDS:
                if cmd == 'CONFIG':
                    args.append(item)
                    continue
                elif cmd:
                    cmds.append([cmd] + args)
                cmd = item
                args = []
            else:
                args.append(item)
        if cmd:
            cmds.append([cmd] + args)
        return cmds

    def _parse_data(self, client, data):
        p = parser.Parser(data)
        parsed = p.parse_data()

        ack_pos = data.find(b"GETACK")
        ack_end = ack_pos + data[ack_pos:].find(b"*\r\n") + utils.LEN_CRLF
        if self._parsed_bytes != -1:
            if ack_pos >= 0:
                print(f"ack_pos = {ack_pos} original = {data} stream = {data[ack_pos:]}")
                self._parsed_bytes += len(data[:ack_end+1])
            else:
                self._parsed_bytes += len(data)

        for c in self._split_cmd(parsed):
            if self.master and c[0] in Server.PROPAGATE_LIST:
                self._broadcast(data)

            self._execute_cmd(client, c)

        # If there is a GETACK, return bytes read before GETACK. Then add the rest
        if self._parsed_bytes != -1 and ack_pos >= 0:
            self._parsed_bytes += len(data[ack_end+1:])

    def serve_client(self, client):
        data = client.recv(1024)
        if data:
            print(data, len(data))
            self._parse_data(client, data)

