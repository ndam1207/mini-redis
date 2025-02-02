import socket, os, time
import concurrent.futures
import threading, asyncio
from app import parser, utils, io
from app.parser import Parser, Command
from collections import defaultdict

class Server:
    DEFAULT_PORT = 6379
    EMPTY_RDB_FILE = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
    PROPAGATE_LIST = ['SET', 'DEL', 'INCR']
    SKIP_ACK_LIST = ['REDIS0011']

    def __init__(self, **kwargs):
        self.master = True
        self.socket = None
        self.master_socket = None

        self._master_port = -1
        self._master_hostname = None
        self._cache = {}
        self._port = Server.DEFAULT_PORT
        self._rdb_snapshot = None
        self.replica_lock = threading.Lock()
        self._connections = {} # Client + bytes offset (if client is a replica)
        self._bytes_offset = -1
        self._replica_offset = -1
        self._handshake_done = False
        self._streams = {}
        self._xadd_conditions = {}
        self._xadd_latest = None
        self._multi_queue = defaultdict(list)
        self._multi = {}
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

        if self.master_socket not in self._connections:
            self._connections[self.master_socket] = -1

    def _get_db_image(self):
        rdb_path = os.path.join(self._cache['dir'], self._cache['dbfilename'])
        print(f"[_get_db_image] {rdb_path}")
        self._rdb_snapshot = io.RDB(rdb_path)

    def _delete_key(self, key):
        print(f"[_delete_key] key={key}")
        if key in self._cache:
            del self._cache[key]

    def _execute_ping(self, client):
        if client is not self.master_socket:
            client.send(b"+PONG\r\n")
        if client is self.master_socket and not self._handshake_done:
            client.send(b"+PONG\r\n")

    def _execute_replconf(self, client, cmd):
        print(cmd)
        if cmd[1].lower() == "capa" or cmd[1].lower() == "listening-port":
            client.send(b"+OK\r\n")
        elif cmd[1].upper() == "GETACK":
            offset = self._bytes_offset if self._bytes_offset != -1 else 0
            client.send(
                f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(offset))}\r\n{str(offset)}\r\n" \
                .encode()
                )
        elif cmd[1].upper() == "ACK":
            bytes_offset = int(cmd[2])
            with self.replica_lock:
                self._connections[client] = bytes_offset

    def _execute_psync(self, client):
        repl_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        client.send(f"+FULLRESYNC {repl_id} 0\r\n".encode())
        rdb_file = bytes.fromhex(Server.EMPTY_RDB_FILE)
        client.send(f"${len(rdb_file)}\r\n".encode() + rdb_file)
        self._handshake_done = True
        if client not in self._connections:
            self._connections[client] = 0

    def _finish_handshake(self, master_socket):
        self._handshake_done = True

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
                    t = threading.Timer(expiry, self._delete_key, k)
                    t.start()
                    print(f"[_execute_get] expiry = {expiry}\n")
                self._cache[k] = v

        print(f"[_execute_get] cmd = {cmd} key={cmd[1]}\n")
        if not v:
            client.send(b"$-1\r\n")
            return
        resp = f"${len(str(v))}\r\n{str(v)}\r\n"
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
            t = threading.Timer(utils.ms_to_s(expiry), self._delete_key, args=(key,))
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

    # Master
    def _execute_wait(self, client, cmd):
        num_waits = int(cmd[1])
        if num_waits == 0:
            client.send(":0\r\n".encode())
            return
        timeout = utils.ms_to_s(int(cmd[2]))
        print("[_execute_wait] ", self._replica_offset, self._connections.values())
        with self.replica_lock:
            for c in self._connections.keys():
                self._send_get_ack(c)
        num_acks = 0
        with self.replica_lock:
            for c in self._connections:
                if self._connections[c] >= self._replica_offset:
                    num_acks += 1
        if num_acks >= num_waits:
            client.send(f":{num_acks}\r\n".encode())
        else:
            t = threading.Timer(timeout, self._count_acks_from_wait, args=(client,))
            t.start()

    def _execute_type(self, client, cmd):
        v = ""
        key = cmd[1]
        if key in self._streams:
            client.send("+stream\r\n".encode())
            return
        if key in self._cache:
            v = self._cache[key]
        else:
            if self._rdb_snapshot:
                k, v, expiry = self._rdb_snapshot.get_val(key)
                self._cache[k] = v
        if not v:
            client.send("+none\r\n".encode())
        else:
            val_type = utils.get_type(v)
            client.send(f"+{val_type}\r\n".encode())

    def _execute_xadd(self, client, cmd):
        print("[_execute_xadd]", cmd)
        stream_key = str(cmd[1])
        if stream_key not in self._streams:
            self._streams[stream_key] = io.Stream(stream_key)
        stream = self._streams[stream_key]
        entry_id = str(cmd[2])
        if entry_id == "0-0":
            print(entry_id)
            client.send("-ERR The ID specified in XADD must be greater than 0-0\r\n".encode())
            return
        if entry_id == "*":
            ms, seq = stream.generate_time_and_seq()
            entry_id = f"{ms}-{seq}".strip()
        else:
            ms, seq = entry_id.split("-")[0], entry_id.split("-")[1]
            if seq == '*':
                seq = stream.generate_seq(float(ms))
                entry_id = f"{ms}-{seq}".strip()
        if not stream.id_valid(entry_id):
            client.send("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n".encode())
            return
        idx = 3
        kv_list = []
        while idx < len(cmd):
            key, val = str(cmd[idx]), str(cmd[idx+1])
            kv_list.append((key, val))
            idx += 2
        stream.add_entry(entry_id, kv_list)
        time, seq = int(entry_id.split("-")[0]), int(entry_id.split("-")[1])
        e_time, e_seq = 0, 0
        self._xadd_latest = entry_id

        for e_id in self._xadd_conditions.keys():
            print("wait ids", e_id)
            if e_id != '$':
                e_time, e_seq = int(e_id.split("-")[0]), int(e_id.split("-")[1])
            if seq > e_seq or e_id == '$':
                print("signaling ", e_id, self._xadd_conditions[e_id])
                with self._xadd_conditions[e_id]:
                    self._xadd_conditions[e_id].notify_all()
        client.send(f"${len(entry_id)}\r\n{entry_id}\r\n".encode())

    def _execute_xrange(self, client, cmd):
        stream_key = str(cmd[1])
        start_id = str(cmd[2])
        end_id = str(cmd[3])
        stream = self._streams[stream_key]
        print(f"[_execute_xrange]", start_id, end_id)
        stream_list = stream.find_range(start_id, end_id)

        resp = f"*{len(stream_list)}\r\n"
        for s in stream_list:
            print(s.id, s.kv_list)
            resp += "*2\r\n"
            id, kv_list = s.id, s.kv_list
            resp += f"${len(str(id))}\r\n{str(id)}\r\n"
            resp += f"*{len(kv_list)*2}\r\n"
            for key, val in kv_list:
                resp += f"${len(str(key))}\r\n{str(key)}\r\n"
                resp += f"${len(str(val))}\r\n{str(val)}\r\n"
        client.send(resp.encode())

    def _execute_xread(self, client, cmd):
        if str(cmd[1]).upper() == 'BLOCK':
            block_time = utils.ms_to_s(int(cmd[2]))
            stream_key = str(cmd[4])
            stream_id = str(cmd[5])
            stream_list = []
            if stream_id != '$':
                stream_list = self._streams[stream_key].find_range_start_exclusive(stream_id)
            print("[_execute_xread] with block", stream_id, block_time)
            cmd = cmd[4:]
            if not stream_list:
                self._xadd_conditions[stream_id] = threading.Condition(threading.Lock())
                t = threading.Thread(target=self._wait_for_xadd_and_read, args=(client, stream_key, stream_id, block_time, cmd))
                t.start()
                return
        else:
            cmd = cmd[2:]
        self._handle_xread(client, cmd)

    def _wait_for_xadd_and_read(self, client, stream_key, stream_id, block_time, cmd):
        with self._xadd_conditions[stream_id]:
            print("[_wait_for_xadd_and_read]", stream_key, stream_id)
            # stream_list = self._streams[stream_key].find_range_start_exclusive(stream_id)
            # while not stream_list:
            timeout = block_time if block_time > 0 else None
            self._xadd_conditions[stream_id].wait(timeout)
        print("Done for xadd")
        if stream_id == '$':
            start_time, start_seq = int(self._xadd_latest.split("-")[0]), int(self._xadd_latest.split("-")[1])
            if start_seq > 0:
                start_seq -= 1
            start_id = f"{start_time}-{start_seq}".strip()
            cmd[-1] = start_id
            print("Latest =", self._xadd_latest, start_id)
            del self._xadd_conditions['$']
        if block_time > 0:
            stream_list = self._streams[stream_key].find_range_start_exclusive(stream_id)
            if not stream_list:
                client.send("$-1\r\n".encode())
                return
        self._handle_xread(client, cmd)

    def _handle_xread(self, client, cmd):
        print("[_handle_xread]", cmd)
        num_streams = len(cmd)//2
        resp = f"*{num_streams}\r\n"
        for i in range(num_streams):
            stream_key = str(cmd[i])
            start_id = str(cmd[i+num_streams])
            print("[_handle_xread]", start_id)
            stream_list = self._streams[stream_key].find_range_start_exclusive(start_id)
            if not stream_list:
                client.send("$-1\r\n".encode())
                return
            resp += "*2\r\n"
            resp += f"${len(stream_key)}\r\n{stream_key}\r\n"
            resp += f"*{len(stream_list)}\r\n"
            for s in stream_list:
                # print(s.id, s.kv_list)
                resp += "*2\r\n"
                id, kv_list = s.id, s.kv_list
                resp += f"${len(str(id))}\r\n{str(id)}\r\n"
                resp += f"*{len(kv_list)*2}\r\n"
                for key, val in kv_list:
                    resp += f"${len(str(key))}\r\n{str(key)}\r\n"
                    resp += f"${len(str(val))}\r\n{str(val)}\r\n"
        client.send(resp.encode())

    def _execute_incr(self, client, cmd):
        key = cmd[1]
        v = -1
        if key in self._cache:
            v = self._cache[key]
        else:
            if self._rdb_snapshot:
                k, v, expiry = self._rdb_snapshot.get_val(key)
                self._cache[k] = v

        print(f"[_execute_incr] cmd = {cmd} key={cmd[1]}, val={v}\n")
        print("v = ", v, type(v).__name__)
        if v == -1:
            v = 1
        else:
            if type(v).__name__ != 'int' and not v.isdigit():
                client.send("-ERR value is not an integer or out of range\r\n".encode())
                return
            v = int(v) + 1
        print("Saving to cache")
        self._cache[key] = v
        if self._rdb_snapshot:
            self._rdb_snapshot.set_val(key, v)
        client.send(f":{str(v)}\r\n".encode())

    def _execute_multi(self, client):
        self._multi[client] = True
        client.send("+OK\r\n".encode())

    def _execute_exec(self, client):
        print("[_execute_exec]")
        if client not in self._multi or not self._multi[client]:
            client.send("-ERR EXEC without MULTI\r\n".encode())
            return
        self._multi[client] = False
        if len(self._multi_queue[client]) == 0:
            client.send("*0\r\n".encode())
            return
        client.send(f"*{len(self._multi_queue[client])}\r\n".encode())
        for c in self._multi_queue[client]:
            self._execute_cmd(client, c)
        self._multi_queue[client] = []

    def _execute_discard(self, client):
        print("[_execute_discard]")
        if client not in self._multi or not self._multi[client]:
            client.send("-ERR DISCARD without MULTI\r\n".encode())
            return
        self._multi[client] = False
        client.send("+OK\r\n".encode())

    def _execute_cmd(self, client, cmd):
        print(f"[_execute_cmd] cmd={cmd}\n")
        cmd[0] = cmd[0].upper()
        if cmd[0] == 'PING':
            self._execute_ping(client)
        elif cmd[0] == 'REPLCONF':
            self._execute_replconf(client, cmd)
        elif cmd[0] == 'PSYNC':
            self._execute_psync(client)
        elif cmd[0] == 'REDIS0011':
            self._finish_handshake(client)
        elif cmd[0] == 'ECHO':
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
        elif cmd[0] == 'TYPE':
            self._execute_type(client, cmd)
        elif cmd[0] == 'XADD':
            self._execute_xadd(client, cmd)
        elif cmd[0] == 'XRANGE':
            self._execute_xrange(client, cmd)
        elif cmd[0] == 'XREAD':
            self._execute_xread(client, cmd)
        elif cmd[0] == 'INCR':
            self._execute_incr(client, cmd)
        elif cmd[0] == 'MULTI':
            self._execute_multi(client)
        elif cmd[0] == 'EXEC':
            self._execute_exec(client)
        elif cmd[0] == 'DISCARD':
            self._execute_discard(client)

    def _count_acks_from_wait(self, client):
        num_acks = 0
        print("[_count_acks_from_wait] ", self._replica_offset, self._connections.values())
        with self.replica_lock:
            for c in self._connections:
                if self._connections[c] >= self._replica_offset:
                    num_acks += 1
        client.send(f":{num_acks}\r\n".encode())

    def _send_get_ack(self, client, timeout=0):
        client.send("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n".encode())

    def _broadcast(self, data):
        print("[Broadcasting]", data)
        with self.replica_lock:
            for c in self._connections:
                c.send(data)

    def _parse_data(self, client, data):
        p = parser.Parser(data)
        p.parse_data()
        for c in (p.commands):
            cmd, cmd_size = c.buffer, c.size
            if not cmd:
                continue
            if self._multi.get(client, None) and cmd[0] not in ['EXEC', 'DISCARD']:
                self._multi_queue[client].append(cmd)
                client.send("+QUEUED\r\n".encode())
            else:
                self._execute_cmd(client, cmd)
            if self.master and cmd[0] in Server.PROPAGATE_LIST:
                self._broadcast(data)
                if self._replica_offset == -1:
                    self._replica_offset = cmd_size
                else:
                    self._replica_offset += cmd_size
            if cmd[0] not in self.SKIP_ACK_LIST:
                if self._bytes_offset == -1:
                    self._bytes_offset = cmd_size
                else:
                    self._bytes_offset += cmd_size

    def serve_client(self, client):
        data = client.recv(1024)
        if data:
            self._parse_data(client, data)

