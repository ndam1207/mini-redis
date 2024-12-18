from app import parser, utils, io
from threading import Timer
import socket, os
class Server:
    def __init__(self, **kwargs):
        self._cache = {}
        self.socket = None
        self._rdb_snapshot = None
        self._parse_args(**kwargs)

    def _parse_args(self, **kwargs):
        for key, val in kwargs.items():
            print(key, val)
            self._cache[key] = val
        if self._cache['dir'] and self._cache['dbfilename']:
            self._get_db_image()
        if self._cache['port']:
            port = int(self._cache['port'])
        else:
            port = 6379
        self.socket = socket.create_server(("localhost", port), reuse_port=True, backlog=5)

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
        client.send(b"+PONG\r\n")

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
        if not self._rdb_snapshot:
            if key in self._cache:
                v = self._cache[key]
            else:
                client.send(b"$-1\r\n")
                return
        else:
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
            self._cache[key] = val
        else:
            self._rdb_snapshot.set_val(key, val)
         # With expiry
        if len(cmd) == 5:
            expiry = int(cmd[4])
            t = Timer(utils._ms_to_s(expiry), self._delete_key, key)
            t.start()
            print(f"[_execute_set] expiry = {expiry}\n")
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

    def execute_cmd(self, client, cmd):
        print(f"[execute_cmd] cmd={cmd}\n")
        if cmd[0] == 'PING':
            self._execute_ping(client)
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
        # else:
        #     client.send(b"$-1\r\n")

    def serve_client(self, client):
        data = client.recv(1024)
        if data:
            p = parser.Parser(data)
            cmd = p.parse_stream()
            self.execute_cmd(client, cmd)
