from app import parser
from app import utils
import socket
from threading import Timer

class Server:
    def __init__(self, **kwargs):
        self._db = {}
        self.socket = socket.create_server(("localhost", 6379), reuse_port=True, backlog=5)
        self._parse_args(**kwargs)

    def _parse_args(self, **kwargs):
        for key, val in kwargs.items():
            self._db[key] = val
            print(f"[_parse_args] key={key} val={val}")

    def _delete_key(self, *args):
        key = ''.join(args)
        print(f"[_delete_key] key={key}")
        del self._db[key]

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
        if key not in self._db:
            client.send(b"$-1\r\n")
            return
        print(f"[_execute_get] cmd = {cmd} key={cmd[1]}\n")
        resp = f"${len(self._db[key])}\r\n{self._db[key]}\r\n"
        client.send(resp.encode())

    def _execute_set(self, client, cmd):
        if len(cmd) > 5:
            client.send(b"$-1\r\n")
            return
        key, val = cmd[1], cmd[2]
        print(f"[_execute_set] cmd = {cmd} key={cmd[1]} val={cmd[2]}\n")
        self._db[key] = val
         # With expiry
        if len(cmd) == 5:
            expiry = int(cmd[4])
            t = Timer(utils._ms_to_s(expiry), self._delete_key, key)
            t.start()
            print(f"[_execute_set] expiry = {expiry}\n")
        client.send( b"+OK\r\n")

    def _execute_config(self, client, cmd):
        op = cmd[0]
        if op == 'GET':
            key = cmd[1]
            print(f"[_execute_config] op={op} key={key}")
            if key not in self._db:
                client.send(b"$-1\r\n")
                return
            resp = f"*2\r\n${len(key)}\r\n{key}\r\n${len(self._db[key])}\r\n{self._db[key]}\r\n"
            client.send(resp.encode())

    def execute_cmd(self, client, cmd):
        print(f"[execute_cmd] cmd={cmd}")
        if cmd[0] == 'PING':
            self._execute_ping(client)
        if cmd[0] == 'ECHO':
            self._execute_echo(client, cmd)
        elif cmd[0] == 'SET':
            self._execute_set(client, cmd)
        elif cmd[0] == 'GET':
            self._execute_get(client, cmd)
        elif cmd[0] == 'CONFIG':
            self._execute_config(client, cmd[1:])
        # else:
        #     client.send(b"$-1\r\n")

    def serve_client(self, client):
        data = client.recv(1024)
        if data:
            p = parser.Parser(data)
            cmd = p.parse_stream()
            self.execute_cmd(client, cmd)
