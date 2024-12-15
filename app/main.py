import socket
import select

COMMANDS = ['PING','ECHO', 'GET', 'SET']
LEN_CRLF = 2
db = {}

def _readbytes(stream, length):
    return stream[:length+LEN_CRLF]

def _readline(stream):
    pos = stream.find(b"\r\n")
    return stream[:pos]

def execute_cmd(cmd, socket):
    if cmd[0] == 'PING':
        execute_ping(cmd, socket)
    if cmd[0] == 'ECHO':
        execute_echo(cmd, socket)
    elif cmd[0] == 'SET':
        execute_set(cmd, socket)
    elif cmd[0] == 'GET':
        execute_get(cmd, socket)

def execute_ping(cmd, socket):
    socket.send(b"+PONG\r\n")

def execute_echo(cmd, socket):
    if len(cmd) != 2:
        socket.send(b"$-1\r\n")
        return
    print(f"[execute_echo] cmd = {cmd} msg={cmd[1]}\n")
    msg = cmd[1]
    resp = f"${len(msg)}\r\n{msg}\r\n"
    socket.send(resp.encode())

def execute_get(cmd, socket):
    if len(cmd) != 2:
        socket.send(b"$-1\r\n")
        return
    key = cmd[1]
    if key not in db:
        socket.send(b"$-1\r\n")
        return
    print(f"[execute_get] cmd = {cmd} key={cmd[1]}\n")
    resp = f"${len(db[key])}\r\n{db[key]}\r\n"
    socket.send(resp.encode())

def execute_set(cmd, socket):
    if len(cmd) != 3:
        socket.send(b"$-1\r\n")
        return
    old_val, new_val = cmd[1], cmd[2]
    print(f"[execute_set] cmd = {cmd} old_val={cmd[1]} new_val={cmd[2]}\n")
    db[old_val] = new_val
    socket.send(b"+OK\r\n")

def parse_array(stream, num_args):
    print(f"[parse_array] stream = {stream}, num_args={num_args}\n")
    encoded_buffer = []
    for _ in range(num_args):
        encoded_str, stream = parse_type(stream)
        encoded_buffer.append(encoded_str)
    return encoded_buffer, stream

def parse_bulk_string(stream, s_len):
    print(f"[parse_bulk_string] {stream}")
    bulk_str = _readline(stream).decode()
    print(f"[parse_bulk_string] cmd={bulk_str}\n")
    stream = stream[s_len+LEN_CRLF:]
    return bulk_str, stream

def parse_type(stream):
    header = _readline(stream)
    stream = stream[len(header)+LEN_CRLF:]
    header = header.decode()
    cmd_type = header[0]
    print(f"[parse_type] cmd_type={cmd_type} stream={stream}\n")
    if cmd_type == '*':
        num_args = int(header[1])
        return parse_array(stream, num_args)
    elif cmd_type == '$':
        s_len = int(header[1])
        return parse_bulk_string(stream, s_len)

def serve_client(s):
    data = s.recv(1024)
    if data:
        cmd, _ = parse_type(data)
        execute_cmd(cmd, s)

def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True, backlog=5)
    fds_to_watch = [server_socket]
    while True:
        ready_to_read, _, _ = select.select(fds_to_watch, [], [])
        for s in ready_to_read:
            if s == server_socket:
                client_socket, addr = server_socket.accept()
                fds_to_watch.append(client_socket)
            else:
                serve_client(s)


if __name__ == "__main__":
    main()
