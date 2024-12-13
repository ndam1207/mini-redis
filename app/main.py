import socket
import select

COMMANDS = ['PING','ECHO', 'GET', 'SET']


def get_echo_response(stream):
    data, _ = parse_bulk_string(stream)
    print(f"get_echo_response {data}")
    return data

def get_cmd_response(stream, cmd):
    print(f"[get_cmd_response] stream={stream} cmd={cmd}")
    match cmd:
        case 'ECHO':
            return ''
        case 'PING':
            return b'+PONG\r\n'

def parse_array(stream):
    num_args = int(stream[1]) - ord('0')
    total_bytes_read = 4 # 2 bytes for type and array length, 2 bytes for \r\n
    print(f"stream size = {len(stream)}, num_args={num_args}")
    stream = stream[total_bytes_read:]
    resps = []
    r, bytes_read = parse_type(stream)
    for _ in range(num_args):
        if total_bytes_read == len(stream):
            break
        r, bytes_read = parse_type(stream)
        if len(r) > 0:
            resps.append(r)
        total_bytes_read += bytes_read
        stream = stream[bytes_read:]
        print(f"[parse_array] bytes read = {total_bytes_read} now stream = {stream}")
    return resps, total_bytes_read

def parse_bulk_string(stream):
    print(f"[parse_bulk_string] {stream}")
    s_len = int(stream[1]) - ord('0')
    print(s_len)
    total_bytes_read = 4 + s_len + 2
    data = stream[4:4+s_len].decode()
    print(f"data = {data}")
    if data in COMMANDS:
        # _, bytes_read = get_cmd_response(stream, data)
        # total_bytes_read += bytes_read
        data = get_cmd_response(stream, data)
        print(f"parsed data = {data}")
        return data, total_bytes_read
    print(f"[parse_bulk_string] data size = {s_len} now stream = {stream}")
    print(f"[parse_bulk_string] data = {stream[:total_bytes_read]} total_bytes_read = {total_bytes_read}")
    return stream[:total_bytes_read], total_bytes_read


def parse_type(stream):
    cmd_type = chr(stream[0])
    match cmd_type:
        case '*':
            return parse_array(stream)
        case '$':
            return parse_bulk_string(stream)

def serve_client(s):
    data = s.recv(1024)
    if data:
        resps, _ = parse_type(data)
        if type(resps) is list:
            for r in resps:
                s.send(r)
        else:
            s.send(f"+{resps}\r\n".encode())

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
