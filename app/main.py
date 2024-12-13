import socket
import select

def parse_array(stream):
    n_args = stream[1]
    print(f"{n_args} args. args = {stream}")

def parse_bulk_string(stream):
    pass

def parse_type(stream):
    cmd_type = stream[0]
    print(cmd_type)
    match cmd_type:
        case '*':
            parse_array(stream)
        case '$':
            parse_bulk_string(stream)


def serve_client(s):
    data = s.recv(1024)
    if data:
        parse_type(data)
        # if "PING" in data.decode():
        #     s.send("+PONG\r\n".encode())

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
