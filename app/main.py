import socket
import select

def serve_client(s):
    data = s.recv(1024)
    if data:
        print(data.decode())
        if "PING" in data.decode():
            s.send("+PONG\r\n".encode())

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
