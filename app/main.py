import select
from . import server

LEN_CRLF = 2

def main():
    redis_server = server.Server()
    fds_to_watch = [redis_server.socket]
    while True:
        ready_to_read, _, _ = select.select(fds_to_watch, [], [])
        for s in ready_to_read:
            if s == redis_server.socket:
                client_socket, addr = redis_server.socket.accept()
                fds_to_watch.append(client_socket)
            else:
                redis_server.serve_client(s)


if __name__ == "__main__":
    main()
