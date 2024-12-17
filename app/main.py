from . import server
import select, argparse

LEN_CRLF = 2

def main(args):
    redis_server = server.Server(**vars(args))
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
    parser = argparse.ArgumentParser()
    parser.add_argument('--dir')
    parser.add_argument('--dbfilename')
    args = parser.parse_args()
    main(args)
