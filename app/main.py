import socket

def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    conn, addr = server_socket.accept()
    while True:
        data = conn.recv(1024)
        if data:
            c = data.decode().replace('\\n', '\n')
            commands = c.split('\n')[:-1]
            print(f"Commands: {commands}, len = {len(commands)}")
            for command in commands:
                if 'PING' in command:
                    conn.send("+PONG\r\n".encode())
if __name__ == "__main__":
    main()
