import socket
class Client:
    def __init__(self, socket=None, replica=False, multi=False):
        self.socket = socket
        self.replica = replica
        self.multi = multi
        self.multi_queue = []
        self.bytes_acked = 0

    def queue_multi_command(self, cmd):
        self.multi_queue.append(cmd)

    def execute_cmd(self, cmd):
        self.socket.send(cmd)
