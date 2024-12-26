from app.utils import readline, readbytes_exact, LEN_CRLF, COMMANDS
from app.io import RDB
class Parser:
    def __init__(self, buffer=b""):
        self.commands = []
        self.bytes_read = 0
        self._parsed = []

        self._buffer = buffer

    def _split_cmd(self, parsed):
        cmds = []
        cmd = None
        args = []
        for item in parsed:
            if item in COMMANDS:
                if cmd == 'CONFIG':
                    args.append(item)
                    continue
                elif cmd:
                    cmds.append([cmd] + args)
                cmd = item
                args = []
            else:
                args.append(item)
        if cmd:
            cmds.append([cmd] + args)
        return cmds

    def _parse_array(self, num_args):
        # print(f"[_parse_array] buffer = {self._buffer}, num_args={num_args}\n")
        for _ in range(num_args):
            self._parse_stream()

    def _parse_bulk_string(self, s_len):
        header = readbytes_exact(self._buffer, 9)
        if header == RDB.HEADER_MAGIC:
            # Skip file parsing for now
            # print(f"[_parse_bulk_string] Found RDB buffer={self._buffer} header={header}\n")
            self._parsed.append(header.decode())
            self._buffer = self._buffer[s_len:]
            return
        bulk_str = readline(self._buffer).decode()
        # print(f"[_parse_bulk_string] buffer={self._buffer} cmd={bulk_str}\n")
        self._buffer = self._buffer[s_len+LEN_CRLF:]
        self._parsed.append(bulk_str)
        self.bytes_read += (s_len+LEN_CRLF)

    def _parse_stream(self):
        header = readline(self._buffer)
        self._buffer = self._buffer[len(header)+LEN_CRLF:]
        header = header.decode()
        cmd_type = header[0]
        self.bytes_read += len(header)+LEN_CRLF
        # print(f"[parse_stream] header = {header.encode()} cmd_type={cmd_type} buffer={self._buffer}\n")
        if cmd_type == '*':
            num_args = int(header[1:])
            self._parse_array(num_args)
        elif cmd_type == '$':
            s_len = int(header[1:])
            self._parse_bulk_string(s_len)

    def parse_data(self):
        while self._buffer:
            # print(f"[parse_data] BEFORE buffer={self._buffer}")
            self._parse_stream()
            # print(f"[parse_data] AFTER buffer={self._buffer} bytes_read={self.bytes_read}")
            self.commands.append(Command(cmd_buffer=self._parsed, cmd_size=self.bytes_read))
            self._parsed = []
            self.bytes_read = 0
class Command:
    def __init__(self, cmd_buffer, cmd_size):
        self.buffer = cmd_buffer
        self.size = cmd_size
