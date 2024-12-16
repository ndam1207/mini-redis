import io
from app.utils import _readline
LEN_CRLF = 2
class Parser:
    def __init__(self, buffer=b""):
        print(f"Called Parser. buffer = {buffer}")
        self._buffer = buffer
        self._commands = []

    def _parse_array(self, num_args):
        print(f"[_parse_array] buffer = {self._buffer}, num_args={num_args}\n")
        for _ in range(num_args):
            self.parse_stream()

    def _parse_bulk_string(self, s_len):
        bulk_str = _readline(self._buffer).decode()
        print(f"[_parse_bulk_string] buffer={self._buffer} cmd={bulk_str}\n")
        self._buffer = self._buffer[s_len+LEN_CRLF:]
        self._commands.append(bulk_str)

    def parse_stream(self):
        header = _readline(self._buffer)
        self._buffer = self._buffer[len(header)+LEN_CRLF:]
        header = header.decode()
        cmd_type = header[0]
        print(f"[parse_stream] header = {header.encode()} cmd_type={cmd_type} buffer={self._buffer}\n")
        if cmd_type == '*':
            num_args = int(header[1:])
            self._parse_array(num_args)
        elif cmd_type == '$':
            s_len = int(header[1:])
            self._parse_bulk_string(s_len)
        return self._commands