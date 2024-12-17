import os, mmap
from app.utils import _readbytes_exact

class RDB:
    HEADER_MAGIC = b'\x52\x45\x44\x49\x53\x30\x30\x31\x31'
    META_START = b'\xfa'
    DB_START = b'\xfe'
    HASH_TAB_START = b'\xfb'
    EOF = b'\xff'
    REDIS_VER = b'\x09\x72\x65\x64\x69\x73\x2D\x76\x65\x72'
    ENCODE_MASK = 0b11
    ENCODE_SHIFT = 6

    def __init__(self, file_path=None):
        self._file_path = file_path
        self._buffer = None
        self._db_size = 0
        if self._file_path:
            self._map_file(self._file_path)
            # self._update_metadata()

    def _map_file(self, file_path):
        if not os.path.isfile(file_path):
            self._buffer = b""
        else:
            with open(file_path, mode='r+b') as f:
                self._buffer = f.read()

    def _unmap_file(self):
        self._buffer.close()

    def _get_db_section(self, buffer):
        db_start = buffer.find(RDB.DB_START)
        db_end = buffer.find(RDB.EOF)
        return buffer[db_start+1:db_end]

    def _get_db_tab(self, buffer):
        db_section = self._get_db_section(buffer)
        tab_start = db_section.find(RDB.HASH_TAB_START)
        return db_section[tab_start+1:]

    def _parse_size_encode(self, byte):
        byte = int.from_bytes(byte)
        size = 0 # in bits
        if byte & RDB.ENCODE_MASK == 0b00:
            size = 6
        elif byte & RDB.ENCODE_MASK == 0b01:
            size = 14
        elif byte & RDB.ENCODE_MASK == 0b10:
            pass
        return size

    def _parse_string_encode(self, byte):
        byte = int.from_bytes(byte)
        if (byte >> RDB.ENCODE_SHIFT) & RDB.ENCODE_MASK == 0b11:
            pass
        else:
            return byte
    # def _update_metadata(self):
    #     db_section = self._get_db_section()
    #     db_index = _readbytes_exact(db_section, 1)

    def get_all_keys(self):
        print("[get_all_keys]")
        tab_section = self._get_db_tab(self._buffer)
        print(tab_section)
        num_keys = _readbytes_exact(tab_section, 1)
        tab_section = tab_section[2:]
        keys = []
        print(int.from_bytes(num_keys))
        for _ in range(int.from_bytes(num_keys)):
            value_type = _readbytes_exact(tab_section, 1)
            if value_type == b'\x00':
                byte = _readbytes_exact(tab_section, 1, 1)
                key_size = self._parse_string_encode(byte)
                value = _readbytes_exact(tab_section, key_size, 2).decode()
                keys.append(value)
        return keys
