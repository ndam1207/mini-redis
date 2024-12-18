import os, mmap
from app.utils import _readbytes_exact, _writebytes_exact

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
        self._buffer = b""
        self._db_size = 0
        if self._file_path:
            self._map_file(self._file_path)

    def _map_file(self, file_path):
        if os.path.isfile(file_path):
            with open(file_path, mode='rb') as f:
                self._buffer = f.read()

    def _unmap_file(self):
        self._buffer.close()

    def _db_sync(self):
        pass

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

    def _get_key_val(self, key_section):
        start_pos, curr_pos = 0, 0
        key_type = _readbytes_exact(key_section, 1,start_pos)
        curr_pos += 1
        if key_type == b'\x00':
            byte = _readbytes_exact(key_section, 1, curr_pos)
            key_size = self._parse_string_encode(byte)
            key = _readbytes_exact(key_section, key_size, curr_pos+1).decode()
            curr_pos = curr_pos + 1 + key_size
            byte = _readbytes_exact(key_section, 1, curr_pos)
            value_size = self._parse_string_encode(byte)
            value = _readbytes_exact(key_section, value_size, curr_pos+1).decode()
            curr_pos = curr_pos + 1 + value_size
        bytes_read = curr_pos - start_pos
        return key, value, bytes_read

    def _get_key_section_start(self):
        tab_section = self._get_db_tab(self._buffer)
        num_keys = _readbytes_exact(tab_section, 1)
        key_section = tab_section[2:]
        return key_section, int.from_bytes(num_keys)

    def _locate_key(self, key):
        print("[_locate_key]", key)
        key_section, num_keys = self._get_key_section_start()
        key_start = key_section.find(key.encode())
        print(key_start)
        for _ in range(num_keys):
            k, v, bytes_read = self._get_key_val(key_section)
            if k == key:
                return k, key_section[:bytes_read]
            key_section = key_section[bytes_read:]
        return None, key_section

    def _insert_key(self, key, key_start):
        pass

    def delete_key(self, key):
        pass

    def set_val(self, key, val):
        print(f"[_set_key] key={key}")
        key, key_start = self._locate_key(key)
        # New key
        if key == None:
            pass
        print(key_start)

    def get_val(self, key):
        db = self.get_all()
        return db[key] if key in db else None

    def get_all(self):
        print("[get_all]")
        key_section, num_keys = self._get_key_section_start()
        data = {}
        for _ in range(num_keys):
            key, value, bytes_read = self._get_key_val(key_section)
            key_section = key_section[bytes_read:]
            data[key] = value
        return data
