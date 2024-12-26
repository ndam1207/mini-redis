import os, time, collections
from app import utils
from collections import deque
class RDB:
    HEADER_MAGIC = b'\x52\x45\x44\x49\x53\x30\x30\x31\x31'
    META_START = b'\xfa'
    DB_START = b'\xfe'
    HASH_TAB_START = b'\xfb'
    REDIS_VER = b'\x09\x72\x65\x64\x69\x73\x2D\x76\x65\x72'
    ENCODE_MASK = 0b11
    ENCODE_SHIFT = 6
    STRING_TYPE = b'\x00'
    EXPIRY_MS = b'\xfc'
    EXPIRY_S = b'\xfd'
    EXPIRY_TIME_MS_SIZE = 8
    EXPIRY_TIME_S_SIZE = 4
    EOF = b'\xff'

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
        #TODO: implement caching
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
        key_type = utils.readbytes_exact(key_section, 1,start_pos)
        key, value, expiry = None, None, 0
        bytes_read = 0

        # Parse expiry
        if key_type == RDB.EXPIRY_MS:
            curr_pos += 1
            expiry = utils.readbytes_exact(key_section, RDB.EXPIRY_TIME_MS_SIZE, curr_pos)
            expiry = int.from_bytes(expiry, byteorder='little')
            expiry = utils.ms_to_s(expiry) - time.time()

            curr_pos = curr_pos + RDB.EXPIRY_TIME_MS_SIZE
        elif key_type == RDB.EXPIRY_S:
            curr_pos += 1
            expiry = utils.readbytes_exact(key_section, RDB.EXPIRY_TIME_S_SIZE, curr_pos)
            expiry = int.from_bytes(expiry, byteorder='little') - time.time()
            curr_pos = curr_pos + RDB.EXPIRY_TIME_S_SIZE

        key_type = utils.readbytes_exact(key_section, 1, curr_pos)

        # Parse key type
        if key_type == RDB.STRING_TYPE:
            curr_pos += 1
            # Key (size encoding + key)
            byte = utils.readbytes_exact(key_section, 1, curr_pos)
            curr_pos += 1
            key_size = self._parse_string_encode(byte)
            key = utils.readbytes_exact(key_section, key_size, curr_pos).decode()
            curr_pos = curr_pos + key_size

            # Value (size encoding + value)
            byte = utils.readbytes_exact(key_section, 1, curr_pos)
            curr_pos += 1
            value_size = self._parse_string_encode(byte)
            value = utils.readbytes_exact(key_section, value_size, curr_pos).decode()
            curr_pos = curr_pos + value_size

        bytes_read = curr_pos - start_pos
        print(f"[_get_key_val] key={key} value={value} expiry={expiry}\n")
        return key, value, expiry, bytes_read

    def _get_key_section_start(self):
        tab_section = self._get_db_tab(self._buffer)
        num_keys = utils.readbytes_exact(tab_section, 1)
        key_section = tab_section[2:]
        return key_section, int.from_bytes(num_keys)

    def _locate_key(self, key):
        print(f"[_locate_key] {key}\n")
        key_section, num_keys = self._get_key_section_start()
        key_start = key_section.find(key.encode())
        k, v, expiry = None, None, 0
        for _ in range(num_keys):
            k, v, expiry, bytes_read = self._get_key_val(key_section)
            if k == key:
                break
            key_section = key_section[bytes_read:]
        return k, v, expiry

    def _insert_key(self, key, key_start):
        pass

    def delete_key(self, key):
        pass

    def set_val(self, key, val, expiry=0):
        print(f"[_set_key] key={key}\n")
        key, val, expiry = self._locate_key(key)
        # New key
        if not key:
            pass

    def get_val(self, key):
        k, v, expiry = self._locate_key(key)
        return k, v, expiry

    def get_all(self):
        print("[get_all]\n")
        key_section, num_keys = self._get_key_section_start()
        data = {}
        for _ in range(num_keys):
            key, value, expiry, bytes_read = self._get_key_val(key_section)
            key_section = key_section[bytes_read:]
            print(key_section)
            if key:
                data[key] = value
        return data

class StreamEntry:
    def __init__(self, id, key, val):
        self.id = id
        self.key = key
        self.val = val
class Stream:
    def __init__(self, stream_key):
        self.stream_key = stream_key
        self._entries = deque()
        self._ms_last = 0
        self._seq_num_last = -1

    def id_valid(self, id):
        ms, seq = id.split("-")
        if ms > self._ms_last:
            return True
        elif ms == self._ms_last:
            if seq > self._seq_num_last:
                return True
        return False

    def try_add_entry(self, id, key, val):
        if self.id_valid(id, key, val):
            self._entries.append(StreamEntry(id, key, val))
            return True
        return False

    def find_entry(self, id):
        for e in self._entries:
            if e.id == id:
                return e
        return None

