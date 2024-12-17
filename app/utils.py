LEN_CRLF = 2

def _readbytes_exact(stream, length, start=0):
    return stream[start:start+length]

def _readbytes_crlf(stream, length, start=0):
    return stream[start:start+length+LEN_CRLF]

def _readline(stream):
    pos = stream.find(b"\r\n")
    return stream[:pos]

def _ms_to_s(ms):
    return 0.001*int(ms)