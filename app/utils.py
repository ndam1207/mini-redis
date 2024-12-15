LEN_CRLF = 2

def _readbytes(stream, length):
    return stream[:length+LEN_CRLF]

def _readline(stream):
    pos = stream.find(b"\r\n")
    return stream[:pos]