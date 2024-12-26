LEN_CRLF = 2
COMMANDS = ['PING', 'ECHO', 'SET', 'GET', 'REPLCONF', 'PSYNC', 'KEYS', \
            'INFO', 'CONFIG', 'WAIT', 'TYPE', 'XADD', 'REDIS0011']

def readbytes_exact(stream, length, start=0):
    return stream[start:start+length]

def writebytes_exact(stream, buffer, length, start=0):
    stream[start:start+length] = buffer
    return stream[start:start+length]

def readbytes_crlf(stream, length, start=0):
    return stream[start:start+length+LEN_CRLF]

def readline(stream):
    pos = stream.find(b"\r\n")
    return stream[:pos]

def ms_to_s(ms):
    return 0.001*int(ms)

def s_to_ms(s):
    return 1000.0*int(s)

def get_type(var):
    var_type = type(var).__name__
    if var_type == 'str':
        return 'string'
    elif var_type == 'int':
        return 'int'