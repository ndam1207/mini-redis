LEN_CRLF = 2
COMMANDS = ['PING', 'ECHO', 'SET', 'GET', 'REPLCONF', 'PSYNC', 'KEYS', 'INFO', 'CONFIG', 'WAIT']

def _readbytes_exact(stream, length, start=0):
    return stream[start:start+length]

def _writebytes_exact(stream, buffer, length, start=0):
    stream[start:start+length] = buffer
    return stream[start:start+length]

def _readbytes_crlf(stream, length, start=0):
    return stream[start:start+length+LEN_CRLF]

def _readline(stream):
    pos = stream.find(b"\r\n")
    return stream[:pos]

def _ms_to_s(ms):
    return 0.001*int(ms)

def _split_cmd(parsed):
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