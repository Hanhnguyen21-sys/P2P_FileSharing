# protocol 
"""
TCP message: 
[4 bytes: length N, big-endian unsigned int][N bytes: JSON encoded in UTF-8]


Every JSON message has a "type" field (PEER_REQUEST, PEER_ACK, FILE_LIST,
DATA_REQUEST, FILE_DATA), so a receiver always knows what it just got.

"""

import json
import socket
import struct


"""Raised when the peer has closed or lost the connection."""
class PeerDisconnected(Exception):
    pass

    

def recv_exact(sock, n):
    buf = b""
    # keep calling recv() until we have all n bytes since recv() may 
    # return fewer bytes than requested
    while len(buf) < n:
        try:
            # ask bytes that still missing
            chunk = sock.recv(n - len(buf))
        except (ConnectionResetError, OSError):
            raise PeerDisconnected
        if not chunk:                       # peer disconnected when empty read
            raise PeerDisconnected
        buf += chunk
    return buf

"""
Encode message and send it as a single framed message

Steps:
1. dict -> JSON string
2. Prepend the length as 4 bytes
3. Send whole frame with sendall(), and keep sending until every byte is out
"""

def send_msg(sock, send_lock, msg):
    # encode by converting to byte to send 
    data = json.dumps(msg, ensure_ascii=False).encode("utf-8")
    frame = struct.pack("!I", len(data)) + data
    # only one thread writes to this socket at a time
    with send_lock:                        
        sock.sendall(frame)

"""
Read one framed message from sock and return it as a dict

Steps:
1. Read 4 bytes in header and convert it into integer to know length of JSON body
2. Read exactly n bytes from JSON body
"""
def recv_msg(sock):
    # convert from byte to string
    n = struct.unpack("!I", recv_exact(sock, 4))[0]
    body = recv_exact(sock, n)
    msg = json.loads(body.decode("utf-8"))
    if not isinstance(msg, dict) or "type" not in msg:
        raise ValueError("invalid message")
    return msg