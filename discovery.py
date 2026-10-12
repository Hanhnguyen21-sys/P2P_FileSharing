"""
Discovery.py - UDP discovery (PEER_REQUEST/PEER_ACK)
Broadcast: broadcast to the LAN; ignore our own request when received
    
BROADCAST_ADDR = "255.255.255.255" -> OS recognizes this is broadcast address and 
                                      send signal/ msg to all nodes in the same network
Steps:
1. broadcasts PEER_REQUEST to every node (except for itself) in the same LAN
2. Every node receives it replies with a unicast PEER_ACK (node_id + TCP port)
3. PEER_ACK contains information that used to establish TCP connection between 2 nodes

"""


import json
import socket
import time
 
from config import NODE_ID, TCP_PORT, UDP_PORT
from logger import log_msg
from connections import connect_to_peer, is_connected

 
HOST = ""                 # listen on all interfaces
BUFFER_SIZE = 4096        # UDP messages are small JSON objects
BROADCAST_ADDR = "255.255.255.255" # b
REQUEST_REPEATED= 3 # since UDP can drop packets, re-send 3 times
# SOCK_DGRAM = UDP (unreliable, connectionless)
# Each recvfrom() / sendto() call is one independent datagram.

def create_udp_socket():
    # create UDP socket for broadcast and listening on 54321
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) 

    udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    # enable OS to send a broadcast address
    udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)


    # Binding before sending also makes our broadcasts go out from port 54321.
    udp_sock.bind((HOST, UDP_PORT))
    return udp_sock

def send_json(udp_sock ,msg,addr):
    # encode msg as JSON and send it
    udp_sock.sendto(json.dumps(msg).encode("utf-8"), addr)
    

def send_peer_request(udp_sock):
    request = {
                "type": "PEER_REQUEST",
                "node_id": NODE_ID,
            }
    addr = (BROADCAST_ADDR,UDP_PORT)
    # keep sending at most 3 times every 1s
    for i in range(REQUEST_REPEATED):
        send_json(udp_sock, request, addr)
        log_msg("Sent", addr, "PEER_REQUEST")
        time.sleep(1)
        
        
def parse_datagram(data):
    """Return the datagram as a dict (Python object), or None if it is not a valid message."""
    try:
        msg = json.loads(data.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(msg, dict) or "type" not in msg or "node_id" not in msg:
        return None
    return msg

def handle_peer_request(udp_sock,msg,addr):
    # return ACK if other node joins so that the sender know the information to establish TCP connection
    ack = {
        "type": "PEER_ACK",
        "tcp_port": TCP_PORT,
        "node_id":NODE_ID,
    }
    send_json(udp_sock,ack,addr)
    log_msg("Sent", addr, "PEER_ACK")
    
def handle_peer_ack(msg,addr):
    peer_id = msg["node_id"]
    tcp_port = msg.get("tcp_port")
    if not isinstance(tcp_port, int):
        return
    # Never open a second connection to a peer we are already connected to.
    if is_connected(peer_id):
        return
    # IP comes from where the datagram came from, which is always correct.
    connect_to_peer(addr[0], tcp_port, peer_id)
    
def udp_listener(udp_sock):
    while True:
        try:
            data, addr = udp_sock.recvfrom(BUFFER_SIZE)
        except OSError:
            break   
        
        msg = parse_datagram(data)
        if msg is None:
            continue      
        
        # ignore itself request
        if msg["node_id"] == NODE_ID:
            continue           
        
        log_msg("Received", addr, msg["type"])
        
        try:
            if msg["type"] == "PEER_REQUEST":
                handle_peer_request(udp_sock, msg, addr)
            elif msg["type"] == "PEER_ACK":
                handle_peer_ack(msg, addr)
        except Exception as e:              # one bad peer must not kill the listener
            log_msg("ERROR", addr, msg["type"], str(e))

     
    