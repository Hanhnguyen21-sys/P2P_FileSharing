import threading
from datetime import datetime

from config import LOG_FILE

# Use these instead of typing the strings, so a typo is a NameError,
# not a silently wrong log line.
SENT = "SENT"
RECEIVED = "RECEIVED"

_log_lock = threading.Lock()   # one lock shared by every thread in this node


def _timestamp():
    """Current local time with milliseconds, e.g. 2026-10-08 14:02:11.482."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def _fmt_addr(addr):
    """Accept "ip:port" strings or (ip, port) tuples."""
    if isinstance(addr, tuple):
        return f"{addr[0]}:{addr[1]}"
    return str(addr)


def _write_line(line):
    """Append one line to log.txt. Only one thread may write at a time."""
    with _log_lock:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line.rstrip() + "\n")


def log_msg(direction, addr, msg_type, detail=""):
    """Log one message sent or received.

    direction : SENT or RECEIVED
    addr      : peer address, "ip:port" or (ip, port)
    msg_type  : PEER_REQUEST, PEER_ACK, FILE_LIST, DATA_REQUEST, FILE_DATA
    detail    : optional extra info, e.g. a filename or "status=error"
    """
    _write_line(f"{_timestamp()}  {direction:<8}  {_fmt_addr(addr):<21}  {msg_type:<12}  {detail}")


def log_event(text):
    """Log a peer table change or other event.

    Example: log_event(f"PEER ADDED {node_id[:8]} {ip}:{port}")
    """
    _write_line(f"{_timestamp()}  {'EVENT':<8}  {text}")


def log_error(addr, text):
    """Log an error that did not stop the node, e.g. a bad message from a peer."""
    _write_line(f"{_timestamp()}  {'ERROR':<8}  {_fmt_addr(addr):<21}  {text}")