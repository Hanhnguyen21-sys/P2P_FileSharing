import socket
import threading

from config import NODE_ID


class Connection:
    def __init__(self, sock, addr, outbound, node_id=None):
        self.sock = sock
        self.addr = addr
        self.outbound = outbound
        self.node_id = node_id

        self.send_lock = threading.Lock()
        self._close_lock = threading.Lock()
        self.closed = False

        # Updated/read through PeerTable methods.
        self.files = set()

    def close(self):
        """Close safely, including when several threads call close()."""
        with self._close_lock:
            if self.closed:
                return

            self.closed = True

            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

            self.sock.close()

    def __repr__(self):
        direction = "outgoing" if self.outbound else "incoming"
        return (
            f"Connection(node_id={self.node_id!r}, "
            f"addr={self.addr!r}, direction={direction})"
        )


class PeerTable:
    def __init__(self):
        self._peers = {}
        self._lock = threading.Lock()

    def has(self, node_id):
        with self._lock:
            conn = self._peers.get(node_id)
            return conn is not None and not conn.closed

    def get(self, node_id):
        with self._lock:
            conn = self._peers.get(node_id)
            if conn is None or conn.closed:
                return None
            return conn

    def register(self, conn):
        """
        Return (accepted, replaced).

        accepted: whether conn is kept in the table.
        replaced: the old connection that the caller should close,
                  or None.
        """
        if (
            not isinstance(conn.node_id, str)
            or not conn.node_id
            or conn.node_id == NODE_ID
        ):
            return False, None

        with self._lock:
            if conn.closed:
                return False, None

            existing = self._peers.get(conn.node_id)

            # No previous connection.
            if existing is None:
                self._peers[conn.node_id] = conn
                return True, None

            # The same object is already registered.
            if existing is conn:
                return True, None

            # Replace a connection that is already closed.
            if existing.closed:
                self._peers[conn.node_id] = conn
                return True, existing

            # If two nodes connect simultaneously, prefer the
            # connection initiated by the node with the smaller ID.
            # the case that A and B broadcast at the same time 
            # so if A's ID < B's ID -> take connection from A to B
            prefer_outbound = NODE_ID < conn.node_id

            new_is_preferred = conn.outbound == prefer_outbound
            old_is_preferred = existing.outbound == prefer_outbound

            if new_is_preferred and not old_is_preferred:
                self._peers[conn.node_id] = conn
                return True, existing

            return False, None

    def remove(self, conn):
        """
        Remove only if this exact connection is still registered.
        Return True if removed, otherwise False.
        """
        with self._lock:
            if self._peers.get(conn.node_id) is not conn:
                return False

            del self._peers[conn.node_id]
            return True

    def snapshot(self):
        """Return a new list of currently active Connection objects."""
        with self._lock:
            return [
                conn
                for conn in self._peers.values()
                if not conn.closed
            ]

    def update_files(self, conn, filenames):
        """
        Replace the advertised file list.
        Return False if conn is no longer the current connection.
        """
        with self._lock:
            if (
                self._peers.get(conn.node_id) is not conn
                or conn.closed
            ):
                return False

            conn.files = set(filenames)
            return True

    def get_files(self, node_id):
        """Return a copy of the peer's filenames, or None."""
        with self._lock:
            conn = self._peers.get(node_id)

            if conn is None or conn.closed:
                return None

            return set(conn.files)


# One shared table within this node's process.
peer_table = PeerTable()