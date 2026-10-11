# CS158A PA3 – P2P File-Sharing Agent

A peer-to-peer file-sharing agent for a single LAN. Each node:

1. Finds other nodes by broadcasting a UDP `PEER_REQUEST`. Nodes that hear it reply with a `PEER_ACK`.
2. Opens **one** TCP connection to each peer it discovers.
3. Advertises the files in its `shared/` folder with a `FILE_LIST` message, both right after connecting and every 10 seconds after that.
4. Downloads a file from a peer with `DATA_REQUEST` / `FILE_DATA`. The file is saved to `downloads/` and also copied into `shared/`, so this node then shares it too.

Every message the node sends or receives is logged to `log.txt`.

## Project layout

| File             | Purpose                                                                       |
| ---------------- | ----------------------------------------------------------------------------- |
| `node.py`        | Entry point: starts every component and runs the interactive command loop     |
| `config.py`      | Node ID (a random UUID4 on each run), ports, folder names, timers             |
| `discovery.py`   | UDP broadcast discovery (`PEER_REQUEST` / `PEER_ACK`)                         |
| `connections.py` | TCP server, outgoing connects, per-connection receive loop, message dispatch  |
| `peers.py`       | `Connection` object and the thread-safe `PeerTable` (one connection per peer) |
| `protocol.py`    | TCP framing: `send_msg` / `recv_msg` / `recv_exact`                           |
| `files.py`       | File listing, `FILE_LIST`, `DATA_REQUEST`, `FILE_DATA`, saving downloads      |
| `logger.py`      | Thread-safe logging to `log.txt`                                              |

---

## How to run the agent

### Requirements

- Python **3.8+**. Only the standard library is used, so there is nothing to install.
- Every machine must be on the same LAN or subnet, because discovery uses the broadcast address `255.255.255.255`.
- The firewall must allow **UDP 54321** (discovery) and **TCP 50000** (peer connections).
- Run **one node per machine**. Both ports are fixed in `config.py`.

### Start a node

```bash
cd pa3
python3 node.py
```

On startup the node:

1. creates `shared/` and `downloads/` in the current directory if they don't exist,
2. starts listening for TCP on port 50000,
3. starts the UDP listener on port 54321,
4. starts the 10-second `FILE_LIST` timer,
5. broadcasts `PEER_REQUEST` 3 times, 1 second apart,
6. shows the `p2p>` prompt.

Put the files you want to share in `shared/` before or during the run. The folder is re-scanned every time a `FILE_LIST` is sent.

### Commands

| Command                    | Description                                                                                |
| -------------------------- | ------------------------------------------------------------------------------------------ |
| `peers`                    | List connected peers: node ID, address, and whether the connection is incoming or outgoing |
| `files`                    | List the files each connected peer advertises                                              |
| `local`                    | List the files in your own `shared/`                                                       |
| `get <node_id> <filename>` | Download a file from a peer. Quote names that contain spaces: `get <id> "notes.png"`       |
| `help`                     | Show the command list                                                                      |
| `quit` / `exit`            | Stop the node. `Ctrl+C` and `Ctrl+D` also work                                             |

---

## Message format specification

The protocol uses two transports:

| Transport | Port  | Messages                                 |
| --------- | ----- | ---------------------------------------- |
| UDP       | 54321 | `PEER_REQUEST`, `PEER_ACK`               |
| TCP       | 50000 | `FILE_LIST`, `DATA_REQUEST`, `FILE_DATA` |

Every message is a **JSON object encoded in UTF-8**. Every message has a string field **`type`** that tells the receiver what kind of message it is. Field names are case-sensitive.

### Message boundaries

#### UDP: one datagram = one message

UDP keeps message boundaries, so a UDP message has no extra framing. Each `sendto()` sends exactly one JSON object, and each `recvfrom()` receives exactly one.

```
+--------------------------------------+
| JSON object, UTF-8 (whole datagram)  |
+--------------------------------------+
```

- The receive buffer is 4096 bytes. Discovery messages are about 100 bytes.
- A datagram is ignored without a reply if it is not valid UTF-8 JSON, is not an object, or lacks `type` or `node_id`.
- A node ignores datagrams that carry its own `node_id`, because broadcasts loop back to the sender.

#### TCP: 4-byte length prefix

TCP is a byte stream with no message boundaries. One `recv()` can return half a message, or two messages joined together. So each TCP message is sent as a **frame**:

```
[4 byte JSON length][JSON encoded into UTF-8]

```

### Message types

#### 1. `PEER_REQUEST` (UDP broadcast)

Sent to `255.255.255.255:54321` at startup, 3 times, 1 second apart.
{
"type": "PEER_REQUEST",
"node_id": uuid4 | str
}

#### 2. `PEER_ACK` (UDP unicast)

Sent in reply to a `PEER_REQUEST`, back to the address and port the request came from.
{
"type": "PEER_ACK",
"node_id": uuid4 | str,
"tcp_port": int
}

#### 3. `FILE_LIST` (TCP)

Here is all files that node A has, so other nodes can store in their tables
{
"type": "FILE_LIST",
"node_id": uuid4 | str,
"files": ["notes.pdf", "photo.png"]
}

#### 4. `DATA_REQUEST` (TCP)

// request a file to download
{
"type": "DATA_REQUEST",
"filename": str
}

#### 5. `FILE_DATA` (TCP)

The reply to a `DATA_REQUEST`. It has two forms, selected by `status`.

// return what a node requests
{
"type": "FILE_DATA",
"request_id": str
"filename": "notes.pdf",
"status": str,
"encoding": str,
"data": str,
"error": str
}

### How file content is encoded

JSON can't hold raw binary data, so file bytes are converted to text before sending:

1. **Sender:** reads the file as raw bytes, then runs `base64.b64encode(bytes).decode("ascii")`. The result goes in `data`, with `"encoding": "base64"`.
2. The whole file goes in **one** `FILE_DATA` message. There is no chunking, so the 4-byte length prefix covers the entire Base64 string. Base64 makes the data about 33% larger, and the 32-bit length field limits one frame to about 4 GiB.
3. **Receiver:** rejects any `encoding` other than `"base64"`. It decodes with `base64.b64decode(data, validate=True)`, so non-Base64 characters cause an error instead of being silently dropped.
4. **Saving:** the bytes are written to `downloads/<request_id>.part`, then atomically renamed to `downloads/<filename>`. The file is then copied to `shared/<request_id>.part` and renamed to `shared/<filename>`. `.part` files are never advertised, so a peer never sees a half-written file. If a file with that name shows up in `shared/` during the download, the local file is kept.

### Connection rules (one connection per peer)

- Each node keeps at most one live TCP connection per `node_id`.
- Two nodes can connect to each other at the same moment (A→B and B→A). In that case both keep the connection **opened by the node with the smaller `node_id`** (string comparison) and close the other.
- When a connection closes, the peer is removed from the peer table. Any downloads in progress on that connection are cancelled and a message is printed.

### Log format (`log.txt`)

```
<timestamp>  <direction>  <peer ip:port>  <message type>  [detail]
<timestamp>  EVENT     PEER ADDED / PEER REMOVED / DUPLICATE ...
<timestamp>  ERROR     <peer ip:port>  <description>
```

---

## Division of work

> **TODO – fill in before submitting.**

---

## Execution example

### Single node startup (actual output)

The output below is from one node with `shared/notes.txt` and no other nodes on the network:

```
$ python3 node.py
Node ID: edf841e9-fff3-4750-ab9e-af36925e9ed3
TCP listening port: 50000

Commands:
  peers                       Show connected peers
  files                       Show files advertised by peers
  local                       Show files in shared/
  get <node_id> <filename>     Request a file
  help                        Show this help
  quit                        Exit

Example:
  get <node_id> "lecture notes.pdf"

p2p> local
Local shared files:
  notes.txt
p2p> peers
No connected peers.
p2p> quit
Node stopped.
```

`log.txt`:

```
2026-10-11 11:49:47.363  Sent      255.255.255.255:54321  PEER_REQUEST
2026-10-11 11:49:48.369  Sent      255.255.255.255:54321  PEER_REQUEST
2026-10-11 11:49:49.375  Sent      255.255.255.255:54321  PEER_REQUEST
```

### Multi-node demo

> **TODO – paste the real terminal output (or screenshots) from the demo here.**
