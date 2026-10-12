"""
node.py - Entry point for the P2P file-sharing agent.

Run:
    python node.py

Commands:
    peers
    files
    local
    get <node_id> <filename>
    quit
"""

import shlex
import threading

import connections
import discovery
import files

from config import NODE_ID, TCP_PORT
from peers import peer_table


def show_help():
    print(
        "\nCommands:\n"
        "  peers                       Show connected peers\n"
        "  files                       Show files advertised by peers\n"
        "  local                       Show files in shared/\n"
        "  get <node_id> <filename>     Request a file\n"
        "  quit                        Exit\n"
        
    )


def show_peers():
    connections_snapshot = peer_table.snapshot()

    if not connections_snapshot:
        print("No connected peers.")
        return

    for conn in connections_snapshot:
        ip, port = conn.addr
        direction = "outgoing" if conn.outbound else "incoming"

        print(
            f"ID: {conn.node_id} | "
            f"Address: {ip}:{port} | "
            f"{direction}"
        )


def show_files():
    connections_snapshot = peer_table.snapshot()

    if not connections_snapshot:
        print("No connected peers.")
        return

    for conn in connections_snapshot:
        filenames = peer_table.get_files(conn.node_id)

        # The peer may have disconnected since the snapshot.
        if filenames is None:
            continue

        print(f"\nPeer: {conn.node_id}")

        if not filenames:
            print("  No advertised files.")
        else:
            for filename in sorted(filenames):
                print(f"  {filename}")


def show_local_files():
    filenames = files.list_shared_files()

    if not filenames:
        print("No files in shared/.")
        return

    print("Local shared files:")

    for filename in filenames:
        print(f"  {filename}")


def command_loop():
    show_help()

    while True:
        line = input("p2p> ")

        try:
            parts = shlex.split(line)
        except ValueError as e:
            print(f"Invalid command: {e}")
            continue

        if not parts:
            continue

        command = parts[0].lower()

        try:
            if command in {"quit", "exit"}:
                if len(parts) != 1:
                    print("Usage: quit")
                    continue
                return

            elif command == "peers":
                show_peers()

            elif command == "files":
                show_files()

            elif command == "local":
                show_local_files()

            elif command == "get":
                if len(parts) != 3:
                    print('Usage: get <node_id> "<filename>"')
                    continue

                node_id = parts[1]
                filename = parts[2]

                request_id = files.request_file(
                    node_id,
                    filename,
                )

                print(f"Request sent: {request_id}")

            else:
                print("Unknown command. Type 'help'.")

        except (ValueError, ConnectionError, OSError) as e:
            print(f"Command failed: {e}")


def main():
    stop_event = threading.Event()

    # Initialize before startup so cleanup also works
    # if only part of startup succeeds.
    server_sock = None
    udp_sock = None
    file_list_thread = None

    try:
        # 1. Prepare local directories.
        files.initialize_directories()

        print(f"Node ID: {NODE_ID}")
        print(f"TCP listening port: {TCP_PORT}")

        # 2. Be ready to accept TCP before advertising our port.
        server_sock = connections.start_tcp_server()

        # 3. Listen for discovery requests and ACKs.
        udp_sock = discovery.create_udp_socket()

        udp_thread = threading.Thread(
            target=discovery.udp_listener,
            args=(udp_sock,),
            daemon=True,
        )
        udp_thread.start()

        # 4. Advertise our file list every 10 seconds.
        file_list_thread = files.start_file_list_thread(
            stop_event
        )

        # 5. Discover peers.
        discovery.send_peer_request(udp_sock)

        # 6. Accept user commands.
        command_loop()

    except (KeyboardInterrupt, EOFError):
        print("\nStopping node...")

    except Exception as e:
        print(f"Node error: {e}")

    finally:
        # Stop future periodic file-list sends.
        stop_event.set()

        # Stop discovery and new incoming connections.
        for sock in (udp_sock, server_sock):
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass

        # Closing peer sockets wakes their receive loops.
        # Those loops perform peer/request cleanup.
        for conn in peer_table.snapshot():
            try:
                conn.close()
            except OSError as e:
                print(f"Could not close peer {conn.node_id}: {e}")

        if file_list_thread is not None:
            file_list_thread.join(timeout=2)

        print("Node stopped.")


if __name__ == "__main__":
    main()