'''
files.py: file list, request downloading files, transfer files, store files
handle_data_request(conn, msg) : read file, encode base64, and send
                                 if can't read -> send error response

handle_file_data(conn, msg) :  check status, if success 0 -> decode and store file, otherwise end request
'''
import threading
import base64
import binascii
import shutil
from config import FILE_LIST_INTERVAL
from logger import log_error
from pathlib import Path

from config import NODE_ID, SHARED_FILES, DOWNLOAD_FILES
from connections import send_to
from peers import peer_table
from uuid import uuid4


_pending_requests = {}
_pending_lock = threading.Lock()
SHARED_DIR = Path(SHARED_FILES)
DOWNLOAD_DIR = Path(DOWNLOAD_FILES)


def initialize_directories():
    SHARED_DIR.mkdir(parents=True, exist_ok=True)
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

# get files directly under shared/ or downloads/
# not ../notes.pdf
def is_valid_filename(filename):
    if not isinstance(filename, str):
        return False

    if filename in {"", ".", ".."}:
        return False

    if any(char in filename for char in ("/", "\\", "\x00")):
        return False

    # Reserve this suffix for temporary download/copy files.
    if filename.endswith(".part"):
        return False

    return True

# get local files -> return list of files under directory
def list_shared_files():
    return sorted(
        path.name
        for path in SHARED_DIR.iterdir()
        if (
            path.is_file()
            and not path.is_symlink()
            and is_valid_filename(path.name)
        )
    )

# send files to connected node -> return True if successfully sent
def send_file_list(conn):
    msg = {
        "type": "FILE_LIST",
        "node_id": NODE_ID,
        "files": list_shared_files(),
    }

    return send_to(conn, msg)

# receive list of file
def handle_file_list(conn, msg):
    if msg.get("type") != "FILE_LIST":
        raise ValueError("expected FILE_LIST")

    peer_id = msg.get("node_id")
    filenames = msg.get("files")

    if not isinstance(peer_id, str) or not peer_id:
        raise ValueError("invalid node_id")

    if peer_id != conn.node_id:
        raise ValueError("node_id does not match connection")

    if not isinstance(filenames, list):
        raise ValueError("files must be a list")

    if not all(is_valid_filename(name) for name in filenames):
        raise ValueError("invalid filename in FILE_LIST")

    # update node's peer table
    return peer_table.update_files(conn, filenames)

#loop to send file within 10s
def file_list_loop(stop_event):
    while not stop_event.wait(FILE_LIST_INTERVAL):
        # get active connections
        for conn in peer_table.snapshot():
            if stop_event.is_set():
                return

            try:
                send_file_list(conn)
            except Exception as e:
                log_error(
                    conn.addr,
                    f"periodic FILE_LIST failed: {e}",
                )
                
# start thread to send file list every 10s
def start_file_list_thread(stop_event):
    thread = threading.Thread(
        target=file_list_loop,
        args=(stop_event,),
        daemon=True,
    )
    thread.start()
    return thread

# request file 
def request_file(node_id, filename):
    # 1. Validate filename.
    if not is_valid_filename(filename):
        raise ValueError("invalid filename")

    # 2. Keep the local copy if we already have this name.
    if (SHARED_DIR / filename).exists():
        raise ValueError("file already exists in shared/")

    # 3. Find the peer's current connection.
    conn = peer_table.get(node_id)
    if conn is None:
        raise ValueError("peer is not connected")

    # 4. Check the peer's advertised files.
    advertised_files = peer_table.get_files(node_id)
    if advertised_files is None or filename not in advertised_files:
        raise ValueError("peer does not advertise this file")

    request_id = str(uuid4())

    # 5. Register before sending: a response could arrive immediately.
    with _pending_lock:
        # Prevent concurrent downloads of the same filename,
        # even from different peers.
        if any(
            request["filename"] == filename
            for request in _pending_requests.values()
        ):
            raise ValueError("this file is already being requested")

        _pending_requests[request_id] = {
            "conn": conn,
            "filename": filename,
        }

    # 6. Send outside the pending-request lock.
    msg = {
        "type": "DATA_REQUEST",
        "request_id": request_id,
        "filename": filename,
    }

    try:
        sent = send_to(conn, msg)
    except Exception:
        with _pending_lock:
            _pending_requests.pop(request_id, None)
        raise

    if not sent:
        with _pending_lock:
            _pending_requests.pop(request_id, None)
        raise ConnectionError("could not send DATA_REQUEST")

    return request_id


def send_file_error(conn, request_id, filename, error):
    msg = {
        "type": "FILE_DATA",
        "request_id": request_id,
        "filename": filename,
        "status": "error",
        "error": error,
    }

    return send_to(conn, msg)


def handle_data_request(conn, msg):
    # 1. Validate the message.
    if msg.get("type") != "DATA_REQUEST":
        raise ValueError("expected DATA_REQUEST")

    request_id = msg.get("request_id")
    filename = msg.get("filename")

    if not isinstance(request_id, str) or not request_id:
        raise ValueError("invalid request_id")

    if not is_valid_filename(filename):
        return send_file_error(
            conn,
            request_id,
            filename,
            "INVALID_FILENAME",
        )

    # 2. Locate the file in our shared directory.
    path = SHARED_DIR / filename

    # Do not serve directories or symbolic links.
    if path.is_symlink():
        return send_file_error(
            conn,
            request_id,
            filename,
            "INVALID_FILENAME",
        )

    # 3. Read the file as bytes.
    try:
        with path.open("rb") as file:
            file_bytes = file.read()
    except FileNotFoundError:
        return send_file_error(
            conn,
            request_id,
            filename,
            "FILE_NOT_FOUND",
        )
    except OSError:
        return send_file_error(
            conn,
            request_id,
            filename,
            "READ_FAILED",
        )

    # 4. Convert bytes into a string that JSON can carry.
    encoded_data = base64.b64encode(file_bytes).decode("ascii")

    # 5. Send the complete file in one FILE_DATA message.
    response = {
        "type": "FILE_DATA",
        "request_id": request_id,
        "filename": filename,
        "status": "ok",
        "encoding": "base64",
        "data": encoded_data,
    }

    return send_to(conn, response)



def handle_file_data(conn, msg):
    # 1. Find and verify the pending request.
    if msg.get("type") != "FILE_DATA":
        raise ValueError("expected FILE_DATA")

    request_id = msg.get("request_id")

    if not isinstance(request_id, str) or not request_id:
        raise ValueError("invalid request_id")

    with _pending_lock:
        request = _pending_requests.get(request_id)

        if request is None:
            raise ValueError("unknown request_id")

        if request["conn"] is not conn:
            raise ValueError("response came from the wrong connection")

        filename = request["filename"]

        if msg.get("filename") != filename:
            raise ValueError("response filename does not match request")

    # Unique temporary paths; .part files are not advertised.
    temp_name = f"{request_id}.part"
    download_temp = DOWNLOAD_DIR / temp_name
    shared_temp = SHARED_DIR / temp_name

    download_path = DOWNLOAD_DIR / filename
    shared_path = SHARED_DIR / filename

    try:
        # 2. Handle the peer's error response.
        status = msg.get("status")

        if status == "error":
            error = msg.get("error")

            if not isinstance(error, str) or not error:
                raise ValueError("invalid error response")

            print(f"Download failed for {filename}: {error}")
            return False

        if status != "ok":
            raise ValueError("invalid FILE_DATA status")

        # 3. Validate and decode file content.
        if msg.get("encoding") != "base64":
            raise ValueError("unsupported file encoding")

        encoded_data = msg.get("data")

        if not isinstance(encoded_data, str):
            raise ValueError("FILE_DATA data must be a string")

        try:
            file_bytes = base64.b64decode(
                encoded_data,
                validate=True,
            )
        except (binascii.Error, ValueError) as e:
            raise ValueError("invalid base64 file data") from e

        # Keep a local copy that appeared while the request was pending.
        if shared_path.exists():
            print(f"Keeping existing shared file: {filename}")
            return False

        # 4. Finish writing in downloads/ first.
        with download_temp.open("wb") as file:
            file.write(file_bytes)

        download_temp.replace(download_path)

        # 5. Copy to a temporary file in shared/.
        # The periodic FILE_LIST excludes .part files.
        shutil.copyfile(download_path, shared_temp)

        # Check again before publishing the downloaded copy.
        if shared_path.exists():
            print(f"Keeping existing shared file: {filename}")
            return False

        shared_temp.replace(shared_path)

        print(f"Downloaded successfully: {filename}")
        return True

    finally:
        # Remove temporary files after success or failure.
        # Always release the pending request even if cleanup fails.
        try:
            try:
                download_temp.unlink(missing_ok=True)
            finally:
                shared_temp.unlink(missing_ok=True)
        finally:
            with _pending_lock:
                if _pending_requests.get(request_id) is request:
                    del _pending_requests[request_id]
                    
                    
def on_peer_disconnected(conn):
    # Collect and remove requests belonging to this exact connection.
    with _pending_lock:
        cancelled = [
            (request_id, request["filename"])
            for request_id, request in _pending_requests.items()
            if request["conn"] is conn
        ]

        for request_id, _ in cancelled:
            del _pending_requests[request_id]

    # Notify outside the lock.
    for request_id, filename in cancelled:
        print(
            f"Download cancelled: {filename} "
            f"(peer disconnected, request_id={request_id})"
        )