# set up config

import uuid

NODE_ID = str(uuid.uuid4())

UDP_PORT = 54321
TCP_PORT = 50000
SHARED_FILES = "shared"
DOWNLOAD_FILES = "downloads"
LOG_FILE = "log.txt" 

FILE_LIST_INTERVAL = 10   # seconds between FILE_LIST messages
CONNECT_TIMEOUT = 5       # seconds to wait for a TCP connect before giving up