# common.py
import uuid
import time

CHUNK_SIZE = 64 * 1024 * 1024  # 64 MB
REPLICATION_FACTOR = 3  # Replicate each chunk across 3 chunk servers
HEARTBEAT_INTERVAL = 5  # Interval for heartbeat messages in seconds

class Chunk:
    def __init__(self):
        self.id = uuid.uuid4()  # Unique ID for each chunk
        self.data = b''

class FileMetadata:
    def __init__(self, filename):
        self.filename = filename
        self.chunks = []  # List of chunk IDs for this file

class ChunkServerStatus:
    def __init__(self, server_address):
        self.server_address = server_address
        self.last_heartbeat = time.time()  # Timestamp of last heartbeat
        self.is_alive = True  # Whether the server is alive

    def update_heartbeat(self):
        self.last_heartbeat = time.time()
        self.is_alive = True  # Reset the server status to alive

    def check_heartbeat(self):
        # Check if the chunk server is down by comparing the last heartbeat time
        if time.time() - self.last_heartbeat > HEARTBEAT_INTERVAL:
            self.is_alive = False
        return self.is_alive
