# chunkserver.py
import socket
import pickle
import threading
import time
from common import Chunk, ChunkServerStatus, HEARTBEAT_INTERVAL

class ChunkServer:
    def __init__(self, server_port):
        self.server_port = server_port
        self.chunks = {}  # Maps chunk IDs to Chunk objects
        self.status = ChunkServerStatus(f"localhost:{server_port}")  # Server address
        self.is_alive = True

    def store_chunk(self, chunk_id, data):
        self.chunks[chunk_id] = Chunk()
        self.chunks[chunk_id].data = data

    def handle_client_request(self, conn, addr):
        request = pickle.loads(conn.recv(1024))
        operation = request.get("operation")
        chunk_id = request.get("chunk_id")
        
        if operation == "READ":
            data = self.chunks.get(chunk_id).data
            conn.send(data)

        elif operation == "WRITE":
            data = request.get("data")
            self.store_chunk(chunk_id, data)
            conn.send(b"WRITE_SUCCESS")

        elif operation == "RECORD_APPEND":
            data = request.get("data")
            chunk = self.chunks.get(chunk_id)
            if chunk:
                chunk.data += data
                conn.send(b"APPEND_SUCCESS")

        conn.close()

    def heartbeat(self):
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            self.status.update_heartbeat()  # Update heartbeat status

    def start_server(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(("localhost", self.server_port))
        server.listen(5)
        print(f"Chunk server is running on port {self.server_port}...")

        # Start heartbeat thread
        threading.Thread(target=self.heartbeat, daemon=True).start()

        while True:
            conn, addr = server.accept()
            threading.Thread(target=self.handle_client_request, args=(conn, addr)).start()

if __name__ == "__main__":
    chunk_server = ChunkServer(5001)
    chunk_server.start_server()
