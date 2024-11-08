# master.py
import socket
import threading
import pickle
import random
import time
from common import FileMetadata, REPLICATION_FACTOR,HEARTBEAT_INTERVAL, ChunkServerStatus

class Master:
    def __init__(self):
        self.file_table = {}  # Maps filenames to FileMetadata
        self.chunk_locations = {}  # Maps chunk IDs to a list of chunk server addresses
        self.chunk_servers = []  # List of ChunkServerStatus objects

    def register_chunk_server(self, address):
        status = ChunkServerStatus(address)
        self.chunk_servers.append(status)

    def assign_chunk_servers(self, chunk_id):
        # Select chunk servers for replication
        selected_servers = random.sample(self.chunk_servers, REPLICATION_FACTOR)
        self.chunk_locations[chunk_id] = [server.server_address for server in selected_servers]
        return selected_servers

    def handle_client_request(self, conn, addr):
        request = pickle.loads(conn.recv(1024))
        operation = request.get("operation")
        filename = request.get("filename")
        
        if operation == "READ":
            metadata = self.file_table.get(filename)
            conn.send(pickle.dumps(metadata))
        
        elif operation == "WRITE":
            if filename not in self.file_table:
                self.file_table[filename] = FileMetadata(filename)
            metadata = self.file_table[filename]
            chunk_id = self.create_chunk(filename)
            conn.send(pickle.dumps(chunk_id))
        
        elif operation == "RECORD_APPEND":
            metadata = self.file_table.get(filename)
            chunk_id = metadata.chunks[-1]  # Get the last chunk for record append
            locations = self.chunk_locations[chunk_id]
            conn.send(pickle.dumps((chunk_id, locations)))

        conn.close()

    def create_chunk(self, filename):
        chunk_id = uuid.uuid4()
        self.file_table[filename].chunks.append(chunk_id)
        selected_servers = self.assign_chunk_servers(chunk_id)
        return chunk_id

    def heartbeat_check(self):
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            for server in self.chunk_servers:
                if not server.check_heartbeat():
                    print(f"ChunkServer {server.server_address} is down.")
                    # Handle the failure, maybe replicate or reassign chunks
                    self.handle_chunkserver_failure(server)

    def handle_chunkserver_failure(self, server_status):
        # Example: Replicate chunks from the failed chunk server
        for chunk_id, chunk_servers in self.chunk_locations.items():
            if server_status.server_address in chunk_servers:
                print(f"Re-replicating chunk {chunk_id} from failed server {server_status.server_address}")
                # Reassign to new chunk server (simplified)
                new_server = random.choice(self.chunk_servers)
                while new_server.server_address == server_status.server_address:
                    new_server = random.choice(self.chunk_servers)
                chunk_servers.remove(server_status.server_address)
                chunk_servers.append(new_server.server_address)

    def start_server(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(("localhost", 5000))
        server.listen(5)
        print("Master server is running on port 5000...")

        # Start heartbeat check thread
        threading.Thread(target=self.heartbeat_check, daemon=True).start()

        while True:
            conn, addr = server.accept()
            threading.Thread(target=self.handle_client_request, args=(conn, addr)).start()

if __name__ == "__main__":
    master = Master()
    master.start_server()
