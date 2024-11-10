import os
import socket
import threading
import json

class MasterServer:
    def __init__(self, host, port, root_dir='master_metadata', chunk_size=64 * 1024 * 1024):
        self.host = host
        self.port = port
        self.root_dir = root_dir
        self.replication_factor = 3
        self.chunk_servers = []  # List of chunk server addresses
        self.next_chunk_id = 1
        self.lock = threading.Lock()
        self.chunk_size = chunk_size

        # Load metadata from persistent storage if available
        os.makedirs(self.root_dir, exist_ok=True)
        self.file_to_chunks = self.load_metadata('file_to_chunks.json')
        self.chunk_locations = self.load_metadata('chunk_locations.json')
        self.chunk_leases = {}

    def load_metadata(self, filename):
        filepath = os.path.join(self.root_dir, filename)
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                return json.load(f)
        return {}

    def save_metadata(self, data, filename):
        filepath = os.path.join(self.root_dir, filename)
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=4)

    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)
        print(f"Master server started on {self.host}:{self.port}")

        while True:
            client_socket, address = server_socket.accept()
            threading.Thread(target=self.handle_client, args=(client_socket,)).start()

    def handle_client(self, client_socket):
        data = json.loads(client_socket.recv(1024))
        request = data.get('type')
        response = {}

        if request == 'REGISTER_CHUNKSERVER':
            self.handle_register_chunkserver(data['address'])
            response = {"status": "OK", "message": "Chunk server registered"}
        elif request == 'READ':
            response = self.handle_read(data['filename'])
        elif request == 'WRITE':
            response = self.handle_write(data['filename'], data.get('data'))

        client_socket.send(json.dumps(response).encode())
        client_socket.close()

    def handle_register_chunkserver(self, chunkserver_address):
        with self.lock:
            if chunkserver_address not in self.chunk_servers:
                self.chunk_servers.append(chunkserver_address)
            print(f"Chunk server registered: {chunkserver_address}")

    def handle_read(self, filename):
        if filename not in self.file_to_chunks:
            return {"status": "File Not Found"}

        chunks = self.file_to_chunks[filename]
        primary_locations = [self.chunk_locations[chunk][0] for chunk in chunks if chunk in self.chunk_locations]
        return {"status": "OK", "chunks": chunks, "locations": primary_locations}

    def handle_write(self, filename, data):
        if not data:
            return {"status": "Error", "message": "No data provided for writing"}

        with self.lock:
            chunk_id = self.next_chunk_id
            self.next_chunk_id += 1

            # Split file into chunks of 64MB
            chunks = self.split_into_chunks(data)
            self.file_to_chunks[filename] = []

            if len(self.chunk_servers) < self.replication_factor:
                return {"status": "Error", "message": "Not enough chunk servers available"}

            # Assign chunks to servers
            chunk_servers = self.chunk_servers[:self.replication_factor]
            primary_server = chunk_servers[0]

            # Prepare chunking and replication
            chunk_ids = []
            for i, chunk_data in enumerate(chunks):
                chunk_id = self.next_chunk_id
                self.next_chunk_id += 1

                # Save chunk locations
                self.chunk_locations[chunk_id] = chunk_servers
                self.chunk_leases[chunk_id] = primary_server
                self.file_to_chunks[filename].append(chunk_id)

                # Distribute chunks across the chunk servers
                print(f"Assigned chunk {chunk_id} to primary {primary_server}, replicas: {chunk_servers[1:]}")
                self.save_metadata(self.file_to_chunks, 'file_to_chunks.json')
                self.save_metadata(self.chunk_locations, 'chunk_locations.json')

                chunk_ids.append(chunk_id)

            # Now send the response including 'primary_servers' key
            return {
                "status": "OK",
                "chunk_ids": chunk_ids,
                "primary_servers": [primary_server] * len(chunk_ids),  # Use the same primary for all chunks
                "locations": [chunk_servers] * len(chunk_ids)  # Locations for each chunk
            }



    def split_into_chunks(self, data):
        """Split data into chunks of size self.chunk_size"""
        chunks = [data[i:i + self.chunk_size] for i in range(0, len(data), self.chunk_size)]
        return chunks

if __name__ == "__main__":
    master_host = '127.0.0.1'
    master_port = 5000
    master_server = MasterServer(master_host, master_port)
    master_server.start()
