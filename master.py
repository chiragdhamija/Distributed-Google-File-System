import socket
import threading
import pickle

# Master Server to handle metadata
class MasterServer:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.file_to_chunks = {}  # file path -> list of chunk handles
        self.chunk_locations = {}  # chunk handle -> list of chunk servers
        self.replication_factor = 3
        self.chunk_servers = []  # List of chunk server addresses
        self.next_chunk_id = 1
        self.lock = threading.Lock()

    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)
        print(f"Master server started on {self.host}:{self.port}")

        while True:
            client_socket, address = server_socket.accept()
            print("Client connected:", address)
            threading.Thread(target=self.handle_client, args=(client_socket,)).start()

    def handle_client(self, client_socket):
        data = pickle.loads(client_socket.recv(1024))
        request = data.get('type')

        if request == 'REGISTER_CHUNKSERVER':
            self.handle_register_chunkserver(data['address'])

        elif request == 'READ':
            response = self.handle_read(data['filename'])
        elif request == 'WRITE':
            response = self.handle_write(data['filename'], data['write_offset'])

        client_socket.send(pickle.dumps(response))
        client_socket.close()

    def handle_register_chunkserver(self, chunkserver_address):
        # Register the chunk server dynamically
        with self.lock:
            if chunkserver_address not in self.chunk_servers:
                self.chunk_servers.append(chunkserver_address)
            print(f"Chunk server registered: {chunkserver_address}")

    def handle_read(self, filename):
        if filename in self.file_to_chunks:
            chunks = self.file_to_chunks[filename]
            locations = [self.chunk_locations[chunk] for chunk in chunks]
            return {"status": "OK", "chunks": chunks, "locations": locations}
        return {"status": "File Not Found"}

    def handle_write(self, filename, write_offset):
        with self.lock:
            chunk_id = self.next_chunk_id
            self.next_chunk_id += 1

            # Register chunk metadata
            if filename not in self.file_to_chunks:
                self.file_to_chunks[filename] = []
            self.file_to_chunks[filename].append(chunk_id)

            # Assign chunk servers to the chunk, now from the dynamic list
            if len(self.chunk_servers) < self.replication_factor:
                return {"status": "Error", "message": "Not enough chunk servers available"}
            
            chunk_servers = self.chunk_servers[:self.replication_factor]  # Use only the replication_factor number of chunk servers
            self.chunk_locations[chunk_id] = chunk_servers

            return {"status": "OK", "chunk_id": chunk_id, "locations": chunk_servers}

# Start the master server
if __name__ == "__main__":
    master_host = '127.0.0.1'
    master_port = 5000  # Hardcoded port
    master_server = MasterServer(master_host, master_port)
    master_server.start()
