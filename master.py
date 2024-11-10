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
        self.chunk_leases = {}  # Track which chunkserver has the primary lease
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
        # Receive and deserialize the data from the client
        data = pickle.loads(client_socket.recv(1024))
        request = data.get('type')

        # Initialize the response variable
        response = {}

        # Process the request based on the type
        if request == 'REGISTER_CHUNKSERVER':
            self.handle_register_chunkserver(data['address'])
            response = {"status": "OK", "message": "Chunk server registered"}

        elif request == 'READ':
            response = self.handle_read(data['filename'])

        elif request == 'WRITE':
            response = self.handle_write(data['filename'], data['write_offset'])

        # Send the response back to the client
        client_socket.send(pickle.dumps(response))
        client_socket.close()

    def handle_register_chunkserver(self, chunkserver_address):
        # Register the chunk server dynamically
        with self.lock:
            if chunkserver_address not in self.chunk_servers:
                self.chunk_servers.append(chunkserver_address)
            print(f"Chunk server registered: {chunkserver_address}")

    def handle_read(self, filename):
        # Check if the file exists in the system
        if filename not in self.file_to_chunks:
            return {"status": "File Not Found"}

        chunks = self.file_to_chunks[filename]
        locations = []

        # Ensure each chunk has locations assigned
        for chunk in chunks:
            if chunk in self.chunk_locations:
                locations.append(self.chunk_locations[chunk])
            else:
                return {"status": "Error", "message": f"Chunk {chunk} has no location information"}

        return {"status": "OK", "chunks": chunks, "locations": locations}

    def handle_write(self, filename, write_offset):
        with self.lock:
            chunk_id = self.next_chunk_id
            self.next_chunk_id += 1

            if filename not in self.file_to_chunks:
                self.file_to_chunks[filename] = []
            self.file_to_chunks[filename].append(chunk_id)

            if len(self.chunk_servers) < self.replication_factor:
                return {"status": "Error", "message": "Not enough chunk servers available"}

            # Select primary and replica servers
            chunk_servers = self.chunk_servers[:self.replication_factor]
            primary_server = chunk_servers[0]  # Assign the first as primary

            self.chunk_locations[chunk_id] = chunk_servers
            self.chunk_leases[chunk_id] = primary_server  # Record primary lease

            print(f"Assigned primary for chunk {chunk_id} to {primary_server}, replicas: {chunk_servers[1:]}")
            return {"status": "OK", "chunk_id": chunk_id, "locations": chunk_servers, "primary": primary_server}

# Start the master server
if __name__ == "__main__":
    master_host = '127.0.0.1'
    master_port = 5000  # Hardcoded port
    master_server = MasterServer(master_host, master_port)
    master_server.start()
