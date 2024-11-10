import socket
import threading
import pickle
import sys

class ChunkServer:
    def __init__(self, host, port, master_host, master_port):
        self.host = host
        self.port = port
        self.master_host = master_host
        self.master_port = master_port
        self.storage = {}  # chunk handle -> chunk data

    def start(self):
        self.register_with_master()
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)
        print(f"Chunk Server started on {self.host}:{self.port}")

        while True:
            client_socket, address = server_socket.accept()
            print("Client connected:", address)
            threading.Thread(target=self.handle_client, args=(client_socket,)).start()

    def register_with_master(self):
        master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_socket.connect((self.master_host, self.master_port))
        master_socket.send(pickle.dumps({
            'type': 'REGISTER_CHUNKSERVER',
            'address': (self.host, self.port)
        }))
        master_socket.close()

    def handle_client(self, client_socket):
        data = pickle.loads(client_socket.recv(1024))

        if data['type'] == 'WRITE':
            chunk_id = data['chunk_id']
            self.storage[chunk_id] = data['content']
            response = {"status": "OK", "chunk_id": chunk_id}

            # If this server is primary, it sends data to replicas
            if self.host == data['replicas'][0][0] and self.port == data['replicas'][0][1]:
                for server in data['replicas'][1:]:
                    self.replicate_write(server, chunk_id, data['content'])

            client_socket.send(pickle.dumps(response))

        elif data['type'] == 'READ':
            chunk_id = data['chunk_id']
            # Retrieve the chunk content
            content = self.storage.get(chunk_id, b'')
            response = {"status": "OK", "content": content}
            client_socket.send(pickle.dumps(response))

        elif data['type'] == 'WRITE_REPLICA':
            chunk_id = data['chunk_id']
            content = data['content']
            # Replicate the chunk data on this server
            self.storage[chunk_id] = content
            response = {"status": "OK", "message": f"Chunk {chunk_id} replicated successfully"}
            client_socket.send(pickle.dumps(response))

        client_socket.close()

    def replicate_write(self, server, chunk_id, content):
        print(f"Replicating chunk {chunk_id} to {server}")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(server)
            request = {"type": "WRITE_REPLICA", "chunk_id": chunk_id, "content": content}
            s.send(pickle.dumps(request))
            response = pickle.loads(s.recv(1024))  # Ensure a response is received
            print(f"Acknowledgment from replica {server}: {response}")

# Start the chunk server
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python chunk_server.py <port>")
        sys.exit(1)

    chunk_server_host = '127.0.0.1'
    chunk_server_port = int(sys.argv[1])  # Take port as command line argument
    master_host = '127.0.0.1'
    master_port = 5000  # Hardcoded master port

    chunk_server = ChunkServer(chunk_server_host, chunk_server_port, master_host, master_port)
    chunk_server.start()
