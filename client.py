import socket
import json
import sys

class Client:
    def __init__(self, master_host, master_port):
        self.master_host = master_host
        self.master_port = master_port

    def read(self, filename):
        print("Reading file:", filename)
        request = {"type": "READ", "filename": filename}

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.master_host, self.master_port))
            s.send(json.dumps(request).encode())
            response = json.loads(s.recv(1024))

        # Check if the response contains an error message
        if response.get("status") != "OK":
            print("Error:", response.get("message", "Unknown error"))
            return

        print("File found, retrieving chunks...")
        for chunk_id, servers in zip(response["chunks"], response["locations"]):
            for server in servers:
                self.retrieve_chunk_data(server, chunk_id)

    def write(self, filename, data):
        print("Writing data to file:", filename)
        request = {"type": "WRITE", "filename": filename}  # Removed write_offset

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.master_host, self.master_port))
            s.send(json.dumps(request).encode())
            response = json.loads(s.recv(1024))

        if response.get("status") != "OK":
            print("Error:", response.get("message", "Unknown error"))
            return

        chunk_id = response["chunk_id"]
        primary_server = response["primary"]
        servers = response["locations"]

        # Send data to the primary chunkserver
        self.send_chunk_data(tuple(primary_server), chunk_id, data, servers)

    def retrieve_chunk_data(self, server, chunk_id):
        # Ensure 'server' is a tuple (host, port) before attempting connection
        if isinstance(server, list):
            server = tuple(server)  # Convert list to tuple if necessary
        
        # Simulating data retrieval from chunk server
        print(f"Retrieving chunk {chunk_id} from {server}")

        # Create a socket to request chunk data from the chunk server
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as chunk_socket:
            chunk_socket.connect(server)  # Connect to the chunk server
            request = {"type": "READ", "chunk_id": chunk_id}
            chunk_socket.send(json.dumps(request).encode())

            # Receive the response from the chunk server
            response = json.loads(chunk_socket.recv(1024))

            # Check if content is in bytes or string and handle accordingly
            content = response.get("content", b'')
            if isinstance(content, bytes):
                print(f"Content of chunk {chunk_id}: {content.decode()}")  # decode only if it's bytes
            else:
                print(f"Content of chunk {chunk_id}: {content}")  # handle as string if it's already decoded

    def send_chunk_data(self, primary_server, chunk_id, data, servers):
        print(f"Sending data to primary server {primary_server} for chunk {chunk_id}")
        request = {"type": "WRITE", "chunk_id": chunk_id, "content": data, "replicas": servers}

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(primary_server)
            s.send(json.dumps(request).encode())
            response = json.loads(s.recv(1024))
            print(f"Write response from primary server: {response}")

# Example usage
if __name__ == "__main__":
    client = Client("127.0.0.1", 5000)
    operation = sys.argv[2]

    filename = sys.argv[1]

    if operation == "write":
        print("Write operation selected.")
        data = input("Please enter the data that you want to write: ")
        client.write(filename, data)
    elif operation == "read":
        print("Read operation selected.")
        client.read(filename)
    else:
        print("Invalid operation. Use 'read' or 'write'.")
