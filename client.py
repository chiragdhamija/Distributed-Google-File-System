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
        # Fetch and print each chunk from its primary server
        for chunk_id, primary_server in zip(response["chunks"], response["locations"]):
            self.retrieve_chunk_data(primary_server, chunk_id)

        # Write operation in Client
    def write(self, filename, data):
        print("Writing data to file:", filename)
        chunk_size = 64 * 1024 * 1024  # 64 MB per chunk
        chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

        request = {"type": "WRITE", "filename": filename, "data": data}

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.master_host, self.master_port))
            s.send(json.dumps(request).encode())
            response = json.loads(s.recv(1024))

        if response.get("status") != "OK":
            print("Error:", response.get("message", "Unknown error"))
            return

        # Write each chunk to the primary server and replicate to secondary servers
        for idx, chunk_data in enumerate(chunks):
            chunk_id = response["chunk_ids"][idx]  # Get the chunk ID from the response
            primary_server = response["primary_servers"][idx]
            servers = response["locations"][idx]  # List of servers for replication

            # Send chunk data to the primary chunk server
            self.send_chunk_data(tuple(primary_server), chunk_id, chunk_data, servers)


    def retrieve_chunk_data(self, server, chunk_id):
        # Ensure 'server' is a tuple (host, port) before attempting connection
        if isinstance(server, list):
            server = tuple(server)  # Convert list to tuple if necessary
        
        print(f"Retrieving chunk {chunk_id} from primary server {server}")

        # Create a socket to request chunk data from the primary chunk server
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as chunk_socket:
            chunk_socket.connect(server)  # Connect to the primary server
            request = {"type": "READ", "chunk_id": chunk_id}
            chunk_socket.send(json.dumps(request).encode())

            # Receive the response from the chunk server
            response = json.loads(chunk_socket.recv(1024))
            content = response.get("content", "")

            # Print the content of the chunk
            print(f"Content of chunk {chunk_id}: {content}")
            
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
