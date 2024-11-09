import socket
import pickle
import sys

class Client:
    def __init__(self, master_host, master_port):
        self.master_host = master_host
        self.master_port = master_port

    def read(self, filename):
        print("here 1")
        request = {"type": "READ", "filename": filename}

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.master_host, self.master_port))
            s.send(pickle.dumps(request))
            response = pickle.loads(s.recv(1024))

        # Check if the response contains an error message
        if response.get("status") != "OK":
            print("Error:", response.get("message", "Unknown error"))
            return

        print("Read operation response:", response)
        for chunk_id, servers in zip(response["chunks"], response["locations"]):
            for server in servers:
                self.retrieve_chunk_data(server, chunk_id)

    def write(self, filename, data):
        print("here 1")
        request = {"type": "WRITE", "filename": filename, "write_offset": 0}  # Example offset for simplicity

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.master_host, self.master_port))
            s.send(pickle.dumps(request))
            response = pickle.loads(s.recv(1024))

        # Check if the response contains an error message
        if response.get("status") != "OK":
            print("Error:", response.get("message", "Unknown error"))
            return

        print("Write operation response:", response)
        chunk_id = response["chunk_id"]
        servers = response["locations"]
        for server in servers:
            self.send_chunk_data(server, chunk_id, data)

    def retrieve_chunk_data(self, server, chunk_id):
        # Simulating data retrieval from chunk server
        print(f"Retrieving chunk {chunk_id} from {server}")

        # Create a socket to request chunk data from the chunk server
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as chunk_socket:
            chunk_socket.connect(server)  # Connect to the chunk server
            request = {"type": "READ", "chunk_id": chunk_id}
            chunk_socket.send(pickle.dumps(request))

            # Receive the response from the chunk server
            response = pickle.loads(chunk_socket.recv(1024))

            # Check if content is in bytes or string and handle accordingly
            content = response.get("content", b'')
            if isinstance(content, bytes):
                print(f"Content of chunk {chunk_id}: {content.decode()}")  # decode only if it's bytes
            else:
                print(f"Content of chunk {chunk_id}: {content}")  # handle as string if it's already decoded

    def send_chunk_data(self, server, chunk_id, data):
        print(f"Sending data to chunk {chunk_id} on {server}")
        # Simulating data sending to chunk server
        request = {"type": "WRITE", "chunk_id": chunk_id, "content": data, "replicas": [server]}
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(server)
            s.send(pickle.dumps(request))  # Send the data to chunk server
            response = pickle.loads(s.recv(1024))  # Acknowledge response
            print(f"Write response from server: {response}")

# Example usage
if __name__ == "__main__":
    client = Client("127.0.0.1", 5000)
    operation = sys.argv[2]

    filename = sys.argv[1]
    data = "Sample data for write operation"  # Example data

    if operation == "write":
        print("write")
        client.write(filename, data)
    elif operation == "read":
        print("read")
        client.read(filename)
    else:
        print("Invalid operation. Use 'read' or 'write'.")
