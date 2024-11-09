import socket
import pickle
import sys

class GFSClient:
    def __init__(self, master_host, master_port):
        self.master_host = master_host
        self.master_port = master_port

    def read(self, filename):
        # Step 1: Request metadata from the master
        master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_socket.connect((self.master_host, self.master_port))
        master_socket.send(pickle.dumps({"type": "READ", "filename": filename}))
        
        response = pickle.loads(master_socket.recv(1024))
        master_socket.close()

        if response["status"] == "File Not Found":
            print("File not found on the server.")
            return

        # Step 3: Request data from chunk servers for each chunk
        for chunk_id, servers in zip(response["chunks"], response["locations"]):
            for server in servers:
                chunk_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                chunk_socket.connect(server)
                chunk_socket.send(pickle.dumps({"type": "READ", "chunk_id": chunk_id}))
                chunk_response = pickle.loads(chunk_socket.recv(1024))
                print(f"Chunk {chunk_id} data: {chunk_response['content']}")
                chunk_socket.close()

    def write(self, filename, write_offset, data):
        # Step 1: Request metadata from master
        master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_socket.connect((self.master_host, self.master_port))
        master_socket.send(pickle.dumps({"type": "WRITE", "filename": filename, "write_offset": write_offset}))
        
        response = pickle.loads(master_socket.recv(1024))
        master_socket.close()

        if response["status"] == "OK":
            chunk_id = response["chunk_id"]
            chunk_servers = response["locations"]

            # Step 3: Write to the primary chunk server
            primary_server = chunk_servers[0]  # Primary chunk server is first
            chunk_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            chunk_socket.connect(primary_server)
            chunk_socket.send(pickle.dumps({"type": "WRITE", "chunk_id": chunk_id, "content": data, "replicas": chunk_servers[1:]}))
            chunk_response = pickle.loads(chunk_socket.recv(1024))
            chunk_socket.close()

            # Step 6: Final acknowledgment from the primary server
            print("Write successful, acknowledgment received.")

# Start the client
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python client.py <filename> <write|read>")
        sys.exit(1)

    filename = sys.argv[1]
    operation = sys.argv[2]
    
    client = GFSClient('127.0.0.1', 5000)  # Hardcoded master address and port
    
    if operation == "read":
        client.read(filename)
    elif operation == "write":
        data = b"Hello, GFS! Writing data to file."
        client.write(filename, 0, data)
    else:
        print("Invalid operation. Use 'read' or 'write'.")
