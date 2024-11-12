import socket
import json
import sys

class Client:
    def __init__(self, master_host, master_port):
        self.master_host = master_host
        self.master_port = master_port
    
    def delete(self, filename):
        print("Deleting file: ",filename)
        request = {"type": "DELETE", "filename": filename}

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.master_host, self.master_port))
            s.send(json.dumps(request).encode())
            response = json.loads(s.recv(1024))

        # Check if the response contains an error message
        if response.get("status") != "OK":
            print("Error:", response.get("message", "Unknown error"))
            return
        print(f"{response.get('message')}")

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
        # Fetch and print each chunk from its primary and replica servers
        for chunk_id, servers in zip(response["chunks"], response["locations"]):
            self.retrieve_chunk_data(chunk_id, servers)
        

    def retrieve_chunk_data(self, chunk_id, servers):
        # Try to connect to each server (primary first, then replicas)
        content = None
        for server in servers:
            try:
                # Ensure 'server' is a tuple (host, port) before attempting connection
                if isinstance(server, list):
                    server = tuple(server)  # Convert list to tuple if necessary
                
                print(f"Attempting to retrieve chunk {chunk_id} from server {server}")

                # Create a socket to request chunk data from the server
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as chunk_socket:
                    chunk_socket.connect(server)  # Connect to the server
                    request = {"type": "READ", "chunk_id": chunk_id}
                    chunk_socket.send(json.dumps(request).encode())

                    # Receive the response from the chunk server
                    response = json.loads(chunk_socket.recv(1024))
                    if response.get("status") == "OK":
                        content = response.get("content", "").rstrip('%')
                        print(f"Content of chunk {chunk_id} (without padding): {content}")
                        break  # Exit the loop once data is successfully retrieved

            except (ConnectionRefusedError, socket.timeout):
                print(f"Failed to connect to server {server} for chunk {chunk_id}. Trying next server...")

        if content is None:
            print(f"Error: Unable to retrieve chunk {chunk_id} from any available server.")


        # Write operation in Client
    def write(self, filename, data):
        print("Writing data to file:", filename)
        chunk_size = 12   # 64 MB per chunk
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
            primary_server = response["primary_servers"][idx]  # Select primary server for this chunk
            servers = response["locations"][idx]  # List of servers for replication

            # Send chunk data to the primary chunk server
            self.send_chunk_data(tuple(primary_server), chunk_id, chunk_data, servers)



    

            
    def send_chunk_data(self, primary_server, chunk_id, data, servers):
        print(f"Sending data to primary server {primary_server} for chunk {chunk_id}")
        request = {"type": "WRITE", "chunk_id": chunk_id, "content": data, "replicas": servers}

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(primary_server)
            s.send(json.dumps(request).encode())
            response = json.loads(s.recv(1024))
            print(f"Write response from primary server: {response}")

    def record_append(self,filename,data):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.master_host, self.master_port))
            request = {
                "type": "RECORD_APPEND",
                "filename": filename,
                "data": data
            }
            s.send(json.dumps(request).encode())
            response = json.loads(s.recv(1024))
        
        if response['status'] != "OK":
            print("Error:", response.get("message"))
            return

        primary_server = tuple(response['primary_server'])
        secondary_servers = response['secondary_servers']
        last_chunk_id = response['last_chunk_id']

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(primary_server)
            append_request = {
                "type": "APPEND",
                "chunk_id": last_chunk_id,
                "content": data,
                "secondary_servers": secondary_servers
            }
            s.send(json.dumps(append_request).encode())
            append_response = json.loads(s.recv(1024))

            if append_response['status'] == "Insufficient Space":
                print("Appending required a new chunk. Please retry.")
                self.retry_append(filename,data)

            else:
                print("Data appended successfully.")
    
    def retry_append(self,filename,data):
        print("Retrying append data to file:", filename)
        chunk_size = 12   # 64 MB per chunk
        chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

        request = {"type": "RECORD_APPEND_RETRY", "filename": filename, "data": data}

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
            primary_server = response["primary_servers"][idx]  # Select primary server for this chunk
            servers = response["locations"][idx]  # List of servers for replication

            # Send chunk data to the primary chunk server
            self.send_chunk_data(tuple(primary_server), chunk_id, chunk_data, servers)

    def upload(self, filename, filepath):
        print("Uploading file:", filepath)
        chunk_size = 12

        with open(filepath, 'r') as file:
            data = file.read(chunk_size)
            is_first_chunk = True

            while data:
                if is_first_chunk:
                    print("Performing initial write for first chunk.")
                    self.write(filename, data)
                    is_first_chunk = False
                else:
                    print("Appending subsequent chunk.")
                    self.record_append(filename, data)

                data = file.read(chunk_size)
    def rename(self, old_filename, new_filename):
        print(f"Renaming file from {old_filename} to {new_filename}")
        request = {"type": "RENAME", "old_filename": old_filename, "new_filename": new_filename}

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.master_host, self.master_port))
            s.send(json.dumps(request).encode())
            response = json.loads(s.recv(1024))

        if response.get("status") != "OK":
            print("Error:", response.get("message", "Unknown error"))
        else:
            print(f"{response.get('message')}")


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
    elif operation == "append":
        print("Append Selected")
        data=input("Please enter that you want to append: ")
        client.record_append(filename,data)
    elif operation == "delete" :
        client.delete(filename)
    elif operation == "upload":
        filepath = input("Please enter the path of the file to upload: ")
        client.upload(filename, filepath)
    elif operation == "rename":
        new_filename = input("Enter the new filename: ")
        client.rename(filename, new_filename)
    else:
        print("Invalid operation")
