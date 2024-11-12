import os
import socket
import threading
import json
import random

class MasterServer:
    def __init__(self, host, port, root_dir='master_metadata', chunk_size=12):
        self.host = host
        self.port = port
        self.root_dir = root_dir
        self.replication_factor = 3
        self.chunk_servers = []  # List of chunk server addresses
        self.next_chunk_id = 0
        self.lock = threading.Lock()
        self.chunk_size = chunk_size

        # Load metadata from persistent storage if available
        os.makedirs(self.root_dir, exist_ok=True)
        self.file_to_chunks = self.load_metadata('file_to_chunks.json')
        self.chunk_locations = self.load_metadata('chunk_locations.json')
        

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
        elif request == 'RECORD_APPEND':
            response = self.handle_record_append(data['filename'], data.get('data'))
        elif request == 'RECORD_APPEND_RETRY':
            response=self.retrying_append(data['filename'], data.get('data'))
        elif request == 'DELETE':
            response=self.handle_delete(data['filename'])
        elif request == 'RENAME':
            response = self.handle_rename(data['old_filename'], data['new_filename'])


        client_socket.send(json.dumps(response).encode())
        client_socket.close()
    
    def handle_rename(self, old_filename, new_filename):
        # Check if the old filename exists
        if old_filename not in self.file_to_chunks:
            return {"status": "Error", "message": f"File '{old_filename}' not found"}

        # Check if the new filename already exists
        if new_filename in self.file_to_chunks:
            return {"status": "Error", "message": f"File '{new_filename}' already exists"}

        # Perform the renaming in the file metadata
        self.file_to_chunks[new_filename] = self.file_to_chunks.pop(old_filename)

        # Save the updated metadata to disk
        self.save_metadata(self.file_to_chunks, 'file_to_chunks.json')

        return {"status": "OK", "message": f"File '{old_filename}' renamed to '{new_filename}'"}

    
    def handle_delete(self, filename):
        """Handle deletion of a file and its chunks from the distributed system."""
        if filename not in self.file_to_chunks:
            return {"status": "File Not Found"}

        # Retrieve the chunk IDs associated with the file
        chunk_ids = self.file_to_chunks.pop(filename, [])
        
        # Delete the associated chunks from chunk locations and servers
        self.delete_old_chunks(chunk_ids)
        
        # Save updated metadata
        self.save_metadata(self.file_to_chunks, 'file_to_chunks.json')
        self.save_metadata(self.chunk_locations, 'chunk_locations.json')
        
        return {"status": "OK", "message": f"File '{filename}' and associated chunks deleted"}

        


    def retrying_append(self,filename,data):
        if not data:
            return {"status": "Error", "message": "No data provided for writing"}

        with self.lock:
            chunk_id = self.next_chunk_id
            self.next_chunk_id += 1

            # Split file into chunks of 64MB
            chunks = self.split_into_chunks(data)
            if filename not in self.file_to_chunks:
                self.file_to_chunks[filename] = []


            if len(self.chunk_servers) < self.replication_factor:
                return {"status": "Error", "message": "Not enough chunk servers available"}

            chunk_ids = []
            primary_servers = []
            for i, chunk_data in enumerate(chunks):
                chunk_id = self.next_chunk_id
                self.next_chunk_id += 1

                # Randomly select a primary chunk server and two other replicas
                random.shuffle(self.chunk_servers)
                primary_server = self.chunk_servers[0]
                secondary_servers = self.chunk_servers[1:3]

                # Save chunk locations
                self.chunk_locations[chunk_id] = [primary_server] + secondary_servers
                
                self.file_to_chunks[filename].append(chunk_id)

                # Distribute chunks across the chunk servers
                print(f"Assigned chunk {chunk_id} to primary {primary_server}, replicas: {secondary_servers}")
                self.save_metadata(self.file_to_chunks, 'file_to_chunks.json')
                self.save_metadata(self.chunk_locations, 'chunk_locations.json')

                chunk_ids.append(chunk_id)
                primary_servers.append(primary_server)

            # Now send the response including 'primary_servers' key
            return {
                "status": "OK",
                "chunk_ids": chunk_ids,
                "primary_servers": primary_servers,  # Different primary for each chunk
                "locations": [self.chunk_locations[chunk_id] for chunk_id in chunk_ids]  # Locations for each chunk
            }

    def handle_register_chunkserver(self, chunkserver_address):
        with self.lock:
            if chunkserver_address not in self.chunk_servers:
                self.chunk_servers.append(chunkserver_address)
            print(f"Chunk server registered: {chunkserver_address}")
    
    def handle_record_append(self, filename, data):
        if filename not in self.file_to_chunks:
            return {"status": "Error", "message": "File not found"}

        last_chunk_id = self.file_to_chunks[filename][-1]
        last_chunk_location = self.chunk_locations.get(last_chunk_id, [])
        if not last_chunk_location:
            return {"status": "Error", "message": "No chunk servers found for last chunk"}

        # Send back last chunk metadata
        response = {
            "status": "OK",
            "last_chunk_id": last_chunk_id,
            "primary_server": last_chunk_location[0],
            "secondary_servers": last_chunk_location[1:]
        }
        return response

    def handle_read(self, filename):
        if filename not in self.file_to_chunks:
            return {"status": "File Not Found"}

        chunks = self.file_to_chunks[filename]
        locations = []
        for chunk in chunks:
            # Retrieve all servers (primary and replicas) for the chunk
            chunk_servers = self.chunk_locations.get(chunk, [])
            locations.append(chunk_servers)
            
        return {"status": "OK", "chunks": chunks, "locations": locations}

    def handle_write(self, filename, data):
        if not data:
            return {"status": "Error", "message": "No data provided for writing"}

        with self.lock:
            chunk_id = self.next_chunk_id
            self.next_chunk_id += 1

            # Split file into chunks of 64MB
            chunks = self.split_into_chunks(data)
            
            # Remove old chunks associated with the file from metadata and servers
            if filename in self.file_to_chunks:
                old_chunk_ids = self.file_to_chunks[filename]
                self.delete_old_chunks(old_chunk_ids)

            self.file_to_chunks[filename] = []

            if len(self.chunk_servers) < self.replication_factor:
                return {"status": "Error", "message": "Not enough chunk servers available"}

            chunk_ids = []
            primary_servers = []
            for i, chunk_data in enumerate(chunks):
                chunk_id = self.next_chunk_id
                self.next_chunk_id += 1

                # Randomly select a primary chunk server and two other replicas
                random.shuffle(self.chunk_servers)
                primary_server = self.chunk_servers[0]
                secondary_servers = self.chunk_servers[1:3]

                # Save chunk locations
                self.chunk_locations[chunk_id] = [primary_server] + secondary_servers
                self.file_to_chunks[filename].append(chunk_id)

                # Distribute chunks across the chunk servers
                print(f"Assigned chunk {chunk_id} to primary {primary_server}, replicas: {secondary_servers}")
                self.save_metadata(self.file_to_chunks, 'file_to_chunks.json')
                self.save_metadata(self.chunk_locations, 'chunk_locations.json')

                chunk_ids.append(chunk_id)
                primary_servers.append(primary_server)

            return {
                "status": "OK",
                "chunk_ids": chunk_ids,
                "primary_servers": primary_servers,
                "locations": [self.chunk_locations[chunk_id] for chunk_id in chunk_ids]
            }

    def delete_old_chunks(self, old_chunk_ids):
        """Delete old chunks and their replicas from chunk locations and servers."""
        print(old_chunk_ids,self.chunk_locations)
        for i in range(len(old_chunk_ids)):
            chunk_id=old_chunk_ids[i]
            servers = self.chunk_locations.pop(chunk_id)
            self.remove_chunk_from_servers(chunk_id, servers)
                    
        
        # Save updated mappings
        self.save_metadata(self.chunk_locations, 'chunk_locations.json')
        self.save_metadata(self.file_to_chunks, 'file_to_chunks.json')

    def remove_chunk_from_servers(self, chunk_id, servers):
        """Delete chunk data from primary and replica servers."""
        for server in servers:
            if isinstance(server, list):  # In case server is incorrectly stored as a list
                server = tuple(server)  # Convert to tuple
            
            # Connect to each server to delete chunk file
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(server)  # server should be a tuple (host, port)
                request = {
                    "type": "DELETE_CHUNK",
                    "chunk_id": chunk_id
                }
                s.send(json.dumps(request).encode())
                response = json.loads(s.recv(1024))
                print(f"Deleted chunk {chunk_id} from server {server}: {response['status']}")


    def split_into_chunks(self, data):
        """Split data into chunks of size self.chunk_size"""
        chunks = [data[i:i + self.chunk_size] for i in range(0, len(data), self.chunk_size)]
        return chunks

if __name__ == "__main__":
    master_host = '127.0.0.1'
    master_port = 5000
    master_server = MasterServer(master_host, master_port)
    master_server.start()
