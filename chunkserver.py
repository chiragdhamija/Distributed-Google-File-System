import os
import socket
import threading
import json
import sys

class ChunkServer:
    def __init__(self, host, port, master_host, master_port, storage_dir='chunk_storage'):
        self.host = host
        self.port = port
        self.master_host = master_host
        self.master_port = master_port
        self.storage_dir = f'{storage_dir}_{port}'
        self.chunk_size=12
        os.makedirs(self.storage_dir, exist_ok=True)

    def start(self):
        self.register_with_master()
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)
        print(f"Chunk Server started on {self.host}:{self.port}")

        while True:
            client_socket, address = server_socket.accept()
            threading.Thread(target=self.handle_client, args=(client_socket,)).start()

    def register_with_master(self):
        master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_socket.connect((self.master_host, self.master_port))
        master_socket.send(json.dumps({
            'type': 'REGISTER_CHUNKSERVER',
            'address': (self.host, self.port)
        }).encode())
        master_socket.close()

    def handle_client(self, client_socket):
        data = json.loads(client_socket.recv(1024))
        request = data.get('type')

        if request == 'READ':
            chunk_id = data['chunk_id']
            self.handle_read(client_socket, chunk_id)
        elif request == 'WRITE':
            chunk_id = data['chunk_id']
            content = data['content']
            replicas = data['replicas']
            self.handle_write(client_socket, chunk_id, content, replicas)
        elif request == 'DELETE_CHUNK':
            chunk_id = data['chunk_id']
            self.handle_delete_chunk(client_socket, chunk_id)
        elif request == 'APPEND':
            chunk_id = data['chunk_id']
            content = data['content']
            secondary_servers = data.get('secondary_servers', [])
            self.handle_append(client_socket, chunk_id, content, secondary_servers)
    
    def handle_append(self, client_socket, chunk_id, content, secondary_servers):

        if len(secondary_servers)==2:
            chunk_file=os.path.join(self.storage_dir, f'chunk_{chunk_id}.dat')
        elif len(secondary_servers)==0:
            chunk_file=chunk_file = os.path.join(self.storage_dir, f'chunk_{chunk_id}_replica.dat')

        with open(chunk_file, 'a+') as f:
            f.seek(0, os.SEEK_END)
            current_size = f.tell()

            if len(secondary_servers)==0 or current_size + len(content) > self.chunk_size :
                remaining_space = self.chunk_size - current_size
                f.write('%' * remaining_space)  # Pad with '%'
                if len(secondary_servers)==2:
                    self.send_padding_to_secondary(secondary_servers, chunk_id, remaining_space)
                    response = {"status": "Insufficient Space", "message": "Need new chunk"}
                elif len(secondary_servers)==0:
                    response = {"status": "Replica Padded", "message": "replica padded"}                                  
                
            else:
                f.write(content)
                if len(secondary_servers)==2:
                    self.replicate_append_to_secondary(secondary_servers, chunk_id, content)

                response = {"status": "OK", "message": "Data appended"}

        client_socket.send(json.dumps(response).encode())

    def send_padding_to_secondary(self,replicas,chunk_id,padding_length):
        if not replicas:
            return

        for server in replicas:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(tuple(server))
                request = {
                    "type": "APPEND",
                    "chunk_id": chunk_id,
                    "content": '%' * padding_length,
                    "secondary_servers": []
                }
                s.send(json.dumps(request).encode())
                s.recv(1024)
    
    def replicate_append_to_secondary(self,replicas,chunk_id,content):
        if not replicas:
            return

        for server in replicas:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(tuple(server))
                request = {
                    "type": "APPEND",
                    "chunk_id": chunk_id,
                    "content": content,
                    "secondary_servers": []
                }
                s.send(json.dumps(request).encode())
                s.recv(1024)


    def handle_read(self, client_socket, chunk_id):
        chunk_file = os.path.join(self.storage_dir, f'chunk_{chunk_id}.dat')
        if os.path.exists(chunk_file):
            with open(chunk_file, 'r') as f:
                content = f.read()
                response = {"status": "OK", "content": content}
        else:
            response = {"status": "Error", "message": "Chunk not found"}
        
        print(f"here {response}")
        client_socket.send(json.dumps(response).encode())
        client_socket.close()

    def handle_write(self, client_socket, chunk_id, content, replicas):
        # For the primary server, store as chunk_{chunk_id}.dat
        if len(replicas) == 3:  # Primary server
            chunk_file = os.path.join(self.storage_dir, f'chunk_{chunk_id}.dat')
        elif len(replicas)==0:  # For secondary (replica) servers, store as chunk_{chunk_id}_replica.dat
            chunk_file = os.path.join(self.storage_dir, f'chunk_{chunk_id}_replica.dat')

        with open(chunk_file, 'w') as f:
            f.write(content)
        
        # Acknowledge the client that data was written
        response = {"status": "OK", "message": "Chunk data written"}
        client_socket.send(json.dumps(response).encode())

        # Replicate to secondary servers if on the primary
        if len(replicas) == 3:
            self.replicate_to_secondary_servers(chunk_id, content, replicas)

    def replicate_to_secondary_servers(self, chunk_id, content, replicas):
        if not replicas:
            print("No replicas available for replication.")
            return
        
        for server in replicas[1:]:  # Skip the primary server
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(tuple(server))
                request = {
                    "type": "WRITE",
                    "chunk_id": chunk_id,
                    "content": content,
                    "replicas": []  # No replicas needed for replication; secondary server will handle it
                }
                s.send(json.dumps(request).encode())
                s.recv(1024)  # Await acknowledgment from secondary servers

    def send_acknowledgment_to_primary(self, primary_server, chunk_id):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(tuple(primary_server))
            ack_request = {
                "type": "WRITE_ACK",
                "chunk_id": chunk_id
            }
            s.send(json.dumps(ack_request).encode())
            s.recv(1024)  # Await acknowledgment of write completion

    def handle_delete_chunk(self, client_socket, chunk_id):
        """Delete chunk data from the chunk server."""
        chunk_file = os.path.join(self.storage_dir, f'chunk_{chunk_id}.dat')
        chunk_replica_file = os.path.join(self.storage_dir, f'chunk_{chunk_id}_replica.dat')

        deleted = False

        if os.path.exists(chunk_file):
            os.remove(chunk_file)
            deleted = True
            print(f"Deleted chunk {chunk_id} from {self.storage_dir}")
        
        if os.path.exists(chunk_replica_file):
            os.remove(chunk_replica_file)
            deleted = True
            print(f"Deleted replica chunk {chunk_id} from {self.storage_dir}")
        
        # Send response back to the client
        if deleted:
            response = {"status": "OK", "message": f"Chunk {chunk_id} deleted"}
        else:
            response = {"status": "Error", "message": f"Chunk {chunk_id} not found"}

        client_socket.send(json.dumps(response).encode())
        client_socket.close()

if __name__ == "__main__":
    chunkserver_host = '127.0.0.1'
    chunkserver_port = int(sys.argv[1])
    master_host = '127.0.0.1'
    master_port = 5000
    chunkserver = ChunkServer(chunkserver_host, chunkserver_port, master_host, master_port)
    chunkserver.start()
