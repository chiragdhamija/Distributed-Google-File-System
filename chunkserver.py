import os
import socket
import threading
import json
import sys


class ChunkServer:
    def __init__(
        self, host, port, master_host, master_port, storage_dir="chunk_storage"
    ):
        self.host = host
        self.port = port
        self.master_host = master_host
        self.master_port = master_port
        self.storage_dir = f"{storage_dir}_{port}"
        self.chunk_size = 12
        os.makedirs(self.storage_dir, exist_ok=True)

    def start(self):
        self.register_with_master()
        threading.Thread(target=self.handle_master).start()
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)
        print(f"Chunk Server started on {self.host}:{self.port}")

        while True:
            client_socket, address = server_socket.accept()
            threading.Thread(target=self.handle_client, args=(client_socket,)).start()

    def handle_master(self):
        """
        Function to handle dynamic replication tasks
        """
        master_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_listener.bind((self.host, self.port + 1))
        master_listener.listen(1)  # Accept only one connection for master
        # print(f"DEBUG: Listening for master connection on {self.host}:{self.port + 1}")

        # Accept a single connection from master
        conn, addr = master_listener.accept()
        print(f"DEBUG: Connected to master at {addr}")

        try:
            buffer = b""
            while True:
                chunk = conn.recv(1024)
                if not chunk:
                    continue

                buffer += chunk

                try:
                    data = json.loads(buffer.decode())
                    request = data.get("type")
                    print(f"DEBUG: Received request from master: {request}")

                    if request == "INCREASE_REPLICATION":
                        chunk_id = data["chunk_id"]
                        servers_without_replicas = data["available_servers"]
                        resp = self.increase_replication(
                            chunk_id,
                            servers_without_replicas,
                        )
                        resp["server"] = (self.host, self.port)
                        resp["type"] = "INCREASE_REPLICATION"
                        resp["chunk_id"] = chunk_id

                        conn.send(json.dumps(resp).encode())
                        # print("DEBUG: Response sent to master.")

                except json.JSONDecodeError:
                    print("DEBUG: Invalid JSON received, ignoring...")
                    continue

        except Exception as e:
            print(f"Error handling master request: {e}")
        finally:
            print("DEBUG: Closing connection with master.")
            conn.close()

    def register_with_master(self):
        master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_socket.connect((self.master_host, self.master_port))
        master_socket.send(
            json.dumps(
                {"type": "REGISTER_CHUNKSERVER", "address": (self.host, self.port)}
            ).encode()
        )
        master_socket.close()

    def handle_client(self, client_socket):
        data = json.loads(client_socket.recv(1024))
        request = data.get("type")

        if request == "READ":
            chunk_id = data["chunk_id"]
            self.handle_read(client_socket, chunk_id)
        elif request == "WRITE":
            chunk_id = data["chunk_id"]
            content = data["content"]
            replicas = data["replicas"]
            self.handle_write(client_socket, chunk_id, content, replicas)
        elif request == "DELETE_CHUNK":
            chunk_id = data["chunk_id"]
            self.handle_delete_chunk(client_socket, chunk_id)
        elif request == "APPEND":
            chunk_id = data["chunk_id"]
            content = data["content"]
            secondary_servers = data.get("secondary_servers", [])
            self.handle_append(client_socket, chunk_id, content, secondary_servers)
        elif request == "WRITE_OFFSET":
            chunk_id = data["chunk_id"]
            content = data["content"]
            chunk_offset = data["chunk_offset"]
            replicas = data["replicas"]
            self.handle_write_offset(
                client_socket, chunk_id, content, chunk_offset, replicas
            )
        elif request == "GET_CHUNK_SIZE":
            chunk_id = data["chunk_id"]
            self.get_chunk_size(client_socket, chunk_id)

    def get_chunk_size(self, client_socket, chunk_id):

        primary_chunk_file = os.path.join(self.storage_dir, f"chunk_{chunk_id}.dat")
        replica_chunk_file = os.path.join(
            self.storage_dir, f"chunk_{chunk_id}_replica.dat"
        )

        if os.path.exists(primary_chunk_file):
            chunk_size = os.path.getsize(
                primary_chunk_file
            )  # Get the size of the chunk file
            response = {"status": "OK", "chunk_size": chunk_size}
        elif os.path.exists(replica_chunk_file):
            chunk_size = os.path.getsize(
                replica_chunk_file
            )  # Get the size of the chunk file
            response = {"status": "OK", "chunk_size": chunk_size}
        else:
            response = {"status": "Error", "message": "Chunk file not found"}
        client_socket.send(json.dumps(response).encode())

    def handle_append(self, client_socket, chunk_id, content, secondary_servers):

        if len(secondary_servers) == 2:
            chunk_file = os.path.join(self.storage_dir, f"chunk_{chunk_id}.dat")
        elif len(secondary_servers) == 0:
            chunk_file = chunk_file = os.path.join(
                self.storage_dir, f"chunk_{chunk_id}_replica.dat"
            )

        with open(chunk_file, "a+") as f:
            f.seek(0, os.SEEK_END)
            current_size = f.tell()

            if (
                len(secondary_servers) == 0
                or current_size + len(content) > self.chunk_size
            ):
                remaining_space = self.chunk_size - current_size
                f.write("%" * remaining_space)  # Pad with '%'
                if len(secondary_servers) == 2:
                    self.send_padding_to_secondary(
                        secondary_servers, chunk_id, remaining_space
                    )
                    response = {
                        "status": "Insufficient Space",
                        "message": "Need new chunk",
                    }
                elif len(secondary_servers) == 0:
                    response = {"status": "Replica Padded", "message": "replica padded"}

            else:
                f.write(content)
                if len(secondary_servers) == 2:
                    self.replicate_append_to_secondary(
                        secondary_servers, chunk_id, content
                    )

                response = {"status": "OK", "message": "Data appended"}

        client_socket.send(json.dumps(response).encode())

    def send_padding_to_secondary(self, replicas, chunk_id, padding_length):
        if not replicas:
            return

        for server in replicas:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(tuple(server))
                request = {
                    "type": "APPEND",
                    "chunk_id": chunk_id,
                    "content": "%" * padding_length,
                    "secondary_servers": [],
                }
                s.send(json.dumps(request).encode())
                s.recv(1024)

    def replicate_append_to_secondary(self, replicas, chunk_id, content):
        if not replicas:
            return

        for server in replicas:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(tuple(server))
                request = {
                    "type": "APPEND",
                    "chunk_id": chunk_id,
                    "content": content,
                    "secondary_servers": [],
                }
                s.send(json.dumps(request).encode())
                s.recv(1024)

    def handle_read(self, client_socket, chunk_id):
        # Paths for primary chunk and replica chunk
        primary_chunk_file = os.path.join(self.storage_dir, f"chunk_{chunk_id}.dat")
        replica_chunk_file = os.path.join(
            self.storage_dir, f"chunk_{chunk_id}_replica.dat"
        )

        # Try to read the primary chunk file first
        if os.path.exists(primary_chunk_file):
            with open(primary_chunk_file, "r") as f:
                content = f.read()
                response = {"status": "OK", "content": content}
        # If primary chunk is not found, try the replica
        elif os.path.exists(replica_chunk_file):
            with open(replica_chunk_file, "r") as f:
                content = f.read()
                response = {"status": "OK", "content": content}
        else:
            # Neither primary nor replica chunk file was found
            response = {"status": "Error", "message": "Chunk not found"}

        # Send the response to the client
        print(f"here {response}")
        client_socket.send(json.dumps(response).encode())
        client_socket.close()

    def handle_write(self, client_socket, chunk_id, content, replicas):
        # For the primary server, store as chunk_{chunk_id}.dat
        if len(replicas) == 3:  # Primary server
            chunk_file = os.path.join(self.storage_dir, f"chunk_{chunk_id}.dat")
        elif (
            len(replicas) == 0
        ):  # For secondary (replica) servers, store as chunk_{chunk_id}_replica.dat
            chunk_file = os.path.join(self.storage_dir, f"chunk_{chunk_id}_replica.dat")

        with open(chunk_file, "w") as f:
            f.write(content)

        # Replicate to secondary servers if on the primary
        if len(replicas) == 3:
            self.replicate_to_secondary_servers(chunk_id, content, replicas)

        # Acknowledge the client that data was written
        response = {"status": "OK", "message": "Chunk data written"}
        client_socket.send(json.dumps(response).encode())

    def handle_write_offset(
        self, client_socket, chunk_id, content, chunk_offset, replicas
    ):
        if len(replicas) == 3:  # Primary server
            chunk_file = os.path.join(self.storage_dir, f"chunk_{chunk_id}.dat")
        elif (
            len(replicas) == 0
        ):  # For secondary (replica) servers, store as chunk_{chunk_id}_replica.dat
            chunk_file = os.path.join(self.storage_dir, f"chunk_{chunk_id}_replica.dat")
        # Read existing data and overwrite from the offset
        print(f"here {len(replicas)}")
        existing_data = ""
        if os.path.exists(chunk_file):
            with open(chunk_file, "r") as f:
                existing_data = f.read()

        # Combine existing data with new content
        updated_data = existing_data[:chunk_offset] + content
        with open(chunk_file, "w") as f:
            f.write(updated_data)

        if len(replicas) == 3:
            self.replicate_to_secondary_servers(chunk_id, updated_data, replicas)

        # Acknowledge the client
        response = {"status": "OK", "message": "Offset write completed"}
        client_socket.send(json.dumps(response).encode())

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
                    "replicas": [],  # No replicas needed for replication; secondary server will handle it
                }
                s.send(json.dumps(request).encode())
                s.recv(1024)  # Await acknowledgment from secondary servers

    def handle_delete_chunk(self, client_socket, chunk_id):
        """Delete chunk data from the chunk server."""
        chunk_file = os.path.join(self.storage_dir, f"chunk_{chunk_id}.dat")
        chunk_replica_file = os.path.join(
            self.storage_dir, f"chunk_{chunk_id}_replica.dat"
        )

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

    def increase_replication(self, chunk_id, servers_without_replicas):
        """
        Function to increase replication of a chunk
        """
        print(
            f"DEBUG: Increasing replication for chunk {chunk_id} from {self.host}:{self.port}"
        )

        # Read the chunk data from the current server (could be primary or replica)
        chunk_file = os.path.join(self.storage_dir, f"chunk_{chunk_id}.dat")
        if not os.path.exists(chunk_file):
            print(f"DEBUG: Checking for replica")
            chunk_file = os.path.join(self.storage_dir, f"chunk_{chunk_id}_replica.dat")

        if not os.path.exists(chunk_file):
            print(f"Chunk file {chunk_file} not found on server")
            return {
                "status": "Error",
                "message": "Chunk file not found on server",
                "server": (self.host, self.port),
            }

        with open(chunk_file, "r") as f:
            content = f.read()

        # print(
        #     f"DEBUG: Read chunk {chunk_id} from {self.host}:{self.port}. Content: {content}"
        # )

        # Replicate the chunk to the servers without replicas
        success = 0
        new_replica_server = None
        for server in servers_without_replicas:
            # print(f"DEBUG: Replicating chunk {chunk_id} to {server}")
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                # print(f"DEBUG: Connecting to {server}")
                s.connect(tuple(server))
                # print(f"DEBUG: Connected to {server}")
                request = {
                    "type": "WRITE",
                    "chunk_id": chunk_id,
                    "content": content,
                    "replicas": [],
                }
                # print(f"DEBUG: Sending request to {server}: {request}")
                s.send(json.dumps(request).encode())
                # print(f"DEBUG: Waiting for response from {server}")
                response = s.recv(1024)
                # print(f"DEBUG: Received response from {server}: {response}")
                if json.loads(response).get("status") == "OK":
                    print(
                        f"DEBUG: Successfully replicated chunk {chunk_id} to {server}"
                    )
                    success += 1
                    new_replica_server = server
                    break

        if success == 0:
            print(f"Failed to replicate chunk {chunk_id} to any server")
            return {
                "status": "Error",
                "message": "Failed to replicate chunk",
                "server": (self.host, self.port),
            }
        elif success == 1:
            print(
                f"Successfully replicated chunk {chunk_id} to a new server, {new_replica_server}"
            )
            return {
                "status": "OK",
                "message": "Successfully increased chunk replication",
                "new_server": tuple(new_replica_server),
            }


if __name__ == "__main__":
    chunkserver_host = "127.0.0.1"
    chunkserver_port = int(sys.argv[1])
    master_host = "127.0.0.1"
    master_port = 5000
    chunkserver = ChunkServer(
        chunkserver_host, chunkserver_port, master_host, master_port
    )
    chunkserver.start()
