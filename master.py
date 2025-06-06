import os
import socket
import threading
import json
import random
from time import time
import queue


class MasterServer:
    def __init__(self, host, port, root_dir="master_metadata", chunk_size=12):
        self.host = host
        self.port = port
        self.root_dir = root_dir
        self.max_request_threshold = (
            20  # Requests server can handle within threshold_timeout
        )
        self.threshold_timeout = 15  # seconds
        self.replication_factor = 3
        self.chunk_servers = []  # List of chunk server addresses
        self.next_chunk_id = 0
        self.lock = threading.Lock()
        self.chunk_size = chunk_size
        self.chunk_access_times = {}  # Track chunk access times
        self.chunk_modified_replication = {}  # Track chunks modified for replication

        # Heartbeat
        self.heartbeat_data = {}
        self.heartbeat_queue = queue.Queue()
        self.heartbeat_interval = 5  # seconds
        self.heartbeat_failure_threshold = 3 * self.heartbeat_interval  # seconds
        self.max_chunk_server_request_threshold = 20
        self.heartbeat_lock = threading.Lock()
        self.failed_chunk_servers = set()

        # Load metadata from persistent storage if available
        os.makedirs(self.root_dir, exist_ok=True)
        self.file_to_chunks = self.load_metadata("file_to_chunks.json")
        self.chunk_locations = self.load_metadata("chunk_locations.json")

    def load_metadata(self, filename):
        filepath = os.path.join(self.root_dir, filename)
        if os.path.exists(filepath):
            with open(filepath, "r") as f:
                return json.load(f)
        return {}

    def save_metadata(self, data, filename):
        filepath = os.path.join(self.root_dir, filename)
        with open(filepath, "w") as f:
            json.dump(data, f, indent=4)

    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)
        print(f"Master server started on {self.host}:{self.port}")

        # Heartbeats
        threading.Thread(target=self.receive_heartbeats).start()
        threading.Thread(target=self.process_heartbeats).start()
        threading.Thread(target=self.check_failed_servers).start()

        while True:
            client_socket, address = server_socket.accept()
            threading.Thread(target=self.handle_client, args=(client_socket,)).start()

    def handle_client(self, client_socket):
        data = json.loads(client_socket.recv(1024))
        request = data.get("type")
        response = {}

        if request == "REGISTER_CHUNKSERVER":
            self.handle_register_chunkserver(data["address"])
            response = {"status": "OK", "message": "Chunk server registered"}
        elif request == "READ":
            response = self.handle_read(data["filename"])
        elif request == "WRITE":
            response = self.handle_write(data["filename"], data.get("data"))
        elif request == "RECORD_APPEND":
            response = self.handle_record_append(data["filename"], data.get("data"))
        elif request == "RECORD_APPEND_RETRY":
            response = self.retrying_append(data["filename"], data.get("data"))
        elif request == "DELETE":
            response = self.handle_delete(data["filename"])
        elif request == "RENAME":
            response = self.handle_rename(data["old_filename"], data["new_filename"])
        elif request == "WRITE_OFFSET":
            response = self.handle_write_offset(
                data["filename"], data["data"], data["offset"]
            )

        # print(f"DEBUG: Sending response {response} for {response} to client {client_socket}")
        client_socket.send(json.dumps(response).encode())
        client_socket.close()

    def receive_heartbeats(self):
        """
        Receive heartbeats from chunk servers and update the heartbeat data.
        """
        # Create TCP socket to receive heartbeats
        heartbeat_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        heartbeat_socket.bind((self.host, self.port + 1))
        print(f"Heartbeat listener started on {self.host}:{self.port + 1}")

        while True:
            try:
                data = heartbeat_socket.recvfrom(1024)
                heartbeat_data = json.loads(data[0].decode())

                # Check if heartbeat
                if heartbeat_data.get("type") != "HEARTBEAT":
                    print(f"Invalid data in heartbeat port: {heartbeat_data}")
                    continue

                chunk_server_id = heartbeat_data["chunk_server_id"]
                timestamp = heartbeat_data["timestamp"]
                num_requests = heartbeat_data["num_requests"]

                # print(f"Received heartbeat from chunk server {chunk_server_id}, {timestamp}, {num_requests}")
                self.heartbeat_queue.put((chunk_server_id, timestamp, num_requests))

            except Exception as e:
                print(f"Error receiving heartbeat: {e}")

    def process_heartbeats(self):
        """
        Process the heartbeats received from chunk servers.
        """
        while True:
            try:
                if self.heartbeat_queue.empty():
                    continue

                chunk_server_id, timestamp, num_requests = self.heartbeat_queue.get()

                # print(f"Processing heartbeat from chunk server {chunk_server_id}")

                # Check if the chunk server is already in the failed list and remove it since it is now active
                if chunk_server_id in self.failed_chunk_servers:
                    print(f"Chunk server {chunk_server_id} is now active")
                    self.failed_chunk_servers.remove(chunk_server_id)

                # Check if requests are beyond the threshold
                if num_requests > self.max_chunk_server_request_threshold:
                    print(
                        f"Chunk server {chunk_server_id} exceeded request threshold ({num_requests})"
                    )
                    print("Replicating chunks...")
                    self.handle_server_replication(chunk_server_id, False)

                # Save the heartbeat data
                with self.heartbeat_lock:
                    self.heartbeat_data[chunk_server_id] = {
                        "timestamp": timestamp,
                        "num_requests": num_requests,
                    }

            except Exception as e:
                print(f"Error processing heartbeat: {e}")

    def check_failed_servers(self):
        """
        Iterate over the heartbeat data to check for failed servers.
        """
        while True:
            try:
                with self.heartbeat_lock:
                    for chunk_server_id, data in self.heartbeat_data.items():
                        current_time = time()
                        last_heartbeat = data["timestamp"]
                        if (
                            current_time - last_heartbeat
                            > self.heartbeat_failure_threshold and
                            chunk_server_id not in self.failed_chunk_servers
                        ):
                            print(f"Chunk server {chunk_server_id} failed")
                            self.failed_chunk_servers.add(chunk_server_id)
                            print("Replicating chunks...")
                            self.handle_server_replication(chunk_server_id, True)

            except Exception as e:
                print(f"Error checking failed servers: {e}")

    def get_server_list(self, server):
        """
        Function to convert server id string to list
        """
        host, port = server.split(":")
        return [host, int(port)]

    def handle_server_replication(self, server_details, failed):
        """
        Replicate chunks from a server to other servers.
        """

        server_list = self.get_server_list(server_details)

        found = False

        for chunk_id, servers in self.chunk_locations.items():
            if server_list in servers:

                found = True

                print(f"DEBUG: Replicating chunk {chunk_id} from server {server_list}")

                req = self.handle_increase_replication(chunk_id)

                if failed:
                    # Remove the failed server from the chunk locations
                    servers.remove(server_list)
                    self.chunk_locations[chunk_id] = servers
                    self.save_metadata(self.chunk_locations, "chunk_locations.json")

                if not req:
                    print(
                        f"Failed to replicate chunk {chunk_id} from server {server_list}"
                    )
                else:
                    print(
                        f"Successfully replicated chunk {chunk_id} from server {server_list}"
                    )

        if not found:
            print(f"No chunks found for server {server_list}")
        else:
            print(f"Replication complete for server {server_list}")

    def handle_increase_replication(self, chunk_id):
        """
        Increase the replication factor for a chunk by adding a new replica server.
        """
        if chunk_id not in self.chunk_locations:
            print(f"Chunk {chunk_id} not found")
            return False

        # Retrieve the current chunk locations
        current_locations = self.chunk_locations[chunk_id]
        # print(f"DEBUG: Current locations for chunk {chunk_id}: {current_locations}")

        # Randomly select a new replica server from the available chunk servers but not in chunk locations
        # available_servers = set(self.chunk_servers) - set(current_locations)
        available_servers = []
        for server in self.chunk_servers:
            if server not in current_locations:
                available_servers.append(server)

        if not available_servers:
            print("INC_REPL: No available servers to increase replication")
            return False
        else:
            possible_replica_servers = list(available_servers)

        success = False
        for server in current_locations:
            # Connect to each server to replicate chunk
            # Check if server is not failed

            # print(f"DEBUG: {server}")

            copy_server = server.copy()
            # Convert copy_server[1] to string and concatenate
            copy_server = ":".join([copy_server[0], str(copy_server[1])])
            # print(f"DEBUG: copy = {copy_server}, failed = {self.failed_chunk_servers}")
            if copy_server in self.failed_chunk_servers:
                print(f"Server {copy_server} is failed. Skipping...")
                continue

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                if isinstance(server, list):
                    # server[1] += 1
                    copy_server = server.copy()
                    copy_server[1] += 1
                    copy_server = tuple(copy_server)
                s.connect(copy_server)
                request = {
                    "type": "INCREASE_REPLICATION",
                    "chunk_id": chunk_id,
                    "available_servers": possible_replica_servers,
                }
                # print(f"DEBUG: Sending request {request} to server {server}")
                s.sendall(json.dumps(request).encode())
                # print(f"DEBUG: Sent request to server {server}")
                response = json.loads(s.recv(1024))
                # print(f"DEBUG: Received response {response} from server {server}")
                if response.get("status") == "Error":
                    print(
                        f"Failed to increase replication for chunk {chunk_id} on server {server}: {response['message']}.\nTrying next server..."
                    )
                elif response.get("status") == "OK":
                    success = True
                    new_server_replicated = response.get("new_server")
                    print(
                        f"Successfully increased replication for chunk {chunk_id} on server {server}.\nNew replica server: {new_server_replicated}"
                    )
                    current_locations.append(new_server_replicated)
                    self.chunk_locations[chunk_id] = current_locations
                    # print(f"DEBUG: Updated chunk locations: {self.chunk_locations}")
                    self.save_metadata(self.chunk_locations, "chunk_locations.json")
                    break
                else:
                    print(
                        f"Unexpected response from server {server}: {response['status']}"
                    )

        if not success:
            print("INC_REPL: Failed to increase replication for chunk {chunk_id}")
            return False
        else:
            print(f"INC_REPL: Successfully increased replication for chunk {chunk_id}")
            return True

    def handle_rename(self, old_filename, new_filename):
        # Check if the old filename exists
        if old_filename not in self.file_to_chunks:
            return {"status": "Error", "message": f"File '{old_filename}' not found"}

        # Check if the new filename already exists
        if new_filename in self.file_to_chunks:
            return {
                "status": "Error",
                "message": f"File '{new_filename}' already exists",
            }

        # Perform the renaming in the file metadata
        self.file_to_chunks[new_filename] = self.file_to_chunks.pop(old_filename)

        # Save the updated metadata to disk
        self.save_metadata(self.file_to_chunks, "file_to_chunks.json")

        return {
            "status": "OK",
            "message": f"File '{old_filename}' renamed to '{new_filename}'",
        }

    def handle_delete(self, filename):
        """Handle deletion of a file and its chunks from the distributed system."""
        if filename not in self.file_to_chunks:
            return {"status": "File Not Found"}

        # Retrieve the chunk IDs associated with the file
        chunk_ids = self.file_to_chunks.pop(filename, [])

        # Delete the associated chunks from chunk locations and servers
        self.delete_old_chunks(chunk_ids)

        # Save updated metadata
        self.save_metadata(self.file_to_chunks, "file_to_chunks.json")
        self.save_metadata(self.chunk_locations, "chunk_locations.json")

        return {
            "status": "OK",
            "message": f"File '{filename}' and associated chunks deleted",
        }

    def retrying_append(self, filename, data):
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
                return {
                    "status": "Error",
                    "message": "Not enough chunk servers available",
                }

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
                print(
                    f"Assigned chunk {chunk_id} to primary {primary_server}, replicas: {secondary_servers}"
                )
                self.save_metadata(self.file_to_chunks, "file_to_chunks.json")
                self.save_metadata(self.chunk_locations, "chunk_locations.json")

                chunk_ids.append(chunk_id)
                primary_servers.append(primary_server)

            # Now send the response including 'primary_servers' key
            return {
                "status": "OK",
                "chunk_ids": chunk_ids,
                "primary_servers": primary_servers,  # Different primary for each chunk
                "locations": [
                    self.chunk_locations[chunk_id] for chunk_id in chunk_ids
                ],  # Locations for each chunk
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
            return {
                "status": "Error",
                "message": "No chunk servers found for last chunk",
            }

        # Send back last chunk metadata
        response = {
            "status": "OK",
            "last_chunk_id": last_chunk_id,
            "primary_server": last_chunk_location[0],
            "secondary_servers": last_chunk_location[1:],
        }
        return response

    def handle_read(self, filename):
        if filename not in self.file_to_chunks:
            return {"status": "File Not Found"}

        chunks = self.file_to_chunks[filename]
        locations = []
        for chunk in chunks:
            # Retrieve all servers (primary and replicas) for the chunk
            self.record_chunk_access(chunk)
            # print(f"DEBUG: Recorded access for chunk {chunk}")
            chunk_servers = self.chunk_locations.get(chunk, [])
            locations.append(chunk_servers)

        print(f"DEBUG: Read locations: {locations}")

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
                return {
                    "status": "Error",
                    "message": "Not enough chunk servers available",
                }

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
                print(
                    f"Assigned chunk {chunk_id} to primary {primary_server}, replicas: {secondary_servers}"
                )
                self.save_metadata(self.file_to_chunks, "file_to_chunks.json")
                self.save_metadata(self.chunk_locations, "chunk_locations.json")

                chunk_ids.append(chunk_id)
                primary_servers.append(primary_server)

            # print(f"DEBUG: Write locations: {self.chunk_locations}")

            return {
                "status": "OK",
                "chunk_ids": chunk_ids,
                "primary_servers": primary_servers,
                "locations": [self.chunk_locations[chunk_id] for chunk_id in chunk_ids],
            }

    def delete_old_chunks(self, old_chunk_ids):
        """Delete old chunks and their replicas from chunk locations and servers."""
        print(old_chunk_ids, self.chunk_locations)
        for i in range(len(old_chunk_ids)):
            chunk_id = old_chunk_ids[i]
            servers = self.chunk_locations.pop(chunk_id)
            self.remove_chunk_from_servers(chunk_id, servers)

        # Save updated mappings
        self.save_metadata(self.chunk_locations, "chunk_locations.json")
        self.save_metadata(self.file_to_chunks, "file_to_chunks.json")

    def remove_chunk_from_servers(self, chunk_id, servers):
        """Delete chunk data from primary and replica servers."""
        for server in servers:
            if isinstance(
                server, list
            ):  # In case server is incorrectly stored as a list
                server = tuple(server)  # Convert to tuple

            # Connect to each server to delete chunk file
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(server)  # server should be a tuple (host, port)
                request = {"type": "DELETE_CHUNK", "chunk_id": chunk_id}
                s.send(json.dumps(request).encode())
                response = json.loads(s.recv(1024))
                print(
                    f"Deleted chunk {chunk_id} from server {server}: {response['status']}"
                )

    def split_into_chunks(self, data):
        """Split data into chunks of size self.chunk_size"""
        chunks = [
            data[i : i + self.chunk_size] for i in range(0, len(data), self.chunk_size)
        ]
        return chunks

    def get_last_chunk_size(self, filename):
        # Retrieve the chunk IDs for the file
        chunk_ids = self.file_to_chunks.get(filename, [])

        if not chunk_ids:
            return {"status": "Error", "message": "File has no chunks"}

        # Get the last chunk ID
        last_chunk_id = chunk_ids[-1]

        # Retrieve the primary and secondary servers for the last chunk
        chunk_servers = self.chunk_locations.get(last_chunk_id, [])

        if not chunk_servers:
            return {"status": "Error", "message": "No servers for the last chunk"}

        # Try each server (primary first, then secondary servers) to fetch chunk size
        content = None
        for server in chunk_servers:
            try:
                # Ensure 'server' is a tuple (host, port) before attempting connection
                if isinstance(server, list):
                    server = tuple(server)  # Convert list to tuple if necessary

                print(
                    f"Attempting to retrieve chunk {last_chunk_id} size from server {server}"
                )

                # Create a socket to request chunk size from the server
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as chunk_socket:
                    chunk_socket.connect(server)  # Connect to the server
                    request = {"type": "GET_CHUNK_SIZE", "chunk_id": last_chunk_id}
                    chunk_socket.send(json.dumps(request).encode())

                    # Receive the response from the chunk server
                    response = json.loads(chunk_socket.recv(1024))
                    if response.get("status") == "OK":
                        chunk_size = response.get("chunk_size")
                        print(f"Chunk size for chunk {last_chunk_id}: {chunk_size}")
                        content = chunk_size
                        break  # Exit the loop once chunk size is successfully retrieved

            except (ConnectionRefusedError, socket.timeout):
                print(
                    f"Failed to connect to server {server} for chunk {last_chunk_id}. Trying next server..."
                )

        if content is None:
            print(
                f"Error: Unable to retrieve chunk size for chunk {last_chunk_id} from any available server."
            )
            return {
                "status": "Error",
                "message": "Unable to retrieve chunk size from any available server.",
            }

        return {"status": "OK", "chunk_size": content}

    def handle_write_offset(self, filename, data, offset):
        if filename not in self.file_to_chunks:
            return {"status": "Error", "message": "File not found"}

        # Retrieve existing chunks for the file
        chunk_ids = self.file_to_chunks[filename]
        total_data_written = 0
        updated_chunk_info = []

        # Calculate the chunk and position within the chunk for the offset
        chunk_index = offset // self.chunk_size

        chunk_offset = offset % self.chunk_size

        last_chunk_size_response = self.get_last_chunk_size(filename)
        if last_chunk_size_response["status"] != "OK":
            return last_chunk_size_response  # Return the error response if size retrieval fails

        last_chunk_size = last_chunk_size_response["chunk_size"]

        if chunk_index >= len(chunk_ids):
            # Adjust the chunk_index and chunk_offset to the end of the last chunk
            chunk_index = len(chunk_ids) - 1
            chunk_offset = (
                last_chunk_size  # Start appending from the end of the last chunk
            )
        # Remove chunks beyond the offset

        chunks_to_delete = chunk_ids[chunk_index + 1 :]
        self.delete_old_chunks(chunks_to_delete)
        chunk_ids = chunk_ids[: chunk_index + 1]

        # Handle writing starting at the specified offset
        for idx, chunk_id in enumerate(chunk_ids[chunk_index:], start=chunk_index):
            data_to_write = data[
                total_data_written : total_data_written + self.chunk_size - chunk_offset
            ]
            total_data_written += len(data_to_write)

            if not data_to_write:
                break

            # Add updated chunk details
            updated_chunk_info.append(
                {
                    "chunk_id": chunk_id,
                    "chunk_offset": chunk_offset if idx == chunk_index else 0,
                    "primary_server": self.chunk_locations[chunk_id][0],
                    "servers": self.chunk_locations[chunk_id],
                }
            )
            chunk_offset = 0  # Reset offset after the first chunk

        # Allocate new chunks if needed
        while total_data_written < len(data):
            new_chunk_id = self.next_chunk_id
            self.next_chunk_id += 1

            # primary_server = random.choice(self.chunk_servers)
            random.shuffle(self.chunk_servers)
            primary_server = self.chunk_servers[0]
            secondary_servers = self.chunk_servers[1:3]
            self.chunk_locations[new_chunk_id] = [primary_server] + secondary_servers
            self.file_to_chunks[filename].append(new_chunk_id)
            chunk_ids.append(new_chunk_id)
            # Distribute chunks across the chunk servers
            print(
                f"Assigned chunk {new_chunk_id} to primary {primary_server}, replicas: {secondary_servers}"
            )

            data_to_write = data[
                total_data_written : total_data_written + self.chunk_size
            ]
            total_data_written += len(data_to_write)

            updated_chunk_info.append(
                {
                    "chunk_id": new_chunk_id,
                    "chunk_offset": 0,
                    "primary_server": self.chunk_locations[new_chunk_id][0],
                    "servers": self.chunk_locations[new_chunk_id],
                }
            )

        # Save metadata
        self.file_to_chunks[filename] = chunk_ids
        self.save_metadata(self.file_to_chunks, "file_to_chunks.json")
        self.save_metadata(self.chunk_locations, "chunk_locations.json")

        return {"status": "OK", "chunk_info": updated_chunk_info}

    def record_chunk_access(self, chunk_id):
        """
        Record the access time for a chunk and increase replication if necessary.
        """
        current_time = time()

        if not chunk_id in self.chunk_access_times:
            self.chunk_access_times[chunk_id] = []

        self.chunk_access_times[chunk_id].append(current_time)

        self.chunk_access_times[chunk_id] = [
            ts
            for ts in self.chunk_access_times[chunk_id]
            if current_time - ts < self.threshold_timeout
        ]

        if chunk_id not in self.chunk_modified_replication:
            if len(self.chunk_access_times[chunk_id]) > self.max_request_threshold:
                # print(
                #     f"DEBUG: Chunk {chunk_id} accessed {len(self.chunk_access_times[chunk_id])} times"
                # )
                self.chunk_modified_replication[chunk_id] = 4
                req_resp = self.handle_increase_replication(chunk_id)
        else:
            if (
                len(self.chunk_access_times[chunk_id])
                > self.chunk_modified_replication[chunk_id]
            ):
                # print(
                #     f"DEBUG: Chunk {chunk_id} accessed {len(self.chunk_access_times[chunk_id])} times. Modified replication: {self.chunk_modified_replication[chunk_id]}"
                # )
                self.chunk_modified_replication[chunk_id] += 1
                req_resp = self.handle_increase_replication(chunk_id)

        # print(
        #     f"Chunk access times: {self.chunk_access_times}, Chunk modified replication: {self.chunk_modified_replication.get(chunk_id, 0)}"
        # )
        return


if __name__ == "__main__":
    master_host = "127.0.0.1"
    master_port = 5000
    master_server = MasterServer(master_host, master_port)
    master_server.start()
