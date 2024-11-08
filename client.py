# client.py
import socket
import pickle

class Client:
    def request_master(self, operation, filename):
        request = {
            "operation": operation,
            "filename": filename
        }
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(("localhost", 5000))
            s.send(pickle.dumps(request))
            response = pickle.loads(s.recv(1024))
        return response

    def read(self, filename):
        file_metadata = self.request_master("READ", filename)
        if file_metadata:
            for chunk_id in file_metadata.chunks:
                data = self.request_chunk_server("READ", chunk_id)
                print(data.decode())  # For simplicity, assume text data

    def write(self, filename, data):
        chunk_id = self.request_master("WRITE", filename)
        self.request_chunk_server("WRITE", chunk_id, data.encode())

    def record_append(self, filename, data):
        chunk_id, servers = self.request_master("RECORD_APPEND", filename)
        for server in servers:
            self.request_chunk_server("RECORD_APPEND", chunk_id, data.encode(), server)

    def request_chunk_server(self, operation, chunk_id, data=None, server_port=5001):
        request = {
            "operation": operation,
            "chunk_id": chunk_id,
            "data": data
        }
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(("localhost", server_port))
            s.send(pickle.dumps(request))
            response = s.recv(1024)
        return response

if __name__ == "__main__":
    client = Client()
    client.write("file1", "Hello, GFS!")
    client.read("file1")
    client.record_append("file1", " Appending to GFS!")
    client.read("file1")
