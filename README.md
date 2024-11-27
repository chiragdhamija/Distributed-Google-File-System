# Google File System with Dynamic Replication

### Assumptions
- An update operation can only be performed if both the primary and secondary servers are alive for the file.
- A file cannot be created with the same name if it already exists in the GFS.
- No master replication is supported.

### How to Run the Code
1. Run the bash script `precompile.sh`.
2. Start the master server by running `python3 master.py`.
3. Start at least three chunkservers by running `python3 chunkserver.py <port_number>`.
4. Run the client using `python3 client.py <file_name> <operation>`.  

### GFS Architecture
- **Master**: A single master server that tracks metadata.
- **Chunkservers**: Multiple chunkservers that store file chunks.
- **Clients**: Multiple clients interacting with the system.
- **File**:
  - Divided into fixed-size chunks.
  - Each chunk is labeled with a unique 64-bit global ID (called a handle).
  - Chunks are stored across chunkservers.
  - Each chunk is 3-way replicated across chunkservers (without dynamic replication).
  - The master keeps track of metadata, such as which chunks belong to which files, and the primary and secondary chunkservers for each chunk.

### GFS Basic Operation
- The client retrieves metadata for operations from the master.
- Read and write data flows between clients and chunkservers.
- Minimizing the master's involvement in read/write operations alleviates the single-master bottleneck.

### Chunks
- Each chunk is a large block of 12 bytes (this size is kept small for ease of testing, but can be adjusted by changing the `self.chunk_size` variable in each file).

### GFS Master
- The master process runs on a separate machine and supports a single master.
- Stores all metadata, including:
  - File-to-chunk mappings.
  - Locations of the primary and secondary chunkservers for each chunk.

### GFS Operations - Read, Write, and RecordAppend

#### Read Operation (`read`)
1. **Client → Master**: Client sends a request for the file name/path to retrieve metadata.
2. **Master → Client**: Master responds with chunk handles, chunkserver locations (primary and replicas)
3. **Client → Chunkservers**: For each chunk, the client requests data from the primary chunkserver. If the primary doesn't respond, the client contacts the secondary chunkserver.
4. **Chunkservers → Client**: Chunkservers send the requested chunk data.
5. **Client Reassembles Data**: The client combines the chunks in the correct order to reconstruct the entire file.

#### Write Operation (`write`)
1. **Client → Master**: Client sends the file name/path and write offset.
2. **Master → Client**: Master responds with the location of the last chunk, its replicas, and the location of new chunks if they are created.
3. **Client → Primary Chunkserver**: Client sends the file chunk number, write offset, and data to be written.
4. **Primary Chunkserver → Replica Chunkservers**: Primary chunkserver instructs the secondary chunkservers to write the data in the same order.
5. **Replica Chunkservers → Primary Chunkserver**: Secondary chunkservers acknowledge the successful write.
6. **Primary Chunkserver → Client**: Primary chunkserver sends the final acknowledgment of a successful write.

**Note**: The primary chunkserver enforces a consistent update order across all replicas and waits for all replicas to finish writing before responding.

#### RecordAppend Operation (`append`)
1. **Client → Master**: Client sends a request to append data to a specific file with the file name and data.
2. **Master → Client**: Master returns metadata of the last chunk, including primary and secondary chunkserver locations.
3. **Client → Primary Chunkserver**: Client contacts the primary chunkserver to append data and sends the secondary chunkserver locations.
   
   **Case 1: Last Chunk Size Exceeds Chunk Size**
   - **Primary Chunkserver**: Checks if there is enough space in the last chunk. If not, it pads the chunk with '%' and instructs secondary chunkservers to do the same.
   - **Primary Chunkserver**: Informs the client that there is insufficient space, and the client contacts the master.
   - **Master → Client**: Master creates new chunks, assigns primary and secondary chunkservers, and updates metadata.
   - **New Primary Chunkserver**: Creates new chunks and replicates them to secondary chunkservers.
   - **New Secondary Chunkservers → New Primary Chunkserver**: Each chunkserver sends an acknowledgment after appending the data.
   - **New Primary Chunkserver → Client**: Sends an acknowledgment to the client after receiving acknowledgments from all chunkservers.

   **Case 2: Last Chunk Size Fits Within Chunk Size**
   - **Primary Chunkserver**: Appends the data to the last chunk.
   - **Primary Chunkserver → Secondary Chunkservers**: Instructs secondary chunkservers to append the data to replica chunks.
   - **Secondary Chunkservers → Primary Chunkserver**: Sends an acknowledgment after appending the data.
   - **Primary Chunkserver → Client**: Sends the final acknowledgment to the client after receiving acknowledgment from secondary chunkservers.

### Additional Operations
Other side operations such as overwrite, upload, delete, and rename a file have also been implemented.

