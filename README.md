# Pending Tasks
- heartbeat
- lease grant
- buffer error
- dynamic replication
- changing names of commands
- Try to relax the assumptions
- Basic file locking (multiple reads and single update)
- Report 
- Presentation

Dynamic Replication:
- Call `record_chunk_access` in `master.py` at appropriate locations.
- (Heartbeat) Get request data from servers, replicate most accessed if above threshold
- (Heartbeat) If chunk server fails, replicate all files in chunk server.

# Assumptions 
- An update operation can be done if and only if the primary and secondary servers both are alive for the file
# How to run the code
- run the bash file precompile.sh
- run the master server by running python3 master.py
- run atleast 3 chunkservers by running python3 chunkserver.py <port_number>
- run client by python3 client.py <file_name> <operation> 
- operations that can be run in the client are read, write.
## GFS Architecture 
### Single master multiple chunkservers , accessed by many clients.
### File
- divided into fixed-size chunks
- labelled with 64-bit unique global IDs(called handles)
- stored at chunk servers
- 3-way replicated across chunk servers
- master keeps track of metadata (e.g., which chunks belong to which files)


## GFS Basic Functioning
- Client retrieves metadata for operation from master
- Read/Write data flows between client and chunkserver
- Minimizing the master’s involvement in read/write operations alleviates the single-master bottleneck

## Chunks 
- Larger blocks of size 64 MB (analogous to FS blocks, except larger)

## GFS Master
- A process running on a separate machine Initially, GFS supported just a single master, but then
they added master replication for fault-tolerance in
other versions/distributed storage systems
- Stores all metadata
 	-File and chunk namespaces
		-Hierarchical namespace for files, flat namespace for chunks
- File-to-chunk mappings
- Locations of a chunk’s replicas

## Chunk Locations
- Kept in memory, no persistent states : Master polls chunkservers at startup
- master can restart and recover chunks from chunkservers
- Note that the hierarchical file namespace is kept on durable storage in the master

## GFS Master <--> ChunkServers (Heartbeat mechanism)
- Master and chunkserver communicate regularly
(heartbeat):
⚫ Is a chunkserver down?
⚫ Are there disk failures on a chunkserver?
⚫ Are any replicas corrupted?
⚫ Which chunks does a chunkserver store?

- Master sends instructions to chunkserver:
⚫ Delete a chunk
⚫ Create a new chunk
⚫ Replicate and start serving this chunk (chunk migration)


## GFS Operations  - Read , Write , RecordAppend
### Read Operation 
Step 1 (Client → Master): File name/path (requesting metadata for the complete file).
Step 2 (Master → Client): Chunk handles, chunkserver locations (primary and replicas), replication information.
Step 3 (Client → Chunkservers): For each chunk:
Chunk handle,
,Read offset (start of the chunk)
,Length (entire chunk size)
Step 4 (Chunkservers → Client): Data for the requested chunk (entire chunk data).
Step 5 (Client Reassembles Data): The client combines the chunks in the correct order to reconstruct the entire file.


### Write Operation 
Step 1 (Client → Master): File name/path, write offset.
Step 2 (Master → Client): Chunk handles, chunkserver locations, replication factor.
Step 3 (Client → Primary Chunkserver): File chunk number, write offset, data to be written.
Step 4 (Primary Chunkserver → Replica Chunkservers): Written data, chunk handle, metadata.
Step 5 (Replica Chunkservers → Primary Chunkserver): Acknowledgment of successful write.
Step 6 (Primary Chunkserver → Client): Final acknowledgment of successful write, status message.

Primary enforces one update order across all replicas for concurrent writes.
It also waits until a write finishes at the otherreplicas before it replies.

Yes, the explanation you provided is **correct**. Here's a slightly refined version for clarity:

#### Selecting a Primary in GFS:
- **For correctness**, at any time, there must be **one single primary** for each chunk. This is crucial to avoid inconsistencies and conflicts during write operations.
  
- To ensure that only one chunkserver is the primary for each chunk, **GFS uses leases**. A lease is a mechanism that ensures that only one chunkserver has the right to act as the primary for a given chunk at any given time.

- **Lease Granting Process**:
  1. The **master** server selects a chunkserver and grants it a **lease** for a particular chunk.
  2. The chunkserver that receives the lease becomes the **primary** for that chunk and is responsible for coordinating write operations for the chunk.
  3. The chunkserver holds the lease for a period **T**. During this period, it **behaves as the primary** for the chunk.

- **Lease Renewal**:
  - The chunkserver can **refresh the lease** indefinitely, as long as it can successfully communicate with the master server.
  - If the chunkserver **fails to refresh the lease** (due to network failure or other issues), it **loses the lease** and stops being the primary for that chunk.

- **Master's Role in Lease Management**:
  - If the master **doesn't hear** from the primary chunkserver within a specified timeout period (due to failure or disconnection), it **revokes the lease** and grants it to another chunkserver.
  
- **At Any Time**:
  - There is **at most one primary** chunkserver for a given chunk.
  - Different chunkservers can be the primary for **different chunks**, allowing parallel writes to different chunks without interference.

#

This padding approach helps ensure record integrity, especially in distributed systems with concurrent access.

**Record Append**

- The client specifies only the data, not the file offset.
- File offset is chosen by the primary.



**Record Append Steps**

1. Application originates a record append request.
2. GFS client translates request and sends it to master.
3. Master responds with chunk handle and (primary + secondary) replica locations.
4. Client pushes write data to all locations.
5. Primary checks if record fits in specified chunk.
6. If record does not fit, then:
   - The primary pads the chunk, tells secondaries to do the same, and informs the client of the inability/error.
   - Client then retries the append with the next chunk.
       - The primary chunk server for the next chunk may not be the same as this primary! So client has to retry.
7. If record fits, then the primary:
   - appends the record at some offset in chunk,
   - tells secondaries to do the same (specifies offset),
   - receives responses from secondaries,
   - and sends final response to the client.

---

- GFS may insert padding data in between different record append operations.
- Preferred that applications use this instead of write.
- Applications should use mechanisms such as checksums with unique IDs to handle padding.


### RecordAppend Operation

1. **Client → Master**: Client sends a request to append data to a specific file with file name and data.
2. **Master → Client**: Master server returns metadata of the last chunk of the file(along with primary and secondary chunkservers locations of last chunk).
3. **Client → Primary Chunkserver**: Client contacts the primary chunkserver to append the data and send it location of secondary chunkservers also to primary.
   
   **Case 1: Last chunk size exceeds chunk size**
   - **Primary Chunkserver**: Checks if the available space in the last chunk is enough to append the data. If not, it pads the chunk with '%' and asks the secondary chunkservers to do the same padding. 
   - **Primary Chunkserver**: Informs the client that there is insufficient space and the client contacts the master server.
   - **Master → Client**: Master creates new chunks, assigns primary and secondary chunkservers, and updates metadata.
   - ** New Primary Chunkserver**: Creates new chunks, replicates them to secondary chunkservers.
   - **New Secondary Chunkservers → New Primary Chunkserver**: Each chunkserver sends acknowledgment after data is appended.
   - **New Primary Chunkserver → Client**: Sends ACK to the client after receiving acknowledgments from all chunkservers.

   **Case 2: Last chunk size fits within chunk size**
   - **Primary Chunkserver**: Appends the data to the last chunk.
   - **Primary Chunkserver → Secondary Chunkservers**: Instructs secondary chunkservers to append the data to replica chunks.
   - **Secondary Chunkservers → Primary Chunkserver**: Sends acknowledgment after appending the data.
   - **Primary Chunkserver → Client**: Sends final acknowledgment to the client after receiving acknowledgment from secondary chunkservers.
**Additional Notes**:
- GFS may insert padding data between different record append operations to ensure data alignment and consistency.
- Applications are encouraged to use mechanisms such as checksums or unique identifiers to distinguish valid records from any padding data, especially in cases where data alignment is critical. 
