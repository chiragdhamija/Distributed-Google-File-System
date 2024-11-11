
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


## RecordAppend Operation 
**Step 1 (Client → Master)**:  
The application originates a record append request, specifying the **file name** and data to append (without specifying a file offset).

**Step 2 (Master → Client)**:  
The GFS client translates the request and sends it to the master, including the file name. The master responds with the handle of the last chunk in the file and the locations of primary and secondary replicas for that chunk.

**Step 3 (Client → Primary Chunkserver)**:  
The client pushes the data to primary chunkserver for that  last chunk of the related file.

**Step 4 (Primary Chunkserver)**:  
The primary chunkserver checks if the record fits within the specified chunk:

**Step 4a If the record does not fit**:
 The primary pads the remaining space in the chunk with '%', instructs secondary replicas of that chunk to do the same, and informs the client of the issue. The client retries the record append by creating the new chunks of the data to be appended for the filename by informing the master which updated the metadata and decide primary and secondary chunkserver for each replica, and ask for each chunk it asks primary chunkserver to create the chunks and replicate this chunk in secondary servers of this chunk
**Step 4b If the record fits**:
 The primary appends the record at an offset within the chunk, specifies this offset, and instructs the secondary replicas to append the record at the same offset. The primary collects acknowledgments from all secondary replicas.

**Step 5 (Primary Chunkserver → Client)**:  
Once all secondary chunkservers acknowledge the append, the primary sends a final response back to the client, confirming the success of the record append operation.

---

**Additional Notes**:
- GFS may insert padding data between different record append operations to ensure data alignment and consistency.
- Applications are encouraged to use mechanisms such as checksums or unique identifiers to distinguish valid records from any padding data, especially in cases where data alignment is critical. 
