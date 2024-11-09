

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

##### Summary:
- **Single primary per chunk** at any time.
- The **master grants leases** to chunkservers to ensure a single primary for each chunk.
- **Lease renewal** ensures a chunkserver remains the primary until it fails to refresh the lease.
- The **master** assigns a new primary if it detects a failure to refresh the lease.

This approach helps maintain consistency in GFS by ensuring that write operations are coordinated through a single primary for each chunk.

### RecordAppend Operation 
- Client → Master: Requests chunk handle and replica locations for the last chunk in the file.
- Master → Client: Provides chunk handle and primary and secondary replica locations.
- Client → Chunk Servers: Pushes data to all chunk servers.
- Client → Primary: Sends record append request to the primary chunk server.
- Primary: Determines offset and applies append; forwards the request to secondary chunk servers.
- Secondary → Primary: Acknowledge successful append.
- Primary → Client: Acknowledges completion of the record append operation.



