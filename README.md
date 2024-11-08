

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
- Client → Master: Requests chunk metadata.
- Master → Client: Responds with chunk handles and chunk server locations.
- Client → Chunk Servers: Requests the required data from the closest or specified chunk servers.
- Chunk Server → Client: Sends the data directly back to the client.

### Write Operation 
- Client → Master: Requests chunk handle and replica locations.
- Master → Client: Sends chunk handle, primary, and secondary replica locations.
- Client → Chunk Servers: Pushes data to all chunk servers.
- Client → Primary: Requests write on the primary chunk server.
- Primary → Secondary: Orders the write and forwards it to secondary replicas.
- Secondary → Primary: Acknowledge the write.
- Primary → Client: Acknowledges successful write.

### RecordAppend Operation 
- Client → Master: Requests chunk handle and replica locations for the last chunk in the file.
- Master → Client: Provides chunk handle and primary and secondary replica locations.
- Client → Chunk Servers: Pushes data to all chunk servers.
- Client → Primary: Sends record append request to the primary chunk server.
- Primary: Determines offset and applies append; forwards the request to secondary chunk servers.
- Secondary → Primary: Acknowledge successful append.
- Primary → Client: Acknowledges completion of the record append operation.



