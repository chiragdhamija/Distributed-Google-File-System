# Distributed Google File System with Dynamic Replication

## Assumptions 
- An update operation can be done if and only if the primary and secondary servers both are alive for the file
- You cannot create a file with same name if it is already present in the GFS
- No master replication

## How to run the code
- run the bash file precompile.sh
- run the master server by running python3 master.py
- run atleast 3 chunkservers by running python3 chunkserver.py <port_number>
- run client by python3 client.py <file_name> <operation> 
- operations that can be run in the client are read, write.

### GFS Architecture 
#### Single master multiple chunkservers , accessed by many clients.
#### File
- divided into fixed-size chunks
- labelled with 64-bit unique global IDs(called handles)
- stored at chunk servers
- 3-way replicated across chunk servers (w/o dynamic replication)
- master keeps track of metadata (e.g., which chunks belong to which files, which chunks belongs to which chunkserver(both primary and secondary chunkserver))


### GFS Basic Functioning
- Client retrieves metadata for operation from master
- Read/Write data flows between client and chunkserver
- Minimizing the master’s involvement in read/write operations alleviates the single-master bottleneck

### Chunks 
- Larger blocks of size 12 Bytes each (size is kept very low in order to ease testing , can be changed by changing the variable self.chunk_size in each file)

### GFS Master
- A process running on a separate machine . This GFS supports a single master.
- Stores all metadata		
- File-to-chunk mappings
- Locations of a primary and secondary chunkserver for each chunk.


### GFS Operations  - Read , Write , RecordAppend

#### Read Operation (read)
Step 1 (Client → Master): File name/path (requesting metadata for the complete file).
Step 2 (Master → Client): Chunk handles, chunkserver locations (primary and replicas)
Step 3 (Client → Chunkservers): For each chunk: requests the primary chunkserver for the chunk data , if primary doesnt respond to it , client asks the secondary chunkserver for the data
Step 4 (Chunkservers → Client): Data for the requested chunk (entire chunk data).
Step 5 (Client Reassembles Data): The client combines the chunks in the correct order to reconstruct the entire file.


#### Write Operation  (write)
Step 1 (Client → Master): File name/path, write offset.
Step 2 (Master → Client): Location of last Chunk and its replicas, location  new chunks if they are created.
Step 3 (Client → Primary Chunkserver): File chunk number, write offset, data to be written.
Step 4 (Primary Chunkserver → Replica Chunkservers): Primary chunkserver asks the secondary chunkservers to write the data in same order 
Step 5 (Replica Chunkservers → Primary Chunkserver): Acknowledgment of successful write.
Step 6 (Primary Chunkserver → Client): Final acknowledgment of successful write, status message.

Primary enforces one update order across all replicas.
It also waits until a write finishes at the otherreplicas before it replies.


#### RecordAppend Operation (append)

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

## Other several side operations too have been implemented like overwrite, upload , delete and rename a file 
