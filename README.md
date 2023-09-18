# Distributed-Storage
In this coursework you will build a distributed storage system. This will involve knowledge 
of Java, networking and distributed systems. The system has one Controller and N Data 
Stores (Dstores). It supports multiple concurrent clients sending store, load, list, remove 
requests. You will implement Controller and Dstores; the client will be provided. Each file 
is replicated R times over different Dstores. Files are stored by the Dstores, the 
Controller orchestrates client requests and maintains an index with the allocation of files 
to Dstores, as well as the size of each stored file. The client actually gets the files directly 
from Dstores – which improves scalability. For simplicity, all these processes will be on 
the same machine, but the principles are similar to a system distributed over several 
servers. Files in the distributed storage are not organised in folders and sub-folders. 
Filenames do not contain spaces.
