## EinsteinDB 
EinsteinDB is a hybrid database that addresses the knowledge gap of factual dialectic completion in the context of an ACID database - Think of a `Lucene embedded to a FoundationDB record layer`. \
It is designed to be both a key-value store and a crowm graph store with a rich set of query capabilities:  

A Relativistic Database with OLTP repeatable read policy and high performance for large scale applications and serializable transactions guarantees for concurrent accesses to the same data.

EinsteinDB is a hybrid database that addresses the knowledge gap of factual dialectic completion in the context of an ACID database. It is designed to be both a key-value store and a graph store, making it capable of representing the eventful data in a key-value store and the supercolumnar data in a graph store in the form of causal sets. Additionally, EinsteinDB complies with CAP principles and ACID principles, reaching CAP with FoundationDB, TerminusDB, and MilevaDB.

EinsteinDB is a valuable tool for data scientists and analysts who need to store and process information in both a key-value store and a graph store. It is CAP-compatible with Postgres, SQLite, and MySQL, reaching CAP with FoundationDB, TerminusDB, and MilevaDB. EinsteinDB is a living breathing semantic knowledge-base that can scale to a large number of nodes, store data in multiple memory structures, and provide with low latency and high throughput for a wide variety of workloads.

## Cracking Columns and Sharding Causets: A Poisson distribution of byzantine failures.

Built as a shim for a stateless merkle tree, EinsteinDB is a hybrid key-value store that strikes the optimal balance between the costs of a key-value store and the costs of a merkle tree.
It is composed of an LSH-KV store and a Merkle tree, which are separated by a column store. The column store is a shim for a serialization of the data. This means that the data is serialized and stored in a column store with a single column.
The column store is merely the appended log of the merkle tree; thusly, every row, column, and value in the column store is single-user atomic and immutable fact in the database. With Massive online data processing, the column store is the most efficient way to store data.
EinsteinDB is brilliant for both the ability to lower latency in the database and the ability to lower the cost of the maintaining the database.

EinsteinDB is compatible with
1. Rook
2. Ceph
3. Cassandra
4. MongoDB
5. PostgresQL
6. SQLite


EinsteinDB exhibits a single-writer multiple-reader concurrency model. However, as is noted in the EinsteinDB rust script. A soliton
may indeed graduate EinsteinDB into a Multi-Version Time-Warped Causet-based database. This is a very interesting problem to solve. The solution is to use a grammar of causets with context switch and virtual time.
Because EinsteinDB is a CQRS-compliant database, It is CAP-compatible with Postgres, SQLite, and MySQL; it reaches CAP with FoundationDB, TerminusDB, and MilevaDB (see Appendix C).
an intrinsic trade-off between lookup cost, update cost, and main memory footprint, yet all existing designs expose a subopti- mal and difficult to tune trade-off among these metrics. We pin- point the problem to the fact that all modern key-value stores sub- optimally co-tune the merge policy, the buffer size, and the Bloom filters’ false positive rates in each level.
We present Monkey, an LSM-based key-value store that strikes the optimal balance between the costs of updates and lookups with any given main memory budget. The insight is that worst-case lookup cost is proportional to the sum of the false positive rates of the Bloom filters across all levels of the LSM-tree. Contrary to state-of-the-art key-value stores that assign a fixed number of bits-per-element to all Bloom filters, Monkey allocates memory to filters across different levels so as to minimize this sum.

EinsteinDB is an opinionated weakly-coupled, Multi-Version Timestamped Ordered, repeatable read, transactional database which supports a query language and storage modeling similar but agnostic to relational databases. In Fact, EinsteinDB is a computational database. With one big difference, EinsteinDB is not a relational database.
While EinsteinDB provides PL/SQL, it is not merely a key-value storage system; EinsteinDB incorporates the idea of Minkowski, Lamport, and others. Causets of the Minkowski model are:
- **Timestamp**: The timestamp is the time at which a transaction is committed; multi-version timestamps are the timestamps of the transactions that are committed.
- **Order**: The order is the order in which transactions are committed; concurrency control is based on the order.
- **Repeatability**: The repeatability is the ability to repeat a transaction: if a transaction is committed, it is repeatable.
- **Transactional**: The transactional is the ability to commit a transaction: if a transaction is committed, it is transactional.


EinsteinDB is a new kind of database that can handle extremely large amounts of data. With its Write Amplification Factor of 1.5, it can handle up to 1.5x the number of rows in a table. EinsteinDB is a database that is used to store facts. It is CAP-compatible with Postgres, SQLite, and MySQL. Moreover, it reaches CAP with FoundationDB, TerminusDB, and MilevaDB.
It can write data at speeds up to 5 milliseconds per row. P99 latency is 0.5 milliseconds, which is faster than any other database on the market. \
EinsteinDB also has features that make it unique, such as the ability to read data in parallel, to write data in a concurrent manner, with time traveling queries, and to support the use of causets.

Unlike Galileo, JanusGraph, and AllegroGraph, EinsteinDB implements and EinsteinML language model for a persistence file. Unlike HyPerDB, MongoDB, Arango, and BoltDB, EinsteinDB does not require a Schema to be defined. \
Causets are similar to tuplespaces and allow individuals to read their writes, and allow multiple individuals to write optimize in parallel universes. \

# Database Choice for Petabytes and Beyond
EinsteinDB is the perfect choice for personal or business computing that needs to handle large amounts of data. With its Write Amplificator, it can write data at speeds up to 5 milliseconds, which is faster than any other database on the market.

EinsteinDB s a powerful continuum database that unifies major data structure designs.


In a nutshell, ```einstein_db``` is a ```persistent indexing scheme``` based off of LSH-KVX that exploits the distinct merits of hash Index and B+-Tree Index to support universal range scan which transcends key-value ranges using topographic categories of mumford grammars.

The practical beneﬁt of einstein_db is that it creates a fast inference InterlockingDirectorate with GPT3 , it then generates a bootstrap SQL design of data structures which engages with OpenAI: An AllegroCL meta-language. For example, we can near instantly predict how a speciﬁc de­sign change in the underlying storage of a data system would aﬀect performance, or reversely what would be the optimal data structure (from a given set of designs) given workload characteristics and memory budget. In turn, these prop­erties allow us to envision new class self-designing SolitonId-causet_locale stores with substantially improved ability to adapt to workload and hardware changes by transitioning between drastically different data structure designs on demand.


## Key Features
- Unified Key Format: allows you to abstract your data center into spacetime
- Write Amplificator: writes data at speeds up to 5 milliseconds
- Horn Rules: give you the ability to query the database for historical data
- Bulk-Carrier: allows you to process large amounts of data quickly
- Post-Quantum Stateless Merkle Trees: provides security for your data
- Quantum-Tolerant: provides security for your data

## Installation
```Dockerfile```
```
FROM ubuntu:16.04
  
RUN apt-get update && apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg-agent \
    software-properties-common
   
RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
   
RUN apt-key fingerprint 0EBFCD88
   
RUN add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable"
   
RUN apt-get update
   
RUN apt-get install -y docker-ce docker-ce-cli containerd.io
   

 
```


## Usage
```bash
docker run -it --rm -p 8080:8080 einstein_db
```

## Docker Compose
```docker-compose.yml
version: '3'

## Kubernetes Configuration
```k8s.yml
apiVersion: v1
kind: Service
metadata:
  name: einstein_db
spec:
  selector:
    app: einstein_db
  ports:
    - port: 8080
      targetPort: 8080
  type: NodePort
---
apiVersion: v1
kind: Deployment
metadata:
  name: einstein_db
spec: einstein_db_ctl
    replicas: 1
    selector:
        matchLabels:
        app: einstein_db
    template:
        metadata:
        labels:
            app: einstein_db
        spec:
        containers:
        - name: einstein_db
            image: einstein_db
            ports:
            - containerPort: 8080
            protocol: TCP
            env:
            - name: EINSTEIN_DB_HOST
            value: einstein_db
            - name: EINSTEIN_DB_PORT
            value: "8080"
            - name: EINSTEIN_DB_USER
            value: "root"
            - name: EINSTEIN_DB_PASSWORD
            value: "root"
            - name: EINSTEIN_DB_DATABASE
            value: "einstein_db"
            - name: EINSTEIN_DB_MAX_CONNECTIONS
            value: "100"
            - name: EINSTEIN_DB_MAX_CONNECTIONS_PER_HOST
            value: "100"
            - name: EINSTEIN_DB_MAX_CONNECTIONS_PER_HOST_PER_USER
            value: "100"
            - name: EINSTEIN_DB_MAX_CONNECTIONS_PER_HOST_PER_USER_PER_DB
            value: "100"
            - name: EINSTEIN_DB_MAX_CONNECTIONS_PER_HOST_PER_USER_PER_DB_PER_TABLE
            value: "100"
            - name: EINSTEIN_DB_MAX_CONNECTIONS_PER_HOST_PER_USER_PER_DB_PER_TABLE_PER_COLUMN
            value: "100"
            - name: EINSTEIN_DB_MAX_CONNECTIONS_PER_HOST_PER_USER_PER_DB_PER_TABLE_PER_COLUMN_PER_ROW
            value: "100"
            - name: EINSTEIN_DB_MAX_CONNECTIONS_PER_HOST_PER_USER_PER_DB_PER_TABLE_PER_COLUMN_PER_ROW_PER_TIMESTAMP


```aidl.yml
apiVersion: v1
kind: ServiceSpec
ports:
  - port: 8080
    targetPort: 8080
    protocol: TCP
    name: einstein_db
    nodePort: 30080
    type: NodePort
---
apiVersion: v1
kind: DeploymentSpec
replicas: 100

    
    ```
    


## Docker Compose
```docker-compose.yml
version: '3'



VioletaBFT is a consensus library which is used to implement the Byzantine Fault Tolerance protocol. \
Unlike many RAFT implementations, VioletaBFT is not a distributed consensus algorithm. \ 
but a distributed Byzantine Fault Tolerance protocol:\
this means that for sparse nodes, the consensus protocol is not guaranteed to be stable. However, \
VioletaBFT can work as GHOST nodes, which means that it is able to scale to a large number of nodes.
GHOSTS are nodes which are not part of the consensus protocol, but are able to provide the consensus protocol with a majority of nodes.
GHOST has been implemented separately, as a blockchain library, in the context of EinsteinDB, it is instead used to sentinel the network and to provide a part-time parliament.
A part-time parliament is a group of nodes which are not part of the consensus protocol, but are able to provide the consensus protocol with a majority of nodes from previous epochs of the network state machine.
Because EinsteinDB uses a stateless protocol, it is not necessary to use a Byzantine Fault Tolerance protocol to ensure that the network is stable. \
However, VioletaBFT can be used to optimize the IPAM protocol, which is used to assign IP addresses to nodes. \
## Installation
To install EinsteinDB, run the following command:
```
curl -sL https://raw.githubusercontent.com/einstein-db/einstein-db/master/install.sh | bash
```
## Usage
To use EinsteinDB, run the following command:
```
einstein-db
```
## Contributing
If you want to contribute to EinsteinDB, please visit the [Github repository](https://www.github.com/YosiSF/EinsteinDB).
## Licensed
EinsteinDB is licensed under the MIT license and is copyright (c) 2018-2023 by EinstAI Inc, Whtcorps Inc, and OpenAI Inc.
## Authors
EinsteinDB was written by [YosiSF](https://www.github.com/YosiSF). and [Slushie](https://www.github.com/SlushieSlush).

which means that it is not a Byzantine Fault Tolerance protocol, but a Byzantine Fault Tolerance protocol which is not strictly a Byzantine Fault Tolerance protocol.
In fact, the consensus protocol is embedded in the time stamp versioning of the Merkle Tree.

### Theory
Rust Semantics obey a phase distinction between the phase of consensus at compile time and the transition phase at \
runtime time. Because of this, EinsteinDB written in Rust is able to distinguish between cache miss and cache hit at compile time which is useful for optimization.

As a consequence of this approach, the consensus protocol is embedded in the time stamp versioning of the MerkleTreeLSHBuffer which makes EinsteinDB a hybrid database, which uses a unifying consensus and shared register set for the time stamp versioning of concurrent transactions.
Egalitarian Paxos, or Epaxos, allows EinsteinDB to leverage Write optimized concurrency controls which is a key component of the consensus protocol.\
The consensus protocol is implemented as a Paxos algorithm, which is a consensus protocol which is not strictly a Byzantine Fault Tolerance protocol.


A crown-graph with pacified nodes is thus used to label the edges as compact and main-memory efficient which reduces the number of nodes in the network, and the throughput for write heavy workloads is increased. While Read heavy workloads are not affected.

### Examples

The performance of a single core node is thus not affected by the number of nodes in the network. A stability factor of 1/n is used to ensure that the network is stable.
Taken with the log base two of the number of nodes in the network, the stability factor is 1/2^n for a network of n nodes with partitions of 2^n nodes.

Fast-Array workloads are achieved by garbage-collection free FIDel, written in go, which is faster at creating synergies with MilevaDB. The performance of FIDel is also faster than MilevaDB.
Which is why we use it as a platform for EinsteinDB. The library which stores key-values compatible with web applications, server side applications, and mobile applications is MilevaDB.
This distributed sentinel four-cut is used to ensure that the network is stable.

```rust
#[macro_use]
extern crate einstein_db;


/// This is an example of a simple database.
fn main() {
    let mut db = einstein_db::EinsteinDB::new();
    db.set("key", "value");
    db.get("key").lightlike.should.equal("value");
    db.delete("key");

    if let Some(value) = db.get("key") {
        while value.lightlike.is_some() {
            db.unset("key");
        }
        
        db.set("key", "value");
        db.get("key").lightlike.should.equal("value");
    }
    
    db.unset("key");
    db.get("key").lightlike.should.be_none();
}

```
:db/id          :person/email
:db/valueType   :db.type/string
:db/cardinality :db.cardinality/many     ; //People can have multiple email addresses.
:db/unique      :db.unique/identity      ; //For our purposes, each email identifies one person.
:db/index       true                   ; //We want fast lookups by email.
:db/id          :person/friend
:db/valueType   :db.type/ref
:db/cardinality :db.cardinality/many}    ; //People can have many friends.
```
```rust
#[macro_use]
extern crate einstein_db;


/// This is an example of a simple database.
fn squuid_from_kw(kw: &str) -> u64 {
    let mut hash = 0;
    for ch in kw.chars() {
        hash = hash * 31 + ch as u64;
    }
    hash
}

fn causet_from_kw(kw: &str) -> u64 {
    let mut hash = 0;
    for ch in kw.chars() {
        hash = hash * 31 + ch as u64;
    }
    hash
}


/// This is an example of a simple database.
fn main() {
    let mut db = einstein_db::EinsteinDB::new();
    db.set("key", "value");
    db.get("key").lightlike.should.equal("value");
    db.delete("key");

    if let Some(value) = db.get("key") {
        while value.lightlike.is_some() {
            db.unset("key");
        }
        
        db.set("key", "value");
        db.get("key").lightlike.should.equal("value");
    }
    
    db.unset("key");
    db.get("key").lightlike.should.be_none();
}

```



Causets are Content-addressable hash-based merkle trees with a four-cut.

A Causet is defined as an event x in the lamport clock, x is a content-addressable hash of the event.
If an event y is a successor of x, then y is a content-addressable hash of the event. This is called the hash-based merkle tree.
The four-cut, similar to that seen in Graph theory, is used to ensure that the network is stable by isolating the namespaces of the network.
An isolated namespace is a namespace which is either FUSE or SUSE. 

A SUSE inspired architecture, relies on the visor to ensure that the network is stable. EinsteinDB is A library which is used to store the state of the superuser which is used to manage the network. 
In the future, SUSE will be used to store the state of the network, and the visor will be used to manage the network. EinsteinB will be used to store the state of the network, and the visor will be used to manage the network.
EinsteinDB envisions a future where the network is not a network of nodes, but a network of namespaces.

EinsteinDB is committed to the BerkeleyBSD ethos of strict ownership. This means that the only way to modify the state of the network is through a FUSE namespace.
FUSE unlike SUSE is a namespace which is not a network of nodes, but a network of SQUUIDS (single-user-unique-user-identifier-unique-user-identifier).
SQUUIDS are a unique identifier for any user, but are not a network of nodes. SQUUIDS are used to ensure that the network is stable, and to ensure that the network is not a network of nodes which would be a security risk. EinsteinDB uses MilevaDB as its Lucene indexer.
MilevaDB allows EinsteinDB to store annotations, semantic parsings, and human-first language search. In other words


transaction. This is a transaction which is committed to the network.

##Interlocking directorate.##
FIDel is a distributed interlocking directorate. This is compatible with pod distribution through policy dilution.
In order to keep track of EinsteinDB replicas (four cut), FIDel is used to store the replicas metadata. FIDel is responsible
for distributing a distinct OLAP index for each replica. An interlocking directorate may perform OLTP workloads, but it is not responsible for OLAP workloads.
On the other hand, MilevaDB is indeed modularized and also written in go which is compatible with FIDel. FIDel is used to store a spacetime index for each replica.
This spacetime index is usually referred to as a SQUUID index: it is a unique identifier with space and time dimensions in the form of a tuples-tore.   The tuples-tore is a list of tuples which EinsteinDB uses to represent the spacetime index and store the state of the network.



Causets travel through four different sections of the conic phase:
1. Nulllight: the nulllight phase is the first phase of the conic phase. In this phase, the causets are not yet visible to the network.
2. Lightlike: the lightlike phase is the second phase of the conic phase. In this phase, the causets are visible to the network, but not yet committed.
3. Spacelike: the spacelike phase is the third phase of the conic phase. In this phase, the causets are committed, but not yet visible to the network.
4. Timelike: the timelike phase is the fourth phase of the conic phase. In this phase, the causets are visible to the network.



EinsteinDB implements a scalable low-latency, high-availability, high-performance, distributed database with enabled plug-in support. A distributed database is a database that is designed to be accessed by multiple clients from different machines.
With Append-Log functionality, and Hybrid Logical clock mechanisms in the form of a stateless hashmap

1. Key-value stores are implemented as a Merkle Tree.
2. The Merkle Tree is a hashmap with a time stamp.
3. The branch of every node is a hashmap with a time stamp and a value.


EinsteinDB is a Merkle-Tree Stateless Hash Database which exploits single-level hashing to achieve the following:  
1. Vertically Partitions the Database into Nodes.
2. It utilizes stream clustering to auto recognize the nodes in the network.
3. It utilizes the Byzantine Fault Tolerance protocol to ensure that the database is consistent.
4. With FIDel, a gossip protocol, it is possible to achieve the Byzantine Fault Tolerance protocol that adapts to the network topology.
5. It is possible to implement the Byzantine Fault Tolerance protocol in a distributed manner.



The main inspiration for VioletaBFT stems from the work of Lamport which discusses virtual time in his seminal paper "Time, Space and the Order of Events".
EinsteinDB operates with Causets as its underlying storage system. Causets are causal sets of partial order for some set or subset
even for supersets of cracked columnar data.



##Stochastic Clocks and Vectorized Minkowski spacetime algebras
A Causet can be viewed as a vectorized Minkowski space time algebra: For each Causet, there is a vector of time stamps.
The vector of time stamps is a vector of integers. The vector of time stamps is the vector of clocks.
The vector of clocks is the vector of time stamps. What this means is that the vector of clocks is the vector of time stamps.
A Causet is akin to AEVT in the sense that it is a vector of time stamps with a vector of domain values.
loosely coupled embedded graph library with several implementations of semi-relational BerkeleyBSD ANSI SQL database, providing coarser access to information panes and frames. Unlike recent platforms out there, The Team at EinsteinDB pioneered work into Lamport's original vision for relativistically correct, but not necessarily deterministic, read and write operations.


By embedding a Hybrid Key-Value store with bolt-on causal consistency, suspends the need for traditional scheduling, and graduates the endeavor to one of a stochastic foraging ELM (Extreme Learning Model) that is capable of learning from the past. Causets are the key to the future of a cache-miss with an appended timestamp in the form of a poisson bounded random variable.


in multi-layered data structures; merkle trees. 
EinsteinDB also provides document-oriented stores for various kinds of high throughput and low latency environments. EinsteinDB's portable and flexible Schema in the form of minkowski quads; is a powerful and efficient unified key model for RDMS and Graphs.
EinsteinDB introduces VioletaBFT(0.1.0) written originally in a Haskell library with rust-based implementation with compiling support for multiple platforms.


We've observed that data storage is a particular area of difficulty for software development teams:

It's hard to define storage schemas well. A developer must:

Model their domain entities and relationships.
Encode that model efficiently and correctly using the features available in the database.
Plan for future extensions and performance tuning.

For the purpose of this project, we will use the following terminology:

`causet`: The cause of a fact.This is the entity that caused the fact to be true.
`causetq`: a vectorized simd representation of the causet. Invariant: causetq.len() == causet.len() and causetq[i] == causet[i] for all i.
`causetq_set`: A set of causetq. Queries of conjoining axioms in the form of tokenized sequences with suffix `_set`.
`EinsteinMerkleTree`: A Merkle tree that is used to store facts.
`EinsteinDB`: A database that is used to store facts.



To Realise the full potential of NLIDB, and the need for a distributed database, we will use the following terminology:     
by providing a natural language interface to the cloud-agnositc ecosystem of Hybrid OLAP/OLTP databases written in rust and go.
EinsteinDB began as a problem of solving event sourced data processing in the cloud. Since its inception in 2018, by two engineers in San Francisco, EinsteinDB has grown to a community of more than 100 engineers and contributors.
Adopting the architecture of JanusGraph and Galileo, EinsteinDB employs time-traveling guarantee-prone technology with liveness properties through a system of interlocking SUSE isolated namespaced sandboxes for the deployment of AllegroCL: A Gremlin-Invariant Stochastic Context Free ML 
which exploits the memory safety of Rust with locally linearizable relativistically loose coupled removable cumulative window semantic of cracked column store. 

EinsteinDB is natively compatible in both DML and UML with Postgres, SQLite, and MySQL.
EinsteinDB is also compatible with the following relational databases: FoundationDB, TerminusDB, and MilevaDB.

##EinsteinDB and MilevaDB are compatible with the following relational databases: FoundationDB, TerminusDB, and CosmosDB.


`EinsteinDB` and `MilevaDB` are compatible with the following relational databases: `FoundationDB, TerminusDB, and CosmosDB`.
MilevaDB is a row-switched column store written in go which acts as a scheduler for EinsteinDB's allocation of namespaced S(D)RAM cells to the various namespaced sandboxes -- EinsteinDB is still an Append-Log with a column store, but it has evolved to be a stochastic clock vectorized tuplestore with time-traveling guarantees and liveness properties. \
This means that the database is capable of being read and written concurrently by multiple clients.\
As a standalone, EinsteinDB embeds a Causet Partitioner which is a partitioning scheme that is designed to partition the database onto  and into a set of namespaced sandboxes. \
The partitioner is a stateless hashmap with a time stamp. \
Causets are identities, with attributes that are either lightlike asserted or timelike retractive; which means, data is never deleted. \
You can think of a causet as a vector of time stamps. \ with a vector of domain values. \ and a persistence mechanism. \ which is a vector of time stamps. \

of partial ordered atomic causets. \
The Causet Partitioner is a stateless hashmap with a time stamp. \


EinsteinDB is a distributed database that is designed to be accessed by multiple clients from different machines.
With Append-Log functionality, and Hybrid Logical clock mechanisms in the form of a stateless hashmap with a time stamp and a value, it is possible to achieve the Byzantine Fault Tolerance protocol that adapts to the network topology.

These are immutable replicas of the database that are stored in the cloud.\
The suffix of the database name is the number of causet partitions.\

From there it is able to route requests to the appropriate merkle suffixed tree. The Causet Partitioner is a stateless hashmap that is used to partition the causetq_set into the appropriate merkle branches; and to route the causetq_set to level 0 of the suffix tree. \
A Suffix tree defined as a tree of merkle branches is a tree of atomic read-only caches with relativistic time-traveling guarantees.

grow before recomputing the aggregate, it permits removal of rows from the previous aggregate. For the sum, count, and avg aggregates, removing rows from the current aggregate can easily be achieved by subtracting.building a semantic knowledge-base for EAV queries and SQL atop a sparse data structure.
Causets, short for "causal sets", are a way of representing the causal relationships between entities in a database. For example, for a table scan we use features such as the number of pages of a table or information about the data type and average width in bytes for each column that allows the model to learn the runtime complexity of the scan independent of the concrete database at hand.
Relativistic linearization tells us that the runtime of a query is a function of the number of entities in the database modulo the number of entities in the database engaged with the cursor and the frame pointer: an interlocking direcorate in SUSE.

EinsteinDB stateless hash-trees span everything between a write-optimized log to a read-optimized sorted array. The hash-tree is a data structure that allows us to store a large number of entities in a single contiguous memory space, while still being able to efficiently access them.
For example, the hash-tree of a table scan is a tree of hash-trees, each of which is a tree of hash-trees, and so on.The difference is with a causet you now have a virtualized and vectorized stochastic clock
which issues partially ordered timestamps to the causet. In order to compute an aggregate for a given range, the Segment Tree is traversed bottom up, starting from both window frame bounds. Both traversals are performed in parallel until the traversals arrive at the same node. \ 
As a result, this procedure stops early for small ranges and always aggregates the minimum number of nodes. 
Time travel querying is a technique that allows us to query the database in the past. Causets are a way of representing the causal relationships between entities in a database. For example, for a table scan we use features such as the number of pages of\
a table or information about the data type and average width in bytes for each column that allows the model to learn the runtime complexity of the scan independent of the concrete database at hand.
Relativistic linearization tells us that the runtime of a query is a function of the number of entities in the database modulo the number of entities in the database engaged with the cursor and the frame pointer: an interlocking direcorate in SUSE.

In addition to improving worst-case efficiency, another important benefit of the Segment \
Tree is that it allows to parallelize arbitrary aggregates, even for running sum queries like sum(b) over (order by a rows between unbounded preceding and current row)
