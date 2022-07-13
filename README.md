# EinsteinDB
EinsteinDB addresses being able to reach the full potential of NLIDB by providing a natural language interface to the database ecosystem of Hybrid OLAP/OLTP databases.
Adopting the architecture of JanusGraph and Galileo, EinsteinDB employs guarantee liveness through locally linearizable relativistically loose coupled removable cumulative window protocol allowing the frame to become consistent across different versions of the application.
grow before recomputing the aggregate, it permits removal of rows from the previous aggregate. For the sum, count, and avg aggregates, removing rows from the current aggregate can easily be achieved by subtracting.building a semantic knowledge-base for EAV queries and SQL atop a sparse data structure.
Causets, short for "causal sets", are a way of representing the causal relationships between entities in a database. For example, for a table scan we use features such as the number of pages of a table or information about the data type and average width in bytes for each column that allows the model to learn the runtime complexity of the scan independent of the concrete database at hand.
Relativistic linearization tells us that the runtime of a query is a function of the number of entities in the database modulo the number of entities in the database engaged with the cursor and the frame pointer: an interlocking direcorate in SUSE.

EinsteinDB stateless hash-trees span everything between a write-optimized log to a read-optimized sorted array. The hash-tree is a data structure that allows us to store a large number of entities in a single contiguous memory space, while still being able to efficiently access them.
For example, the hash-tree of a table scan is a tree of hash-trees, each of which is a tree of hash-trees, and so on.The difference is with a causet you now have a virtualized and vectorized stochastic clock
which issues partially ordered timestamps to the causet. In order to compute an aggregate for a given range, the Seg- ment Tree is traversed bottom up starting from both window frame bounds. Both traversals are done simultaneously until the traversals arrive at the same node. As a result, this procedure stops early for small ranges and always aggregates the minimum number of nodes. The details of the traversal algorithm can be found in Appendix C.
In addition to improving worst-case efficiency, another important benefit of the Segment Tree is that it allows to parallelize arbitrary aggregates, even for running sum queries like sum(b) over (order by a rows between unbounded preceding and current row)

## Theory

It has been shown that local linearizability in CAP is equivalent to global linearizability in EinsteinDB. The difference between a holistic approach, to a more practical one, is that the holistic approach is based on the assumption that the database is a single entity, while the practical approach is based on the assumption that the database is a collection of entities.
Causets then are FoundationDB RecordLayer Tuplespaces which interact with shim layers to provide a natural language interface to the database. Semantic Parsings are stored in the causet as a tree of hash-trees.  

# Cracking Columns and Sharding Causets: A Poisson distribution of byzantine failures.




Built as a shim for a stateless merkle tree, with bolt on consistency and relativsitc linearization, EinsteinDB
navigate the Pareto curve to find the best possible balance between the costs of lookups and updates for any given main memory size.

[]: # Language: markdown
[]: # Path: README.md
that key-value stores backed by an LSM-tree exhibit an intrinsic trade-off between lookup cost, update cost, and main memory footprint, yet all existing designs expose a subopti- mal and difficult to tune trade-off among these metrics. We pin- point the problem to the fact that all modern key-value stores sub- optimally co-tune the merge policy, the buffer size, and the Bloom filters’ false positive rates in each level.
We present Monkey, an LSM-based key-value store that strikes the optimal balance between the costs of updates and lookups with any given main memory budget. The insight is that worst-case lookup cost is proportional to the sum of the false positive rates of the Bloom filters across all levels of the LSM-tree. Contrary to state-of-the-art key-value stores that assign a fixed number of bits-per-element to all Bloom filters, Monkey allocates memory to filters across different levels so as to minimize this sum.

EinsteinDB is an opinionated weakly-coupled, Multi-Version Timestamped Ordered, repeatable read, transactional database which supports a query language and storage modeling similar but agnostic to relational databases. In Fact, EinsteinDB is a computational database. With one big difference, EinsteinDB is not a relational database.
While EinsteinDB provides PL/SQL, it is not merely a key-value storage system; EinsteinDB incorporates the idea of Minkowski, Lamport, and others. Causets of the Minkowski model are:
- **Timestamp**: The timestamp is the time at which a transaction is committed.
- **Order**: The order is the order in which transactions are committed.
- **Repeatability**: The repeatability is the ability to repeat a transaction.
- **Transactional**: The transactional is the ability to commit a transaction.

- EinsteinDB uses EinsteinML (einstein_ml) as a Metadata Languge model for persistence_file; unlike HyPerDB, MongoDB, Arango, and BoltDB, EinsteinDB does not require a schema to be defined.
- Causets are similar to tuplespaces and allow individuals to read their writes, and allow multiple individuals to write optimize in parallel universes.
- EinsteinDB is a 
- 

[]: # Language: markdown
[]: # Path: README.md
We call these multiplexers, called Interlocks: because they are more than just simple locks. EinsteinDB builds upon the monadic algebra of query splines which are tracing throughput They are a set of rules that are enforced by the database.
It folows from Lamport's paper, "A Systematic Approach to Linearizability". EinsteinDB operates with compatibility to Postgres, MySQL, and SQLite.

[]: # Language: markdown
[]: # Path: README.md


EinsteinDB is a new kind of database that can handle extremely large amounts of data. With its Write Amplificator, it can write data at speeds up to 5 milliseconds, which is faster than any other database on the market. \
EinsteinDB also has features that make it unique, such as its ```Unified Key Format and Horn Rules```. 
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