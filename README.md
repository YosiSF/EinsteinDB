<img src="images/EinsteinDBLogo.png" alt="einsteindb_logo" width="600"/>

## [Website](https://www.einsteindb.com) | [Documentation](https://einsteindb.com/docs/latest/concepts/overview/) | [Community Chat](https://einsteindb.com/chat)

In a nutshell, EinsteinDB is a persistent indexing scheme based off of LSH-KVX that exploits the distinct merits of hash index and B+-Tree index to support range mutant_search and avoids long NVM writes for maintaining consistency; thus improving on LSH’s performance guarantees for skewed data and adopts ordered-write consistency to ensure crash consistency, while retaining the same storage and query overhead. 

EinsteinDB is a hybrid memory system consisting of DRAM and Non-Volatile Memory(NVM) as its main components. The key-value store stores keys with their associated values in both DRAM and NVM. In addition, EinsteinDB builds an ordered set of differential concurrency schemes appended to a hash index in NVM to retain its inherent ability of fast index searching. 

- **Hybrid Index: Indexes Are Models**

Basic key-value operations include Put, Get, Update, Delete, and Scan. To locate the requested key-value item, the single-key operations (Put/Get/Update/Delete) first take exactly the B+tree index as a model that maps each query key to its page. For a sorted array, larger position id means larger key value, and the range index should effectively approximate the cumulative distribution function (CDF)one key to search the index. Once the KV item is located, Get directly returns the data, and while the write operations (Put/Update/Delete) require to persist updated index entry and new KV item if provided.

- **Differential concurrency**

As the number of CPU cores increases, concurrency control for heavy workloads becomes more challenging. Effective workload scheduling can greatly improve the performance by avoiding the conflicts. We introduce einst.ai, an AI4einsteindb learned transaction management system compatible with EinsteinDB, Foundationeinsteindb, Leveleinsteindb, Cockroacheinsteindb, and Tieinsteindb from two aspects: transaction prediction and transaction scheduling. einstAI predicts the future trend of different workloads

- **Hybrid Language Model s**

EinsteinDB supports declarative queries, which makes it easy to write queries without having to worry about how they're going to be executed. This also means that you can use EinsteinDB's constraint checking capabilities to ensure that your data is always consistent. EinsteinDB provides multi-dimensional spatial indexes, so you can perform geospatial queries on top of its columnar storage format. You can even combine this with time series data! These features make it well suited for mobile applications where users are querying historical data, such as their own location history or nearby points of interest.

--**Causets: 4-clique Causal cuts**--
A 4-clique cut (Information Theory Detour)separates processes into disjoint sets such that all edges within the set are potential causality relationships (happen before). The 4-clique cut can be computed in time linear to the number of nodes in the graph. The 4-clique cut provides us with an efficient way to reason about consistency in distributed systems. We’ve implemented our algorithms on top of einst.a.i and deployed them on Kubernetes for our production service MEDB, where we use it to provide upgrades with eventually consistent stores transformed by einst.a.i via GPT3 (OpenAI powers EinstAI) to convergent causal consistent 4-clique cuts of a Crown Graph.

--**Byzantine State Machine Replication for the Masses**--

VioletaBFT is a post-quantum asynchronous BFT protocol. This means that it uses the same cryptographic primitives as other classical BFT protocols, but with an additional set of constraints:

It guarantees liveness without making any timing assumptions. It ensures that no two processes can independently determine the outcome of the final state of the system. It does not require any coordination between process execution or synchronization among them to achieve this goal. If one process discovers that another process has violated their commitment, then they can undo their previous actions and rejoin at a later time when both processes are still in agreement, except for the period during which both nodes are in disagreement (in which case, only one node will be able to commit). The protocol guarantees consensus on every transaction committed by each node over its entire lifetime, including after failure at any point. This is possible because all nodes have full access to all other nodes' transactions and therefore know how many transactions exist before them and what order they were committed in. Since all nodes have full access to all other nodes' transactions, they can efficiently determine whether or not each other's commitments match up with theirs. All parties involved have complete knowledge about what the next state of the system will be before committing/receiving new information from outside sources such as lightlike databases or lightlike peers.






## License

EinsteinDB is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
