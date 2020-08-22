EinsteinDB is an embedded knowledge base on relativistic sidecars: multi-consumer, multi-producer; console agnostic HTAP.

EinsteinDB as presented herein is a community edition DB-a-a-S built with Rust, Haskell, Docker, Kubernetes, Ansible and ❤️ 
Along with MilevaDB, both components make up a cost-optimized distributed-SQL cluster with prolog querying, protobuf generating, and regular
expression querying of Cold, Warm, and Hot Data Chunks. EinsteinDB implements a two-pronged Append-Merge Pull mechanism (similar to git). Because of EinsteinDB's Homomorphic Keyspace, MilevaDB can transmute key-value pairs as lock-free version-sharing secondary indices but for dirty pages pointing to trees. We define an einstein_merkle tree as the BtreeMapEngine invariant (or the HashMap) for B+Trees but guaranteeing better read-heavy throughput.

EinsteinDB and MilevaDB interact with SQLite(at the top layer), PostgreSQL for persistence(middle layer), and LMDB's Iterator implementation (bottom layer). Designed for Write-Optimized Copy-on-Write Semantics, Repeatable Read isolation guarantees for HTAP, column cracking, and vectorized SIMD for OLAP/OLTP SparkQL streams. VioletaBFT is EinsteinDB's distributed consensus protocol implemented with Haskell.

VioletaBFT is a Byzantine Fault Tolerant HoneyBadger/Raft protocol which provides consistency guarantees and maintains consensus between replicas. Unlike most of the competitors, VioletaBFT compiles to Rust (From Haskell) and pipelines a JIT Automatic failover with Partition persistence.

