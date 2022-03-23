# EinsteinDB

EinsteinDB is a powerful continuum database that unifies major data structure designs. It is the first to implement a NoDB B+Tree continuum uniﬁcation model for all key-value stores, and provides superior performance properties not achievable by existing designs. 

EinsteinDB is the world’s first relativistic linearizable; embedded, bolt-on causal consistent universal key-value store. 

In a nutshell, EinsteinDB is a persistent indexing scheme based off of LSH-KVX that exploits the distinct merits of hash index and B+-Tree index to support range scan and avoids long NVM writes for maintaining consistency; thus improving on LSH’s performance guarantees for skewed data and adopts ordered-write consistency to ensure crash consistency, while retaining the same storage and query overhead. 
