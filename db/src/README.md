A monotonic timestamp grows linearly, there is branching, merging, and simultaneous editing of remote primary timeline changes. 
Inspired by Microsoft's TransactSQL, EinsteinDB's client upholds operations of assertion, retraction, and excisions of temporal mappinh, but while the sets are spawned in an ordered 'monotnic' fashion, the SchemaReplicant tokenizes and indexes the event as a causet memory for time travel fetches in the future.

The beauty of EinsteinDB is embedded in its Supercolumnar Stochastic Database partinioning: A robust adaptive cache index with SQL feature but no strict key-value deference. We treat columns as a stochastic value defining the convergence of an iterative tessellation of event stores with an apative index using AVL trees. These trees are meant to maintain small depth by restricting the number of entries(or the minimum size of a "batch"). 

It's important to understand that EinsteinDB does not follow a traditional RDBMS as it deals with Events as row-wise vectors inside a multidimensional array, with append-log column indices, which serve as batches. Ultimately instead of dealing with Tables in the traditional sense, the causet (causal consistent CausetNet API) irreflexive outer join structure of EinsteinDB consumes  tuple-blocks.

 
