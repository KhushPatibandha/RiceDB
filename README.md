
# RiceDB: LSMTree based storage system

RiceDB is a storage system based on a very popular NoSQL architecture, Log-structured merge-trees also known as LSM Trees. This architecture is followed in many DBs like Amazon's DynamoDB, Cassandra, ScyllaDB, RocksDB, etc.

This storage system is designed to offer fast write throughput with decently fast read operations. This can be used in many embedded storage engines because of the simple key-value pair interface.

This application has all the major components that are required in a system like this.
 - Memtable / in-memory store implemented with AVL Trees
 - SSTable file disk storage
 - Sparse Index for each SSTable for fast reads
 - Bloom Filter for optimization
 - Compaction to deal with Tombstone, stale entries
## API Reference

#### Insert a key-value pair

```http
  POST /api/insert

  json => {
    "key" : "youKey",
    "value" : "yourValue"
  }
```


#### Update a key-value pair

```http
  PUT /api/update

  json => {
    "key" : "youKey",
    "value" : "yourValue"
  }
```

#### Delete a key

```http
  DELETE /api/delete?key=<youKey>
```

#### Get a value

```http
  GET /api/get?key=<yourKey>
```

#### Recover logs and memory after crash

```http
  PUT /api/recover
```

