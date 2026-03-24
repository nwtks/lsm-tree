# F# LSM-Tree Library

A high-performance, fully-featured Log-Structured Merge-Tree (LSM-Tree) storage engine implemented natively in F#.
This project demonstrates the core architectural concepts behind modern top-tier database systems like LevelDB and RocksDB, utilizing F#'s functional data-processing strengths combined with lightweight threading and locking structures.

## 🚀 Key Features

* **Write-Ahead Log (WAL)**
  Ensures crash safety and immediate durability. All `Put` and `Delete` operations are persisted sequentially to a `.log` file before memory allocation, guaranteeing 100% recovery upon engine restart.
* **MemTable (Lock-Free SkipList)**
  In-memory mutations are buffered within a highly performant, custom-built mutable **SkipList**. This achieves stable $O(\log N)$ probabilistic insertions and lookups. 
  * **Extreme Concurrency**: The SkipList natively employs **Lock-Free** `Interlocked.CompareExchange` CAS loops to securely splice nodes without blocking, seamlessly supporting massive multi-threaded `Put` operations simultaneously.
* **SSTable (Sorted String Table)**
  When the MemTable exceeds its size limit, it flushes to immutable on-disk `SSTable` files featuring:
  * **True Binary Search (Footer Indexing)**: Instead of loading entire datasets into RAM, SSTables only load a compressed array of `int64` offsets. The engine accurately jumps through the disk via raw binary search, minimizing RAM footprint.
  * **Bloom Filters**: Each SSTable calculating and embeds a probabilistic bitmask. This filter runs in memory at $O(1)$ time, instantly blocking useless disk reads when a key isn't stored in the segment.
* **Background Multi-Level Compaction & Automatic Pruning**
  Dynamically tracks hierarchical limits and merges older tables.
  * **Configurable Limits**: Hierarchical level limits (e.g., `L0`, `L1`, `L2`...) can be explicitly configured via the engine constructor to tune for specific read/write amplification profiles.
  * **Snapshot Pruning (GC)**: The engine automatically identifies and purges stale versions of keys that are no longer requested by any active transaction, effectively preventing storage bloat while maintaining strict MVCC correctness.
  * **Tombstone Removal**: In the final storage level, old deletion markers (Tombstones) are completely eliminated.
* **Atomic ACID Transactions**
  Supports multi-key atomic updates via a dedicated `ITransaction` API. 
  * **IDisposable Lifecycle**: Transactions implement `IDisposable`, ensuring active snapshots are automatically released and reclaimed by the compaction engine once a task is complete.
  * **Snapshot Isolation**: Every transaction operates on a stable snapshot of the database, ensuring consistent reads even during concurrent writes.

## 🔧 Project Structure

* `src/LsmTree.fs`: The primary Coordinator. Manages active snapshot tracking, background Async Compaction, and orchestrates MemTable flushes.
* `src/SSTable.fs`: Contains binary flushing logic, localized offset scanning, and disk-based Binary Search.
* `src/BloomFilter.fs`: Probabilistic hashing and bitmask logic to instantly block useless disk reads.
* `src/MemTable.fs` & `src/SkipList.fs`: High-performance in-memory buffering. `SkipList.fs` implements a **Lock-Free** concurrent insertion algorithm.
* `src/WAL.fs`: Write-Ahead Log for crash recovery.
* `src/Tests.fs`: XUnit Test Suite for functional verification.
* `benchmark/`: A **BenchmarkDotNet** suite for scientific performance measurement.

## 💻 How to Use and Test

### Running the XUnit Test Suite
```bash
dotnet test
```

### Code Coverage
```bash
dotnet test /p:CollectCoverage=true /p:CoverletOutputFormat=cobertura
```

### Running Benchmarks
```bash
dotnet run -c Release --project benchmark
```

### Usage Example (F#)

```fsharp
open LsmTree

// 1. Initialize with optional Memory and Compaction limits
let db = new LsmTree("./data", memTableSizeLimit = 1024*1024, compactLevelLimits = [| 4; 10; 100 |])

// 2. Standard Operations
db.Put("user:1", "Alice")
db.Put("user:2", "Bob")
let alice = db.Get("user:1") // Returns: Some "Alice"
let unknown = db.Get("user:99") // Returns: None
db.Delete("user:2")
let bob = db.Get("user:2") // Returns: None

// 3. Atomic Transactions (using IDisposable)
use tx = db.BeginTransaction()
tx.Put("acc:1", "100")
tx.Put("acc:2", "200")
tx.Delete("acc:temp")
tx.Commit()

// 4. MVCC Snapshot Isolation (Time-Travel)
db.Put("config:theme", "dark")
let v1 = db.Snapshot()
db.Put("config:theme", "light")
let current = db.Get("config:theme") // Some "light"
let past = db.Get("config:theme", v1) // Some "dark"
```
