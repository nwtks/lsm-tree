# F# LSM-Tree Library

A high-performance, fully-featured Log-Structured Merge-Tree (LSM-Tree) storage engine implemented natively in F#.
This project demonstrates the core architectural concepts behind modern top-tier database systems like LevelDB and RocksDB, utilizing F#'s functional data-processing strengths combined with lightweight threading and locking structures.

## 🚀 Key Features

* **Write-Ahead Log (WAL)**
  Ensures crash safety and immediate durability. All `Put` and `Delete` operations are persisted sequentially to a `.log` file before memory allocation, guaranteeing 100% recovery upon engine restart.
* **MemTable (Custom SkipList)**
  In-memory mutations are buffered within a highly performant, custom-built mutable **SkipList**. This achieves stable $O(\log N)$ probabilistic insertions and lookups, perfectly matched for high throughput and rapid memory ingestion.
* **SSTable (Sorted String Table)**
  When the MemTable exceeds its size limit, it flushes to immutable on-disk `SSTable` files featuring:
  * **True Binary Search (Footer Indexing)**: Instead of loading entire datasets into `.NET Dictionary` instances, SSTables only load a compressed array of `int64` offsets. The engine accurately jumps and skips through the disk via raw binary search, heavily minimizing RAM footprint.
  * **Bloom Filters**: Each SSTable calculates and embeds a probabilistic byte-array bitmask at its footer. This filter runs entirely in memory at $O(1)$ time, instantly blocking useless disk reads when a key isn't stored in the segment.
* **Background Multi-Level Compaction**
  Dynamically tracks hierarchical limits (`L0` max 4 files, `L1` max 10 files...). When limits are exceeded, older tables are K-way merged via a separate `System.Threading.Tasks.Task`. Active user operations (`Put` / `Get`) remain completely unblocked during heavy disk flushes. 
* **MVCC (Multi-Version Concurrency Control) & Time-Travel**
  All mutations inherently track an atomic incremental Sequence Number. The engine natively supports **Snapshot Isolation**, permitting strict time-travel execution where querying an older snapshot reliably reconstructs the database exactly as it was at that coordinate—seamlessly crossing boundaries between MemTables and Flushed SSTables on disk.

## 🔧 Project Structure

* `src/LsmTree.fs`: The primary Coordinator. Exposes `.Put(key, value)`, `.Get(key, ?snapshot)`, `.Delete(key)`, `.Snapshot()`, manages Thread locks, handles background Async Compaction, and orchestrates MemTable flushes.
* `src/SSTable.fs`: Contains binary `BinaryWriter` / `BinaryReader` flushing arrays, localized offset scanning, Bloom Filter definitions, and recursive disk Binary Search capabilities.
* `src/MemTable.fs` & `src/SkipList.fs`: Purely in-memory multi-version buffering logic powered by probabilistic balancing.
* `src/WAL.fs`: Log recording and text-based crash recovery logic.
* `src/Tests.fs`: Contains the **XUnit** Test Suite mathematically asserting the engine's MVCC, Compaction, and structure recovery capabilities sequentially without process blocking.

## 💻 How to Use and Test

This engine is built as a generic `Library (.dll)`, making it universally importable by upstream F# or C# applications. 

**Requirements:**
- .NET SDK (8.0 or newer recommended)

### Running the XUnit Test Suite
The project comes embedded with high-coverage XUnit tests covering all components and Snapshot MVCC assertions. Run the following command at the root to efficiently build and dynamically test the entire multi-threaded data lifecycle:

```bash
dotnet test
```

### Usage Example (F#)

```fsharp
open LsmTree

// 1. Initialize the LSM-Tree engine targeting a local directory.
// It automatically recovers from any existing WAL or SSTable files.
let db = new LsmTree("./my_database_data")

// 2. Standard Operations
db.Put("user:1", "Alice")
db.Put("user:2", "Bob")

let alice = db.Get("user:1") // Returns: Some "Alice"
let unknown = db.Get("user:99") // Returns: None

db.Delete("user:2")
let bob = db.Get("user:2") // Returns: None

// 3. MVCC Snapshot Isolation (Time-Travel)
db.Put("config:theme", "dark")
let version1Snapshot = db.Snapshot()

db.Put("config:theme", "light")

// Query newest state
let current = db.Get("config:theme") // Returns: Some "light"

// Query the past (Time-Travel via Snapshot)
let past = db.Get("config:theme", snapshot = version1Snapshot) // Returns: Some "dark"
```
