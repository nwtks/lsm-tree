# F# LSM-Tree Library

A high-performance, fully-featured **Log-Structured Merge-Tree** (LSM-Tree) storage engine implemented natively in F#.
This project demonstrates the core architectural concepts behind modern database systems like LevelDB and RocksDB, combining F#'s functional data-processing strengths with lock-free concurrent data structures and lightweight synchronization primitives.

---

## 🚀 Key Features

### Write-Ahead Log (WAL)
Ensures crash safety and immediate durability. All `Put` and `Delete` operations are persisted to a `.log` file before being applied to the in-memory MemTable, guaranteeing full recovery upon engine restart.
- **Configurable durability**: `syncOnCommit` toggles `fsync` on every write — balance crash safety vs. throughput.
- **Fast single-key path**: `PutSingle`/`DeleteSingle` bypass transaction markers, reducing WAL overhead for single-key operations.
- **Atomic transaction recovery**: Uncommitted transactions (missing `COMMIT`) are automatically discarded on restart.
- **Fault-tolerant parser**: Malformed lines are skipped; orphaned entries are safely recovered under last-writer-wins semantics.

### MemTable (Lock-Free SkipList)
In-memory mutations are buffered within a custom mutable **SkipList** with $O(\log N)$ probabilistic insertions and lookups.
- **Lock-Free Concurrency**: CAS-based node insertion enables concurrent `Put` operations without blocking.
- **Automatic flush**: When the MemTable exceeds `memTableSizeLimit`, it is atomically swapped to an immutable MemTable and **asynchronously** flushed to an SSTable. Flushes are sequentialized (at most one in-flight at a time).
- **Async waiting**: `FlushAsync()` and `WaitForCompactionAsync()` return `Async<unit>`.

### SSTable (Sorted String Table)
Immutable on-disk files produced when the MemTable is flushed:
- **In-memory index + Bloom filter**: At startup, each SSTable loads an entry index and Bloom filter into RAM. Lookups perform in-memory binary search on the index, then a single `Seek`+`Read` for the value payload — misses are rejected $O(1)$ by the Bloom filter with no disk I/O.
- **Concurrent reads**: Uses `ReaderWriterLockSlim` — concurrent readers proceed in parallel, while `GetAll` and `Dispose` serialize exclusively.
- **Bloom filters**: FNV-1a double-hashing, 10 bits/item, 7 hash functions.

### Background Multi-Level Compaction & Automatic Pruning
- **Configurable level limits**: e.g., `[| 4; 10; 100; 1000 |]` means L0 over 4 files triggers compaction to L1, L1 over 10 triggers compaction to L2, etc.
- **Snapshot-aware pruning**: Versions visible to active snapshots are preserved; stale versions are purged.
- **Tombstone elimination**: Deletion markers are completely removed from the final storage level.
- **Cascade compaction**: A single flush can trigger compaction cascading through multiple levels.

### Direct Put/Delete (Fast Path)
Single-key `Put` and `Delete` bypass the transaction system entirely — no `BeginTransaction`/`Commit` overhead, no snapshot registration, no WAL `BEGIN`/`COMMIT` markers. The operation is written directly to the WAL via `PutSingle`/`DeleteSingle` (with `fsync` if `syncOnCommit` is set) and applied to the MemTable in one atomic step.

### Atomic Transactions with Snapshot Isolation
Multi-key atomic updates via a dedicated `ITransaction` API:
- **Commit & Rollback**: Transactions commit atomically (all operations share a single sequence number) or roll back discarding all pending changes.
- **Read Own Writes**: `ITransaction.Get` reads pending writes within the same transaction before falling back to the snapshot.
- **Snapshot Isolation**: Each transaction operates on a stable snapshot, ensuring consistent reads even during concurrent writes.
- **`IDisposable` lifecycle**: Active snapshots are automatically released when the transaction is disposed.

---

## 🏗️ Architecture & Internals

See [docs/architecture.md](docs/architecture.md) for the WAL format, SSTable binary format, concurrency model, compaction algorithm, and MVCC design.

---

## ⚠️ Known Limitations

- **String-only keys/values**: UTF-8 strings only (base64-encoded in WAL). Binary data must be base64-encoded by the caller.
- **No range queries**: The public API supports point lookups only (`Get`). `SSTable.GetAll()` is internal for compaction.
- **Single WAL file**: One active WAL per instance. Renamed to `wal_<guid>.old` on each flush (deleted after the SSTable is successfully written); stale `.old` files from crashes remain on disk and are replayed during recovery.
- **`fsync` overhead**: `syncOnCommit = true` calls `fsync` on every commit. Set to `false` for higher throughput at the cost of losing the last ~second of data on crash.
- **`LsmTransaction.Get` O(n) local scan**: The pending ops list is scanned linearly (`Seq.tryFind`). Avoid thousands of keys in a single transaction if you need fast local reads.
- **No replication/clustering**: Single-node storage engine only.
- **Empty SSTable flush race**: `MemTable.Put`/`Delete` increment `sizeBytes` before inserting into the SkipList. A flush between increment and insert can produce an empty SSTable (no data loss — WAL guarantees recovery).

---

## 📊 Performance

Run benchmarks locally with:

```bash
dotnet run -c Release --project Benchmark
```

Benchmarks use [BenchmarkDotNet](https://benchmarkdotnet.org/) with `[<MemoryDiagnoser>]`:

| Class | Benchmarks | Description |
|---|---|---|
| `PutBenchmark` | `SequentialPut`, `ConcurrentPut`, `TransactionPut` | Single/parallel writes with `syncOnCommit` toggled |
| `GetBenchmark` | `RandomHitGet`, `RandomMissGet` | Point lookups (N = 10000 / 30000) |

---

## 💻 How to Use and Test

### Prerequisites

- [.NET 10.0 SDK](https://dotnet.microsoft.com/download) or later

### Building

```bash
# Build the library and tests
dotnet build
```

### Running Tests

```bash
# Run all tests with code coverage (coverlet)
dotnet test

# Run a specific test class
dotnet test --filter "FullyQualifiedName~BloomFilterTests"
```

### Running Benchmarks

```bash
dotnet run -c Release --project Benchmark
```

---

## 💡 Usage Examples

### Basic CRUD

```fsharp
open LsmTree

let db = new LsmTree("./data")

db.Put("user:1", "Alice")
db.Put("user:2", "Bob")

let alice = db.Get("user:1")   // Some "Alice"
let unknown = db.Get("user:99") // None

db.Delete("user:2")
let bob = db.Get("user:2")     // None
```

### Atomic Transactions

```fsharp
use tx = db.BeginTransaction()
tx.Put("acc:1", "100")
tx.Put("acc:2", "200")
tx.Delete("acc:temp")

let pending = tx.Get("acc:1")   // Some "100" — reads own uncommitted write
tx.Commit()                      // atomically commits all three ops
// or: tx.Rollback()             // discard all changes
```

### MVCC Time-Travel

```fsharp
db.Put("config:theme", "dark")
let v1 = db.Snapshot()           // snapshot at version 1

db.Put("config:theme", "light")

let current = db.Get("config:theme")       // Some "light"
let past = db.Get("config:theme", v1)      // Some "dark" — historical view
```

### Constructor Options

The data directory is **auto-created** if it doesn't exist. All parameters are optional:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `dataDir` | `string` | (required) | Path to the data directory |
| `syncOnCommit` | `bool` | `true` | Call `fsync` on every commit |
| `memTableSizeLimit` | `int` | 1,048,576 (≈1 MB) | MemTable size threshold for flush |
| `compactLevelLimits` | `int[]` | `[\| 4; 10; 100; 1000 \|]` | Max files per level before compaction (validated: must be non-empty, no negatives) |

```fsharp
// Maximum throughput — no fsync on commit
let fastDb = new LsmTree("./fast_data", syncOnCommit = false)

// Tune MemTable flush threshold
let db = new LsmTree("./data", memTableSizeLimit = 512 * 1024)  // 512 KB

// Custom compaction level limits
let db = new LsmTree("./data", compactLevelLimits = [| 2; 5; 50 |])
```

### Clean Shutdown

```fsharp
// LsmTree implements IDisposable — use 'use' or call .Close()/.Dispose()
use db = new LsmTree("./data")
// ... work ...
// db.Dispose() is called automatically at the end of the 'use' scope
```
