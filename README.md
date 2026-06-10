# F# LSM-Tree Library

A high-performance, fully-featured **Log-Structured Merge-Tree** (LSM-Tree) storage engine implemented natively in F#.
This project demonstrates the core architectural concepts behind modern database systems like LevelDB and RocksDB, combining F#'s functional data-processing strengths with lock-free concurrent data structures and lightweight synchronization primitives.

---

## 🚀 Key Features

### Write-Ahead Log (WAL)
Ensures crash safety and immediate durability. All `Put` and `Delete` operations are persisted sequentially to a `.log` file before memory allocation, guaranteeing full recovery upon engine restart.
- **Configurable `fsync`**: `SyncOnCommit` toggles whether `fsync` is called on every commit — balancing durability and throughput.
- **Explicit buffer flush**: `StreamWriter.AutoFlush` is disabled — the StreamWriter buffer is explicitly flushed (`writer.Flush()`) before the `fsync` call (`stream.Flush(true)`) on every commit or direct write. This avoids redundant page-cache flushes on every `WriteLine`.
- **Non-transactional direct writes**: `PutSingle` / `DeleteSingle` bypass `BEGIN`/`COMMIT` markers for single-key operations — the orphaned `PUT`/`DEL` lines are recovered as committed on crash (safe under last-writer-wins semantics).
- **Atomic transaction recovery**: Uncommitted transactions (missing `COMMIT` marker) are automatically discarded on restart.
- **Fault-tolerant parser**: Malformed lines and unknown entries are skipped (invalid base64 logs a warning to stderr); orphaned `PUT`/`DEL` lines (without a surrounding `BEGIN`/`COMMIT`) are recovered as committed.

### MemTable (Lock-Free SkipList)
In-memory mutations are buffered within a performant, custom-built mutable **SkipList** with $O(\log N)$ probabilistic insertions and lookups.
- **Lock-Free Concurrency**: Uses `Interlocked.CompareExchange` CAS loops to splice nodes without blocking, supporting massive multi-threaded `Put` operations simultaneously.
- **Automatic flush**: When the MemTable exceeds `memTableSizeLimit`, it is atomically swapped (under a write lock) to an immutable MemTable, then **asynchronously** flushed to an SSTable via `Task.Run`. Flushes are sequentialized by `FlushCoordinator` (at most one in-flight flush at a time). Background compaction runs as a separate `Task` after each flush completes.

### SSTable (Sorted String Table)
Immutable on-disk files produced when the MemTable is flushed:
- **In-memory index**: At startup, each SSTable loads an `IndexEntry[]` (key, sequence number, disk offset, key byte length) and Bloom filter into RAM. Lookups perform pure in-memory binary search on the index, then do a single `Seek`+`Read` for the value payload on a hit — the key and sequence number are skipped via `KeyByteLen`, avoiding re-reading from disk.
- **Concurrent reads**: Uses `ReaderWriterLockSlim` — concurrent readers proceed in parallel (`withReadLock`), while `GetAll` and `Dispose` serialise via `withWriteLock`.
- **Bloom filters** (FNV-1a derived, double-hashing): 10 bits/item, 7 hash functions. $O(1)$ in-memory probe rejects non-existent keys before any disk I/O.

### Background Multi-Level Compaction & Automatic Pruning
- **Configurable level limits**: e.g., `[| 4; 10; 100; 1000 |]` means L0 over 4 files triggers compaction to L1, L1 over 10 triggers compaction to L2, etc.
- **Snapshot-aware pruning**: Versions still visible to any active snapshot are preserved; stale versions are purged.
- **Tombstone elimination**: In the final storage level, deletion markers (`None` values) are completely removed from the output.
- **Cascade compaction**: A single flush can trigger compaction cascading through multiple levels if each level's limit is exceeded.

### Direct Put/Delete (Fast Path)
Single-key `Put` and `Delete` bypass the transaction system entirely — no `BeginTransaction`/`Commit` overhead, no snapshot registration, no WAL `BEGIN`/`COMMIT` markers. The operation is written directly to the WAL via `PutSingle`/`DeleteSingle` and applied to the MemTable in one atomic step. This eliminates allocation and lock churn for the common single-key write case.

### Atomic Transactions with Snapshot Isolation
Multi-key atomic updates via a dedicated `ITransaction` API:
- **Commit & Rollback**: Transactions commit atomically (all operations share a single sequence number) or roll back discarding all pending changes.
- **Read Own Writes**: `ITransaction.Get` reads pending writes within the same transaction before falling back to the snapshot.
- **Snapshot Isolation**: Each transaction operates on a stable snapshot, ensuring consistent reads even during concurrent writes.
- **`IDisposable` lifecycle**: Active snapshots are automatically released when the transaction is disposed.

### Immutable Data Structures & Functional Design
Uses F# purely functional `Set` and `list` for deterministic state transitions during WAL recovery and snapshot management. Lock helpers (`withReadLock`/`withWriteLock`) eliminate raw `try`/`finally` boilerplate.

---

## 🏗️ Architecture & Internals

See [docs/architecture.md](docs/architecture.md) for the WAL format, SSTable binary format, concurrency model, compaction algorithm, and MVCC design.

---

## ⚠️ Known Limitations

- **String-only keys/values**: UTF-8 strings only (base64-encoded in WAL). Binary data is supported via base64 encoding by the caller.
- **No range queries**: The public API supports point lookups only (`Get`). `SSTable.GetAll()` is internal, used exclusively during compaction.
- **Single WAL file**: One WAL per instance; renamed to `wal_<guid>.old` on MemTable swap. Very old `.old` files may accumulate if the engine crashes mid-swap.
- **`fsync` overhead**: `syncOnCommit = true` (default) calls `fsync` on every commit, limiting throughput on spinning disks. Set to `false` for higher throughput at the cost of losing the last ~second of data on crash.
- **`LsmTransaction.Get` O(n) local scan**: Within a transaction, the local pending-ops list is scanned linearly (`Seq.tryFind`). Avoid putting thousands of keys in a single transaction if you need fast reads within it.
- **No explicit checkpoint/archive**: The WAL grows indefinitely until the next MemTable swap. There is no periodic WAL archival independent of flush.
- **No replication/clustering**: Single-node storage engine only.
- **Empty SSTable flush race**: `MemTable.Put`/`Delete` increment `sizeBytes` before inserting into the SkipList. A flush check between the increment and the insert can produce an empty SSTable (no data loss — the WAL guarantees full recovery).
- **BloomFilter memory**: Each open SSTable holds a full `byte[]` bloom filter in memory (≈1.25 MB per million entries). BloomFilter lifetimes are tied to SSTable lifetimes; compaction disposes both together.
- **`.sst.tmp` Stale files**: If the process crashes during `SSTableWriter.writeCore` (before the atomic rename), a `.sst.tmp` file may remain. Startup automatically deletes `*.sst.tmp` files — they are cosmetic and never affect correctness.

---

## 📊 Performance

Run benchmarks locally with:

```bash
dotnet run -c Release --project Benchmark
```

The benchmark suite is built with [BenchmarkDotNet](https://benchmarkdotnet.org/) and consists of two test classes:

### `PutBenchmark` (single/parallel writes)

| Benchmark | Description |
|---|---|
| `SequentialPut` | Single-threaded `Put` with `syncOnCommit` toggled (`[<Params(false, true)>]`) |
| `ConcurrentPut` | `Parallel.For`-based `Put` workload |
| `TransactionPut` | Batched puts inside a single `BeginTransaction`/`Commit` |

All put benchmarks use `[<IterationSetup>]` to create a fresh engine per run. `[<MemoryDiagnoser>]` captures allocation statistics.

### `GetBenchmark` (point lookups)

| Benchmark | Description |
|---|---|
| `RandomHitGet` | Point lookup of existing keys (randomly selected) |
| `RandomMissGet` | Point lookup of non-existent keys (randomly selected) |

Get benchmarks pre-populate the database in `[<GlobalSetup>]` with `N = 10000` or `30000` entries.

Results vary by storage medium (NVMe vs HDD), CPU, and the `syncOnCommit` setting. For highest throughput, set `syncOnCommit = false`.

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

### Durability Configuration

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
// db.Close() is called automatically at the end of the scope
```
