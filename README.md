# F# LSM-Tree Library

A high-performance, fully-featured **Log-Structured Merge-Tree** (LSM-Tree) storage engine implemented natively in F#.
This project demonstrates the core architectural concepts behind modern database systems like LevelDB and RocksDB, combining F#'s functional data-processing strengths with lock-free concurrent data structures and lightweight synchronization primitives.

**Target framework:** .NET 10.0

---

## 🚀 Key Features

### Write-Ahead Log (WAL)
Ensures crash safety and immediate durability. All `Put` and `Delete` operations are persisted sequentially to a `.log` file before memory allocation, guaranteeing full recovery upon engine restart.
- **Configurable `fsync`**: `SyncOnCommit` toggles whether `fsync` is called on every commit — balancing durability and throughput.
- **Atomic transaction recovery**: Uncommitted transactions (missing `COMMIT` marker) are automatically discarded on restart.
- **Fault-tolerant parser**: Malformed lines, orphaned `BEGIN`s, and unknown entries are silently skipped.

### MemTable (Lock-Free SkipList)
In-memory mutations are buffered within a performant, custom-built mutable **SkipList** with $O(\log N)$ probabilistic insertions and lookups.
- **Lock-Free Concurrency**: Uses `Interlocked.CompareExchange` CAS loops to splice nodes without blocking, supporting massive multi-threaded `Put` operations simultaneously.
- **Automatic flush**: When the MemTable exceeds `memTableSizeLimit`, it is atomically swapped to an immutable MemTable and flushed to an SSTable in the background.

### SSTable (Sorted String Table)
Immutable on-disk files produced when the MemTable is flushed:
- **Binary search via footer indexing**: At startup, each SSTable loads a compact offset array (`int64[]`) and Bloom filter into RAM. Lookups use binary search over the in-memory offsets, then seek to the exact disk position.
- **Bloom filters** (FNV-1a derived, double-hashing): 10 bits/item, 7 hash functions. $O(1)$ in-memory probe rejects non-existent keys before any disk I/O.

### Background Multi-Level Compaction & Automatic Pruning
- **Configurable level limits**: e.g., `[| 4; 10; 100; 1000 |]` means L0 over 4 files triggers compaction to L1, L1 over 10 triggers compaction to L2, etc.
- **Snapshot-aware pruning**: Versions still visible to any active snapshot are preserved; stale versions are purged.
- **Tombstone elimination**: In the final storage level, deletion markers (`None` values) are completely removed from the output.
- **Cascade compaction**: A single flush can trigger compaction cascading through multiple levels if each level's limit is exceeded.

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

### WAL Format

One line per operation; keys and values are **base64-encoded** to avoid delimiter issues:

```
BEGIN <seq>
PUT <seq> <key_b64> <val_b64>
DEL <seq> <key_b64>
COMMIT <seq>
```

- Recovery is fault-tolerant: orphaned `BEGIN` (no matching `COMMIT`) and orphaned `PUT`/`DEL` (no surrounding `BEGIN`/`COMMIT`) operations are recovered. Only uncommitted transactions are discarded.
- `WALRecovery.recover` handles malformed lines gracefully by returning `None` for unrecognized entries.

### SSTable Binary Format

```
[entry bytes...] [index_offset: int64] [bloom_offset: int64] [max_seq: int64] [magic: int64]
```

- **Footer**: always 32 bytes (four `int64` fields).
- **Magic**: `0x4C534D54` (`"LSMT"` in ASCII). Wrong magic raises `InvalidDataException`.
- **Index**: packed `int32` count + `int64[]` offsets pointing to each entry.
- **Bloom filter**: packed `int32` byte count + raw bytes.
- **`max_seq`**: highest sequence number among all entries — enables O(1) startup without scanning.

**File naming convention:**

```
L{level}_{timestamp_ms}_{guid}.sst
```

| Source | Level |
|--------|-------|
| MemTable flush | L0 |
| Compaction of Ln → L(n+1) | L(n+1) |
| Legacy files (no `L` prefix) | L0 |

### Concurrency Model & Lock Ordering

| Resource | Guard |
|---|---|
| `memTable` / `immutableMemTable` | `ReaderWriterLockSlim` (`mainLock`) |
| `ssTables` array | `lock ssTablesLock` |
| `activeSnapshots` set | `lock activeSnapshotsLock` |
| `globalSeq` | `Interlocked.Increment` / `Interlocked.Read` / `Interlocked.CompareExchange` |
| SkipList nodes | Lock-free CAS (`Interlocked.CompareExchange`) |

**Lock ordering rules:**
1. You may hold `ssTablesLock` while acquiring `mainLock` (write), but **never the reverse** — this prevents deadlocks.
2. `CompactionCoordinator` auto-properties (`IsCompacting`, `Error`) are always read/written under `ssTablesLock`.
3. At most one compaction `Task` runs at a time; coordination uses `ManualResetEvent`.

### Compaction Algorithm

1. **Trigger**: A MemTable flush completes, or the previous compaction finishes.
2. **Level selection**: Starting from L0, if `ssTables[level].Length > compactLevelLimits[level]`, all files at that level are selected for compaction.
3. **Merge**: A k-way streaming merge reads all entries from selected SSTables, deduplicates by key (highest sequence number wins), and applies snapshot pruning.
4. **Output**: A single new SSTable is written to the next level.
5. **Cascade**: Compaction recursively proceeds to the next level if it now exceeds its limit.
6. **Cleanup**: Old SSTable files are disposed, deleted from disk, and removed from the in-memory list.

> **Why all files?** In this implementation, all levels (including Ln for n>0) may contain overlapping key ranges. Partial compaction would leave old versions shadowing newer ones in higher levels. See [AGENTS.md](AGENTS.md) for details.

### MVCC & Snapshot Isolation

- Each write operation is assigned a globally incrementing sequence number (`globalSeq`).
- `LsmTree.Snapshot()` captures the current sequence number; subsequent reads with that snapshot see a consistent view.
- Compaction's `pruneVersions` preserves all entries with `seq >= minActiveSnapshot`, ensuring no visible version is removed.
- Transactions registered with the snapshot manager prevent compaction from pruning versions they might read.

---

## ⚠️ Known Limitations

- **String-only keys/values**: UTF-8 strings only (base64-encoded in WAL). Binary data is supported via base64 encoding by the caller.
- **No range queries**: The public API supports point lookups only (`Get`). `SSTable.GetAll()` is internal, used exclusively during compaction.
- **Single WAL file**: One WAL per instance; renamed to `wal_<guid>.old` on MemTable swap. Very old `.old` files may accumulate if the engine crashes mid-swap.
- **`fsync` overhead**: `syncOnCommit = true` (default) calls `fsync` on every commit, limiting throughput on spinning disks. Set to `false` for higher throughput at the cost of losing the last ~second of data on crash.
- **`LsmTransaction.Get` O(n) local scan**: Within a transaction, the local pending-ops list is scanned linearly. Avoid putting thousands of keys in a single transaction if you need fast reads within it.
- **No explicit checkpoint/archive**: The WAL grows indefinitely until the next MemTable swap. There is no periodic WAL archival independent of flush.
- **No replication/clustering**: Single-node storage engine only.

---

## 📊 Performance

Run benchmarks locally with:

```bash
dotnet run -c Release --project benchmark
```

The benchmark suite includes:

| Benchmark | Description |
|---|---|
| `SequentialPut` | Single-threaded `Put` with `syncOnCommit` toggled |
| `ConcurrentPut` | `Parallel.For`-based `Put` workload |
| `TransactionPut` | Batched puts inside a single transaction |
| `RandomHitGet` | Point lookup of existing keys |
| `RandomMissGet` | Point lookup of non-existent keys |

Results vary by storage medium (NVMe vs HDD), CPU, and the `syncOnCommit` setting. For highest throughput, set `syncOnCommit = false`.

---

## 💻 How to Use and Test

### Prerequisites

- [.NET 10.0 SDK](https://dotnet.microsoft.com/download) or later

### Building

```bash
dotnet build
```

### Running Tests

```bash
# Run all tests with code coverage
dotnet test

# Run a specific test class
dotnet test --filter "FullyQualifiedName~BloomFilterTests"
```

Current test coverage: **~97% line coverage** across all modules.

### Running Benchmarks

```bash
dotnet run -c Release --project benchmark
```

### Code Quality

```bash
# Check for formatting issues
dotnet format --verify-no-changes
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
