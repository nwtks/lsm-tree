# F# LSM-Tree

A high-performance **Log-Structured Merge-Tree** (LSM-Tree) storage engine implemented natively in F#.
The project demonstrates the architectural concepts behind engines like LevelDB and RocksDB.
WAL durability, lock-free SkipList mutations, immutable SSTables with in-memory indexes and Bloom filters, multi-level cascade compaction, and MVCC snapshot isolation.
Combining F#'s functional data-processing strengths with lightweight synchronization.

---

## Key Features

### Write-Ahead Log (WAL)

Durability journal persisted to `wal.log` before each mutation.
On restart, the WAL is replayed into a fresh MemTable.

- **Fast single-key path**: `Put`/`Delete` call `PutSingle`/`DeleteSingle`, writing a bare `PUT`/`DEL` line (no `BEGIN`/`COMMIT` markers) with `sync = false` (no `fsync`).
  Transaction `Commit` always uses `fsync`.
- **Atomic transaction recovery**: Committed transactions (`BEGIN`+`COMMIT`) are fully recovered.
  Uncommitted ones (`BEGIN` without `COMMIT`) are discarded.
  Explicit `ABORT <seq>` records are recognized and ignored.
- **Fault-tolerant parser**: Malformed lines are skipped with a `[WARN]` log.
  Orphaned `PUT`/`DEL` (no preceding `BEGIN`) are recovered as committed under the engine's last-writer-wins semantics.
- **Streaming recovery**: Uses `File.ReadLines` (lazy enumeration, not `File.ReadAllLines`) in two passes.
  First to collect committed/begun sequence sets, second to emit entries.
  Memory use is O(unique sequence count), not O(file size).
- **Flush rotation**: On each MemTable flush, the live `wal.log` is renamed to `wal_<guid>.old` and deleted only after the SSTable is written.
  Stale `.old` files from a crash remain on disk and are scanned during recovery.
  Their entries are filtered out once absorbed into SSTables.

### MemTable (Lock-Free SkipList)

In-memory mutations are buffered in a custom mutable **SkipList** with O(log N) probabilistic insertions and lookups.

- **Lock-Free Concurrency**: CAS-based node insertion (`Interlocked.CompareExchange`) enables concurrent `Put` operations without blocking.
  `SkipListNode.Next` is `internal` so the publish-once invariant is enforced by the compiler.
- **Atomic swap**: When `SizeBytes` exceeds `memTableSizeLimit`, the MemTable is atomically swapped to an immutable MemTable under `mainLock` and **asynchronously** flushed to an SSTable.
  Flushes are sequentialized (at most one in flight) by `FlushCoordinator`.
- **Size accounting**: `Put`/`Delete` add `NODE_OVERHEAD (32) + keyBytes + SEQ_SIZE + valueBytes` (tombstones omit the value term) to `SizeBytes` via `Interlocked.Add`.
  `Flush()` checks the threshold after each direct write.
- **Flush APIs**: `Flush()` (synchronous, waits for completion) and `FlushAsync()` (`Async<unit>`).
  Both propagate flush failures as `AggregateException` (one-shot error slot — the first waiter observes it, then it is cleared).
  `Dispose()` waits for in-flight flush completion before disposing the coordinator.

### SSTable (Sorted String Table)

Immutable on-disk files produced when the MemTable is flushed.
The on-disk layout is `[data][index][bloom][footer]` (footer: always 32 B — `indexOffset`/`bloomOffset`/`maxSeq`/`magic`, magic `0x4C534D54` = `"LSMT"`).

- **In-memory index + Bloom filter**: `SSTable.load` does three sequential reads — footer (from end of file), Bloom filter, then the whole index region in one read.
  The data region is **not touched** at open time.
  `Get` does an in-memory binary search on the `IndexEntry[]`, then reads the value payload at a computed offset via `RandomAccess.Read`.
  The Bloom filter rejects non-existent keys O(1) with no disk I/O.
- **Inline index**: `IndexEntry` is a `[<Struct>]` record (`Key`, `Seq`, `Offset`, `KeyByteLen`, `ValueByteLen`) stored inline in the index region.
  Building the in-memory array needs no data-region access.
  Keys are thus stored twice (data + index).
  The overhead is bounded by Σ key length.
  The index carries the value length, so a point `Get` reads the value payload in a single `RandomAccess.Read`.
- **Range scan via index**: `SSTable.GetRange` uses `lowerBound`/`upperBound` binary searches to find the range.
  Then reads the in-range data region in one `RandomAccess.Read` and parses it in memory.
- **`GetAll` (compaction)**: Reads the entire data region with one `RandomAccess.Read` and parses it in memory.
  Uses the same region-batch pattern as `loadIndex`.
- **Concurrent reads**: `ReaderWriterLockSlim` per SSTable.
  Concurrent `Get`/`GetRange`/`GetAll` proceed in parallel via position-independent `RandomAccess.Read`.
  `Dispose` serializes exclusively.
- **Bloom filters**: FNV-1a double-hashing, 10 bits/item, 7 hash functions.
  The second hash is forced odd (`h2 ||| 1u`) so probes never collapse onto a single bit or a fixed parity.
  **Caveat**: bit placement changed in an earlier build.
  Regenerate SSTables (delete the data directory) when upgrading across that change.
  Old files still load but cannot be trusted (see `docs/trade-off.md`).
- **Atomic install**: `SSTableWriter.writeCore` writes to a `.sst.tmp` file and renames it to `.sst` on success.
  Stale `.tmp` files are deleted on startup.

### Range Scans

In-memory k-way merge across all storage layers: MemTable, immutable MemTable, and each SSTable.

- **Sorted results**: Keys are returned in `String.CompareOrdinal` order, deduplicated.
  The latest visible version (highest `seq ≤ snapshot`) wins.
- **Snapshot-isolated**: `NewIterator` registers its snapshot with `SnapshotManager` **before** collecting sources.
  `RangeScan`/`NewIterator` accept an optional `ISnapshotHandle` for consistent time-travel scans that compaction cannot prune.
  Dispose the iterator (or `use` it) to release the snapshot.
- **Two APIs**: `RangeScan` returns `seq<string * string>` (auto-disposing).
  `NewIterator` returns `IIterator` (manual lifetime).
  `IIterator.Current` before the first `MoveNext()` (or after `false`) throws `InvalidOperationException`.
- **Source materialization**: Each layer produces a `(string * int64 * string option)[]` of in-range entries.
  `MoveNext` then performs an in-memory k-way merge holding no locks.
  For very large ranges, memory is proportional to the in-range entry count.
  Prefer bounded ranges.
- **Disposal safety**: `SSTable.GetRange` returns `RangeDisposed` when a concurrent compaction disposes a table mid-read.
  The whole collection is retried up to `rangeScanMaxRetries` times for both disposal and snapshot drift, then falls back to collecting under `ssTablesLock`.

### Background Multi-Level Compaction & Automatic Pruning

- **Configurable level limits**: e.g., `[| 4; 10; 100; 1000 |]` means L0 over 4 files triggers compaction to L1, L1 over 10 triggers compaction to L2, etc.
  Reducing the **length** of this array for an existing database makes startup **fail fast** (`InvalidDataException`).
  Orphaned files at deleted levels must be removed (or `compactLevelLimits` expanded) first.
- **All-files selection**: When a level exceeds its limit, **all** files in that level are merged into one SSTable at the next level.
  Partial (overflow-only) compaction is unsafe here because Ln files are not guaranteed to be non-overlapping (L0→L1 merges cover the entire key range).
- **Snapshot-aware pruning**: Versions visible to **registered** snapshots (`ISnapshotHandle`, transactions, iterators) are preserved.
  Stale versions are purged.
  `GetMinActiveSnapshot` is re-queried at the start of each merge — never cached.
- **Tombstone elimination**: Deletion markers are completely removed from the final storage level.
  `isLastLevel` → tombstones with `seq < minSnap` and no live value are dropped.
- **Cascade**: A single flush can trigger compaction cascading through multiple levels (`compact` recurses to `level + 1`).
  At most one compaction runs at a time (`CompactionCoordinator`).
- **Compaction APIs**: `WaitForCompaction()` (synchronous) and `WaitForCompactionAsync()` (`Async<unit>`).
  Both propagate compaction failures as `AggregateException` (one-shot error slot — same semantics as the flush APIs).
  `Dispose()` cancels compaction and waits for it to drain.

### Direct Put/Delete (Fast Path)

Single-key `Put` and `Delete` bypass the transaction system entirely.
No `BeginTransaction`/`Commit` overhead, no snapshot registration, no WAL `BEGIN`/`COMMIT` markers.
The operation is written directly to the WAL via `PutSingle`/`DeleteSingle` (with `sync = false`) outside the lock.
Then it is applied to the MemTable under a `mainLock` read lock (`writeWithFlushCheck`), where the flush threshold is re-checked.

### Atomic Transactions with Snapshot Isolation

Multi-key atomic updates via a dedicated `ITransaction` API:

- **Commit & Rollback**: `Commit` writes `BEGIN`, all ops, then `COMMIT` with `sync = true` to the WAL.
  Applies the ops to the MemTable under a single `commitSeq` (one `NextSequence`).
  `Rollback` discards pending changes and releases the snapshot.
- **Read Own Writes**: `ITransaction.Get` reads pending writes within the same transaction before falling back to the engine snapshot (`lsm.Get(key, snapshot)` via `ILsmTree`).
- **Snapshot Isolation**: Each transaction registers its snapshot with `SnapshotManager` at `BeginTransaction` time.
  Ensures consistent reads even during concurrent writes.
- **`IDisposable` lifecycle**: Disposing the transaction releases its registered snapshot.
  Use `use` in F#.
  A leaked transaction (or `ISnapshotHandle`) pins `GetMinActiveSnapshot` and prevents compaction from pruning.
  This causes unbounded disk growth.

---

## Architecture & Internals

See [docs/architecture.md](docs/architecture.md) for the WAL format, SSTable binary layout, concurrency model, compaction algorithm, range-scan algorithm, and MVCC design.
See [docs/trade-off.md](docs/trade-off.md) for design trade-offs and [docs/gotchas.md](docs/gotchas.md) for known pitfalls.

---

## Known Limitations

- **String-only keys/values**: UTF-8 strings only (base64-encoded in WAL).
  Binary data must be base64-encoded by the caller.
- **Single WAL file**: One live `wal.log` per instance, renamed to `wal_<guid>.old` on each flush (deleted after the SSTable is written).
  Stale `.old` files from crashes remain on disk and are scanned during recovery.
- **`LsmTransaction.Get`**: Looks up pending writes via `Dictionary.TryGetValue` (O(1) average).
- **No replication/clustering**: Single-node storage engine only.
- **Empty SSTable flush race**: `MemTable.Put`/`Delete` increment `SizeBytes` **before** inserting into the SkipList.
  A flush triggered between the increment and the insert can produce an empty SSTable (no data loss — the WAL guarantees recovery).
  Tests asserting post-flush SSTable file counts should account for this.

---

## Performance

Run benchmarks locally with:

```bash
dotnet run -c Release --project Benchmark
```

Benchmarks use [BenchmarkDotNet](https://benchmarkdotnet.org/) with `[<MemoryDiagnoser>]`:

| Class | Benchmarks | N | Value Size |
|---|---|---|---|
| `PutBenchmark` | `SequentialPut`, `ConcurrentPut`, `TransactionPut` | 10,000 | 1 / 100 |
| `GetBenchmark` | `RandomHitGet`, `RandomMissGet`, `SequentialGet` | 10,000 / 30,000 | — |
| `DeleteBenchmark` | `SequentialDelete`, `ConcurrentDelete`, `TransactionDelete` | 10,000 | — |
| `MixedWorkloadBenchmark` | `ReadHeavy`, `WriteHeavy` | 10,000 | — |

---

## How to Use and Test

### Prerequisites

- [.NET 10.0 SDK](https://dotnet.microsoft.com/download) or later

### Building

```bash
dotnet build
```

### Running Tests

```bash
dotnet test
```

### Running Benchmarks

```bash
dotnet run -c Release --project Benchmark
```

---

## Usage Examples

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
```

### Range Scans

```fsharp
// Sequential scan (seq<string * string> – auto-disposes)
for key, value in db.RangeScan("user:1", "user:9") do
    printfn "%s = %s" key value

// Manual iterator with explicit lifetime
use it = db.NewIterator("a", "z")
while it.MoveNext() do
    printfn "%s = %s" (fst it.Current) (snd it.Current)

// With snapshot isolation (safe against compaction)
use snap = db.Snapshot()
// ... concurrent writes ...
let pastView = db.RangeScan("a", "z", snapshot = snap) |> Seq.toList
```

### MVCC Time-Travel

```fsharp
db.Put("config:theme", "dark")
use v1 = db.Snapshot()           // registered snapshot handle at version 1

db.Put("config:theme", "light")

let current = db.Get("config:theme")       // Some "light"
let past = db.Get("config:theme", v1)      // Some "dark" — historical view
```

`Snapshot()` returns an `ISnapshotHandle` that **registers** the referenced version with the compaction pruner.
It cannot be pruned while the handle is alive.
Dispose it (or use `use`) to release the version for pruning.
A leaked handle keeps old versions on disk indefinitely.

### Constructor Options

The data directory is **auto-created** if it doesn't exist.
All parameters are optional:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `dataDir` | `string` | (required) | Path to the data directory |
| `memTableSizeLimit` | `int` | 1,048,576 (≈1 MB) | MemTable size threshold for flush |
| `compactLevelLimits` | `int[]` | `[\| 4; 10; 100; 1000 \|]` | Max files per level before compaction (validated: must be non-empty, no negatives) |

```fsharp
// Tune MemTable flush threshold
let db = new LsmTree("./data", memTableSizeLimit = 512 * 1024)  // 512 KB

// Custom compaction level limits
let db = new LsmTree("./data", compactLevelLimits = [| 2; 5; 50 |])
```

### Clean Shutdown

```fsharp
use db = new LsmTree("./data")
// ... work ...

// Force flush MemTable to SSTable and wait for compaction
db.Flush()
db.WaitForCompaction()

// db.Dispose() is called automatically at the end of the 'use' scope
```
