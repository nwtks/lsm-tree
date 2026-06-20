# Design Trade-offs

## MemTable flush: async, fire-and-forget, sequentialized

**Choice**: MemTable flush dispatches a fire-and-forget `async { } |> Async.Start`. At most one flush runs at a time, coordinated by `FlushCoordinator` (a `ManualResetEvent` + lock). If a flush is in-flight when the next swap is needed, the caller blocks on `completedEvent.WaitOne()`.

**Why**: The swap + dispatch overhead on the calling thread is ~microseconds. For typical workloads, flushes are infrequent relative to writes, so back-pressure is rare. When it does occur, blocking is the correct behavior — a second concurrent flush would consume more memory and disk I/O without meaningful throughput gain.

**Trade-off**: Burst writes that fill the MemTable faster than a single flush can write to disk will stall. Parallel flushes would absorb bursts better but require sequence-number gating to ensure the correct SSTable installation order.

**Alternatives considered**:
- **Synchronous flush**: predictably blocks the caller for the full SSTable write duration (tens of ms for large MemTables).
- **Parallel flushes**: higher peak memory and disk I/O; ordering complexity.

---

## WAL durability: explicit flush, no AutoFlush

**Choice**: `StreamWriter.AutoFlush` is disabled. On every durable write (`PutSingle`, `DeleteSingle`, `Commit` with `sync=true`), the code explicitly calls `writer.Flush()` (drains the `StreamWriter` buffer to the `FileStream`) followed by `stream.Flush(true)` (`fsync`).

**Why**: `AutoFlush` calls `StreamWriter.Flush()` on every `WriteLine`, which is `FileStream.Flush(false)` — data reaches the OS page cache but not disk. Only `Commit` actually calls `stream.Flush(true)`. So `AutoFlush` produced redundant page-cache flushes on every `WriteLine` without adding durability.

**Trade-off**: On process crash (SIGKILL), data in the `StreamWriter` buffer that hasn't been explicitly flushed is lost. This window is limited to writes between the last durable `Commit` and the crash — consistent with ACID durability guarantees. The `sync=false` mode skips `fsync` entirely, trading durability for throughput.

**Alternatives considered**:
- **`AutoFlush = true`**: no crash-process protection for the `StreamWriter` buffer, but redundant `Flush(false)` calls on every line.
- **`stream.Flush(true)` on every `WriteLine`**: ~100–200 ops/s on HDDs; uncommitted data is discarded on recovery anyway.

---

## Single-key fast path: bypass transactions

**Choice**: `LsmTree.Put` and `LsmTree.Delete` bypass the transaction system entirely. They use `wal.PutSingle`/`wal.DeleteSingle`, which write bare `PUT`/`DEL` lines to the WAL (no `BEGIN`/`COMMIT` markers), and apply the mutation directly to the MemTable under a read lock — no snapshot registration, no `ops` list.

**Why**: The original code created a full transaction (`BeginTransaction` → `Put`/`Delete` → `Commit`) for every single-key write, adding allocation, lock churn, and 2× WAL lines. Since the engine is last-writer-wins, there are no partial-state concerns for single-key writes.

**Trade-off**: WAL recovery must handle orphaned `PUT`/`DEL` lines (without `BEGIN`/`COMMIT`) as committed. This is safe under last-writer-wins semantics. The WAL file is no longer purely transactional — recovery iterates once to find committed + begun sequences, then filters.

**Alternatives considered**:
- **Full transactions for every write**: simpler WAL recovery, but every `Put` pays 2× WAL lines + snapshot overhead.
- **Object pooling for transactions**: reduces allocation but doesn't eliminate WAL marker overhead.

---

## SSTable read path: in-memory index + bloom filter

**Choice**: At open time, each SSTable scans all entries and builds an in-memory `IndexEntry[]` (key, sequence number, disk offset, key byte length). A `Get` does a pure in-memory binary search on the index, then a single `Seek`+`Read` for the value payload. A Bloom filter (10 bits/item, 7 hash functions) rejects non-existent keys before the binary search.

**Why**: The original design did a `Seek`+`Read` on every binary search iteration (~log₂N disk seeks per lookup). The in-memory index reduces this to one seek per hit. The Bloom filter makes misses O(1) with no disk I/O.

**Trade-off**: Memory cost is ~32+ bytes per entry (two `int64` fields, one `int32`, plus the key string reference). For 1M entries, that's ~32 MB across all SSTables. At this project's scale, bounded by `memTableSizeLimit`, the trade-off is favorable.

`IndexEntry` is a `[<Struct>]` record — the array stores entries inline, improving CPU cache locality during binary search and avoiding a separate heap allocation per entry.

**Alternatives considered**:
- **Disk-based binary search**: zero index memory, but log₂N seeks per lookup.
- **Memory-mapped I/O**: lets the OS page lazily, but cross-platform behavior varies.
- **Prefetch entire data region at open time**: eliminates all seeks, but memory is proportional to total data size.

---

## SSTable concurrent reads: `ReaderWriterLockSlim`

**Choice**: `SSTable.Get` takes a read lock (`withReadLock`). `GetAll` (used by compaction) and `Dispose` take a write lock (`withWriteLock`). Multiple readers proceed concurrently; a writer blocks all readers.

**Why**: The in-memory index made binary search lock-free. Only the final `Seek`+`Read` payload access needs protection. A mutual-exclusion lock would serialize concurrent readers on hot SSTables even though each read is fast (~microseconds).

**Trade-off**: `ReaderWriterLockSlim.Dispose()` cannot be called while any lock is held. `SSTable.Dispose()` uses the `shouldDispose` flag pattern: take write lock → set `disposed` flag → release file handles → return `true` → caller disposes `rwLock` *outside* the lock scope.

**Alternatives considered**:
- **`lock fs`**: simple, but serializes all readers.
- **`SemaphoreSlim(1,1)`**: doesn't distinguish readers from writers.

---

## Compaction: full-level, cascade, at most one at a time

**Choice**: When a level exceeds its file-count limit, *all* files in that level are merged into one SSTable at the next level. Compaction cascades recursively. At most one compaction runs at a time (`CompactionCoordinator`).

**Why**: In this implementation, all levels (including Ln for n>0) may contain overlapping key ranges — L0→L1 merges cover the entire key range, producing L1 files with overlapping keys. Partial compaction would leave old versions in the level that shadow newer versions in higher levels.

**Trade-off**: Full-level compaction rewrites all files in a level even if only one file exceeds the limit. This amplifies write I/O for levels with many small files. A leveled compaction strategy (e.g., RocksDB's level compaction) that picks one file + overlapping range would reduce write amplification.

**Alternatives considered**:
- **Partial compaction**: smaller write batches, but old versions in higher levels shadow newer ones.
- **Size-tiered compaction**: merges files of similar size, avoiding rewriting the entire level.

---

## Transaction local `Get`: O(n) linear scan

**Choice**: `LsmTransaction.Get` scans the pending `ops` list linearly with `Seq.tryFind`. No index, no map.

**Why**: The `ops` list keeps the transaction implementation trivially simple and correct. Transactions are designed as short-lived, small-scope units — the O(n) scan only becomes noticeable at thousands of uncommitted keys, which is an unlikely usage pattern.

**Trade-off**: With N uncommitted entries, a `Get` for a non-existent or oldest key visits all N elements. If large write-only-then-read transactions become a first-class use case, this will be a performance trap.

**Alternatives considered**:
- **`Map<string, string option>`**: O(log N) `Get`, but `Commit` must materialize a list from the map, adding allocation overhead even for tiny transactions.
- **Mutable `Dictionary`**: O(1) `Get`, but deviates from functional idioms and still requires list conversion on commit.

---

## BloomFilter `byte[]`: no pooling

**Choice**: Each SSTable allocates a fresh `byte[]` for its bloom filter — once during flush (`Array.zeroCreate`) and once during load (`br.ReadBytes`). The array lives as long as the SSTable.

**Why**: Bloom filter lifetimes are tied to SSTable lifetimes. When compaction merges files, old SSTables (and their bloom filters) are disposed and GC'd together. Pooling (`ArrayPool<byte>`) would require tracking `bitSize` separately (pooled arrays are oversized) and risk use-after-return bugs.

**Trade-off**: With many open SSTables, bloom filter memory adds up (~1.25 KB per SSTable at default `memTableSizeLimit`). At this project's scale, the overhead is acceptable.

**Alternatives considered**:
- **`ArrayPool<byte>`**: reduces allocation but adds complexity (oversized arrays, use-after-return risk).
- **Lazy load on first access**: skips allocation for unqueried SSTables, but adds branch on every `Get`.

---

## Dispose: errors swallowed silently

**Choice**: `WAL.Dispose()` catches all I/O errors from `writer.Flush()`, `stream.Flush(true)`, and the `Dispose()` calls — printing only to stderr. `LsmTree.Dispose()` similarly swallows SSTable disposal errors. Neither propagates exceptions.

**Why**: .NET `Dispose()` guidelines say `Dispose` should not throw. If disposal fails, the try/finally chain in `LsmTree.Dispose()` must still run to completion to release remaining resources. WAL recovery tolerates truncation, so engine integrity is preserved on next startup.

**Trade-off**: Monitoring tools cannot detect a failed final flush. If the last bytes aren't durable, they're recovered from WAL on restart — acceptable for crash-safety, but invisible to operators.

**Alternatives considered**:
- **Propagate errors from `Dispose`**: violates .NET guidelines; skips subsequent cleanup.
- **Explicit `Close()` that returns errors**: breaking API change; `use` pattern no longer usable.

---

## API: string-only, point queries only

**Choice**: Keys and values are UTF-8 strings. Binary data is supported via base64 encoding by the caller. The public API supports point lookups only (`Get`); `SSTable.GetAll()` is internal, used exclusively during compaction.

**Why**: String keys simplify the WAL text format, the binary search (`String.CompareOrdinal`), and the Bloom filter hash. Restricting to point lookups keeps the API minimal and the search chain (`MemTable → immutable → SSTable levels`) straightforward.

**Trade-off**: Callers with binary keys pay base64 encoding overhead on every operation. Range queries would require an iterator API and merging across multiple SSTables + MemTable.

**Alternatives considered**:
- **Binary keys/values**: more general, but complicates WAL parsing and index comparison.
- **Range queries**: requires iterator merging across all storage levels; significant API and implementation complexity.

---

## SearchResult: struct DU replaces `(string option) option`

**Choice**: The internal lookup chain returns `SearchResult`, a `[<Struct>]` discriminated union with three cases: `Found of string`, `Tombstone`, `NotFound`. Previously it used nested `string option option` where `Some(Some v)` = live, `Some None` = tombstone, `None` = not found.

**Why**: Every `Get` on the hot path allocated two heap objects just to return a three-state result. `SearchResult` eliminates all allocation — all three cases are value types. It also makes the cases explicit in the type system, so the compiler enforces exhaustive matching.

**Trade-off**: The `SearchResult` type is defined in `SkipList.fs` (the earliest compilation unit that needs it). Conversion to `string option` happens only at the public API boundary (`LsmTree.Get`, `ITransaction.Get`), adding a small match overhead there while the internal hot path avoids boxing entirely.

---

## No structured concurrency: fire-and-forget async

**Choice**: Both flush and compaction use `async { } |> Async.Start` — fire-and-forget computations with no `CancellationToken` linking back to the parent scope. Coordination is purely via `ManualResetEvent` (completion signaling) and `CompactionCoordinator.Cancel()` (called by `Dispose`).

**Why**: Simple coordination model that avoids the complexity of `Async` cancellation token propagation chains. The `ManualResetEvent` pattern is well-understood and easy to reason about.

**Trade-off**: A fire-and-forget flush's `finally` block may execute after `LsmTree.Dispose()` has already disposed the coordinator's `ManualResetEvent`, causing `ObjectDisposedException`. This is mitigated by the `disposed` guard in `CompactionCoordinator.SetCompleted()`, `FlushCoordinator.SignalCompleted()`, and `FlushCoordinator.AcquireAndReset()` — the latter returns `bool` (`false` if disposed, aborting the flush cycle). Additionally, `triggerCompaction` checks `IsCancellationRequested` to prevent starting new compactions after `Cancel()`. New background operations must follow the same guard pattern.

**Alternatives considered**:
- **Linked `CancellationTokenSource`**: ensures child tasks are cancelled before parent disposal, but adds complexity for coordinating three independent async scopes (flush, compaction, dispose).
- **Task-based with `Task.WhenAll`**: more structured, but `.NET` `Task` doesn't have built-in `Async` interop without `Async.StartAsTask`.
