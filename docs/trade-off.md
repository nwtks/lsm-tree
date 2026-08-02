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

**Choice**: `StreamWriter.AutoFlush` is disabled. On every durable write, the code explicitly calls `writer.Flush()` (drains the `StreamWriter` buffer to the `FileStream`) followed by `stream.Flush(true)` (`fsync`) when needed. Transaction `Commit` operations always use `fsync` to ensure durability.

**Why**: `AutoFlush` calls `StreamWriter.Flush()` on every `WriteLine`, which is `FileStream.Flush(false)` — data reaches the OS page cache but not disk. Disabling `AutoFlush` eliminates redundant page-cache flushes on every `WriteLine`.

**Trade-off**: On process crash (SIGKILL), data in the `StreamWriter` buffer that hasn't been explicitly flushed is lost. This window is limited to writes between the last durable `Commit` and the crash — consistent with ACID durability guarantees.

**Alternatives considered**:
- **`AutoFlush = true`**: no crash-process protection for the `StreamWriter` buffer, but redundant `Flush(false)` calls on every line.
- **`stream.Flush(true)` on every `WriteLine`**: ~100–200 ops/s on HDDs; uncommitted data is discarded on recovery anyway.

---

## WAL write path: single lock + single write helper

**Choice**: All WAL mutations (`Put`, `PutSingle`, `Delete`, `DeleteSingle`, `Begin`, `Commit`, `Abort`) funnel through one private `write sync log` helper that takes `walLock`, writes the line, and flushes to disk only when `sync = true`. Non-durable variants (`Put`/`Delete`/`Begin`, used by the single-key fast path and transaction markers) call it with `sync = false`.

**Why**: Previously the "lock + `WriteLine`" pattern was duplicated inline in `Put`/`Delete`/`Begin` while the `*Single`/`Commit`/`Abort` variants used a helper — the flush-on-demand semantics were scattered across five sites. Consolidating keeps the lock scope and durability decision in exactly one place.

**Trade-off**: The `write` helper intentionally does **not** swallow I/O exceptions (unlike `Dispose`, which must not throw). Callers rely on exceptions propagating to the caller of `Put`/`Commit` etc. No behavior change vs. the previous inline code; this is a structural cleanup.

**Alternatives considered**:
- **Batch writer thread + queue**: higher throughput under concurrency, but breaks the `Commit(sync = true)` durability guarantee (returns before fsync) and adds crash-drain complexity.
- **Merge `walLock` into `mainLock`**: invalid — a write lock would serialize all reads; a read lock would leave `StreamWriter` unprotected across concurrent writers.
- **Move WAL writes outside `mainLock`**: invalid — the swap in `flushMemTable` could interleave between the WAL append and the MemTable insert, losing the write when the old WAL is deleted.

---

## Single-key fast path: bypass transactions

**Choice**: `LsmTree.Put` and `LsmTree.Delete` bypass the transaction system entirely. They use `wal.PutSingle`/`wal.DeleteSingle`, which write bare `PUT`/`DEL` lines to the WAL (no `BEGIN`/`COMMIT` markers), and apply the mutation directly to the MemTable under a read lock — no snapshot registration, no `ops` list.

**Why**: The original code created a full transaction (`BeginTransaction` → `Put`/`Delete` → `Commit`) for every single-key write, adding allocation, lock churn, and 2× WAL lines. Since the engine is last-writer-wins, there are no partial-state concerns for single-key writes.

**Trade-off**: WAL recovery must handle orphaned `PUT`/`DEL` lines (without `BEGIN`/`COMMIT`) as committed. This is safe under last-writer-wins semantics. The WAL file is no longer purely transactional — recovery iterates once to find committed + begun sequences, then filters.

**Alternatives considered**:
- **Full transactions for every write**: simpler WAL recovery, but every `Put` pays 2× WAL lines + snapshot overhead.
- **Object pooling for transactions**: reduces allocation but doesn't eliminate WAL marker overhead.

---

## SSTable read path: in-memory index + bloom filter, inline-index format

**Choice**: The on-disk index region stores inline `IndexEntry` records (`seq + offset + keyByteLen + keyBytes`), not just `int64[]` offsets. At open time, `SSTable.load` reads the entire index region in a single `ReadExactly` and parses it in memory with `BinaryPrimitives` — the data region is not touched during open. A `Get` does a pure in-memory binary search on the index, then a single `Seek`+`Read` for the value payload. A Bloom filter (10 bits/item, 7 hash functions) rejects non-existent keys before the binary search.

**Why**: The earlier `int64[] offsets` format forced `loadIndex` to walk the data region at open time (parsing each entry's seq + key header to reconstruct `IndexEntry`). That meant N small reads or seeks through the data region just to build the in-memory index. Inlining all index fields in a separate region consolidates open-time I/O into **one** sequential read of the index region. The data region is read only on demand for value payloads.

**Trade-off**:
- **File size**: Keys are stored twice (once in the data region, once in the index region). The overhead is bounded by Σ key length and is small relative to value payloads in typical workloads.
- **Format is not self-describing across versions**: Older `.sst` files written with the `int64[] offsets` format are not readable by the current loader. This engine does not ship a migration path — restart from an empty directory if upgrading across this format change.

`IndexEntry` is a `[<Struct>]` record — the array stores entries inline, improving CPU cache locality during binary search and avoiding a separate heap allocation per entry. The SSTable class holds only `IndexEntry[]` (no separate `int64[] offsets` field), so per-entry memory cost is the struct layout (~28 bytes) plus the key string reference.

**Alternatives considered**:
- **`int64[]` offsets in index region, walk data region at open**: avoids storing keys twice, but open-time I/O scales with total entry count (each entry's seq+key header must be parsed).
- **Disk-based binary search**: zero index memory, but log₂N seeks per lookup.
- **Memory-mapped I/O**: lets the OS page lazily, but cross-platform behavior varies.

---

## Range Scan: per-source materialize + in-memory merge

**Choice**: `RangeIterator` materializes each source (MemTable / immutable / per-SSTable) into an array of `(key, seq, value)` tuples under appropriate locks, then performs an in-memory k-way merge in `MoveNext` (no locks held during merge).

**Why**:
- The in-memory index (`IndexEntry[]`) on each SSTable supports O(log N) lowerBound/upperBound — adding range support to `SSTable.Get` was ~30 lines of new code.
- Materializing under lock then merging outside locks avoids complex lock-ordering issues and keeps concurrency safe.
- By materializing only the range `[fromKey, toKey]` instead of the entire file, memory overhead is proportional to the result set, not the data size.

**Trade-off**:
- **Memory**: Range entries are materialized into arrays. For a full `["", "\uFFFF"]` scan over 1 million entries, ~32 MB across all sources (at the project's < 1 MB per SSTable scale, this is bounded).
- **O(K) `pickMinKey` per step**: All SSTables at all levels are included as sources (L0 overlap design requires scanning all files). With `compactLevelLimits [|4;10;100;1000|]`, K ≤ ~1114. Most cursors are exhausted early, so the average case is lower, but worst-case remains O(K).
- **O(R) seek/read per entry in range**: For each entry within range, `SSTable.GetRange` issues one `Seek`+`Read`. Under sequential access within a single SSTable, the OS readahead mitigates this.
- **ReadLock duration on SSTables**: Per-SSTable read lock is held during `GetRange` (binary search + sequential reads). Compaction's `GetAll` (WriteLock) and `Dispose` (WriteLock) wait. For small ranges this is negligible.

**Alternatives considered**:
- **Cursor + heap k-way merge (streaming)**: Lower memory, but per-step locking complexity and cursor lifecycle management is severe.
- **`GetAll` + in-memory sort**: O(N) memory per scan — prohibitive at scale.
- **mmap + direct read from disk**: Reduces locking, but adds cross-platform concerns and is premature at this project's scale.


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

## Bloom filter probe spread: h2 forced odd

**Choice**: `BloomFilter.keyIndex` computes probe positions as `(h1 + seed * h2) % bitSize` with `h2` forced odd (`h2 ||| 1u`) before the multiply.

**Why**: An `h2 = 0` key (FNV-1a low 32 bits happen to be 0, probability 2⁻³²) collapsed all 7 probes onto a single bit — a 1-bit fingerprint with a ~50% false-positive rate for that key. An even `h2` (half of all keys) kept every probe at the same parity, silently halving the effective bit space and roughly doubling the false-positive rate. Forcing `h2` odd fixes both at zero memory cost.

**Trade-off**: **Bit placement changed vs. earlier builds.** The on-disk layout (byte count + bytes) is unchanged and old files still load, but probes land at different positions, so bloom data written by older code can produce false negatives when read by new code — and `SSTable.Get` treats a bloom miss as `NotFound`, silently missing keys that exist in old files. SSTables must be regenerated (delete the data directory) when upgrading across this change. This is a pre-release project, so no on-disk version guard was added.

**Alternatives considered**:
- **Power-of-two `bitSize` + mask**: eliminates all modulo bias but up to doubles bloom memory (10n → 16n bits) and changes layout too.
- **Fixed odd constant when `h2 = 0`**: fixes only the 2⁻³² collapse, not the parity waste.
- **Documentation only**: leaves the degradation in place.

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

## SkipListNode: internal `Next`

**Choice**: `SkipListNode.Next` is now an explicit `internal member` (no longer a public auto-property). `Key`/`Seq`/`Value` remain public immutable get-only properties. All access to `Next` (reads via `Volatile.Read`, CAS writes via `Interlocked.CompareExchange`) happens inside the same assembly (module functions in `SkipList.fs` and the `SkipList` class), so `internal` visibility is sufficient.

**Why**: `Next` is the only mutable field — a `SkipListNode[]` written by CAS during lock-free insertion. Lock-free reads (`Volatile.Read(&pred.Next.[lvl])`) rely on the invariant that a *published* node's `Next` array is never rewritten (only unpublished levels of `newNode.Next` are touched during retry). Public exposure of the array let external code corrupt that invariant (`node.Next.[i] <- x`), turning a subtle concurrency bug into a public API hazard. Internalizing it makes the invariant enforceable by the compiler.

**Trade-off**: External code can no longer walk the skip list structure directly (only via `SkipList.Find`/`Entries`/`EntriesRange`), and cannot construct `SkipListNode` instances. The skip list internals become assembly-private — acceptable because the public API (`SkipList`) never returned nodes anyway.

**Alternatives considered**:
- **Full redesign of insertion (publish-once, copy-on-write)**: eliminates the mutable array entirely, but rewrites the core CAS algorithm with high risk to lock-free correctness.
- **Immutable record**: poor fit — the null-terminated linked structure needs `AllowNullLiteral` and per-level mutation during insertion.
- **Documentation + tests only**: no compile-time protection; the hazard remains for any caller of the public API.

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

---

## Async APIs propagate coordinator errors (Option 1)

**Choice**: `FlushAsync()` and `WaitForCompactionAsync()` now call `LockExtensions.checkCoordinatorError` **inside** the `async { }` workflow after awaiting completion — matching the synchronous `Flush()`/`WaitForCompaction()` semantics (`AggregateException` thrown once, error then cleared).

**Why**: Previously the async variants only awaited the coordinator's `ManualResetEvent` and never inspected `coord.Error` — a flush/compaction failure was silently swallowed (only surfacing later via an unrelated synchronous call or as a `[WARN]` log during `Dispose`). Since `asyncFlushToSSTable`/`triggerCompaction` always set `coord.Error` **before** signaling completion, awaiting-then-checking is race-free.

**Trade-off**:
- **Caller must handle exceptions**: `do! db.FlushAsync()` inside `try...with` to observe failures — inherent to the exception style (same as the sync APIs).
- **Single-consumer error**: `checkCoordinatorError` clears the error, so only the first waiter observes it. Flushes are sequentialized and compaction runs at most one at a time, so multiple simultaneous waiters are rare.
- **Sync critical section inside async**: `checkCoordinatorError` takes a short lock on the async thread — negligible, but it is a blocking section in an otherwise async flow.

**Alternatives considered**:
- **Option 2: `Async<Result<unit, exn>>`**: explicit errors, but a breaking change and inconsistent with the sync APIs' exception style.
- **Option 3: error event/callback**: non-breaking, but easy to forget to subscribe (silent again) and adds threading bookkeeping.
- **Option 4: `AwaitCompletion()` returns `Async<exn option>`**: explicit, but requires manual matching and a new design for who clears the error.
- **Option 5: documentation only**: no protection for async-only callers.

---

## Snapshot handle API (Option 1): registered `SnapshotHandle` for compaction-safe reads

**Choice**: `LsmTree.Snapshot()` now returns a **registered** `SnapshotHandle` (`IDisposable`) instead of a raw `int64`. `AcquireSnapshot()` on `LsmTreeSnapshot` registers the current sequence in a refcounted `Map<int64, int>` (reassigned under a lock — an immutable structure keeps the register/release logic side-effect free); `SnapshotHandle.Dispose()` decrements/removes it. `Get(key, snapshot: SnapshotHandle)` (plus `NewIterator`/`RangeScan` with `snapshot = handle`) reads through the registered sequence. A best-effort `Get(key, ?snapshot: int64)` overload is retained for backward compatibility. `NewIterator` now registers its snapshot **before** collecting range sources (previously after — an ordering hole).

**Why**: The previous design was racy. `Snapshot()` returned an **unregistered** `int64`; compaction's `pruneVersions isLastLevel minSnap` could prune a version between the caller's `Snapshot()` and `Get(key, v1)`, returning `None` for a version that existed at snapshot time (time-travel window). `NewIterator` had the same hole reversed: sources were snapshotted before the snapshot was registered, so a compaction between the two could prune versions the iterator was about to materialize.

**Trade-off**:
- **Breaking API change**: `Snapshot()` return type changed from `int64` to `SnapshotHandle`. Callers comparing sequences (e.g., `snapAfter > snapBefore`) must use `.Seq`. Existing tests were updated accordingly.
- **Caller disposal obligation**: A handle that is never disposed (leaked) keeps `minSnap` pinned low forever → compaction cannot prune → unbounded disk growth. F# `use` makes this easy to get right, but it is now the caller's responsibility.
- **Refcounting cost**: `RegisterSnapshot`/`ReleaseSnapshot` take a lock on every acquire/release. Negligible for point operations, but a hot path that acquires/releases snapshots at high frequency pays a small lock cost.
- **Raw `int64` escape hatch remains**: `Get(key, ?snapshot)` and `handle.Seq` still allow unregistered reads. These are documented as **best-effort** — they may still race with pruning. Keeping them preserves source compatibility at the cost of a footgun.

**Alternatives considered**:
- **Option 2: compaction-safe table swapping (atomic rename + hard-link/rename-retry)**: compaction never deletes a table still referenced by an active reader; the reader keeps a file handle. No API change, but requires OS-level file lifecycle coupling and leaves tombstone versions on disk indefinitely for the reader's lifetime.
- **Option 3: version retention markers (write `RETAIN <seq>` markers)**: compaction leaves a marker table instead of pruning; GC runs later. Keeps `int64` API but adds a second pass and more disk churn.
- **Option 4: epoch-based reclamation**: reads join an epoch; compaction defers pruning until the epoch drains. No API change, but adds per-read epoch bookkeeping and can stall pruning under long-lived readers.
- **Option 5: copy-on-write / refcounted SSTables**: table-level refcounts instead of sequence-level. More memory per table and more complex lifecycle management.

---

## Shrinking `compactLevelLimits`: fail-fast on startup (Option 1)

**Choice**: `LsmTreeLoader.loadSSTableFiles` throws `System.IO.InvalidDataException` if any `*.sst` file's level is `>=` the configured level count (`compactLevelLimits.Length + 1`). The message names the offending file and the minimum required `compactLevelLimits` length.

**Why**: The previous behavior silently skipped out-of-range files. Consequences: (1) **data loss** — the file's WAL was already deleted after flush, so contents are unrecoverable; (2) **`currentSeq` regression** — skipped files' `MaxSeq` is not absorbed, so a later restart with the original config can resurrect pruned old data over newer writes (last-writer-wins breaks); (3) **orphaned files accumulate** and can unexpectedly reappear if the config is later expanded. Refusing to start surfaces the misconfiguration immediately instead of corrupting data.

**Trade-off**:
- **Breaking change**: existing databases opened with a smaller config now fail to start instead of silently degrading. Recovery requires restoring the original (or longer) `compactLevelLimits` or manually removing the orphaned files.
- **No auto-recovery**: the engine stops and leaves the decision to the operator; there is no in-place migration.
- **Level-count-only validation**: files below the count but written under different limit *values* (e.g., different deepest-level pruning semantics) are not detected — validation is based on file names only.

**Alternatives considered**:
- **Option 2: auto-migration (rename orphaned files into the new deepest level)**: transparent startup, but silently absorbs config mistakes and changes tombstone-pruning semantics (old middle-level files become deepest level).
- **Option 3: persisted manifest**: records `compactLevelLimits` at open time and validates on restart. Detects intent changes, but adds a new on-disk artifact, a fallback path for old databases, and new corruption modes.
- **Option 4: warn + absorb `MaxSeq` only**: keeps startup working but still drops data — only fixes the sequence regression, not the loss.
- **Option 5: documentation only**: no protection.

## Point Get: `ssTablesLock` snapshot + skip disposed tables

**Choice**: `LsmTreeSearch.searchInTables` copies the level's `SSTable list` reference under `ssTablesLock` and performs the disk I/O (`SSTable.Get`) **outside** the lock. `SSTable.Get` catches `ObjectDisposedException` and returns `NotFound`, so a table disposed by a concurrent compaction after the snapshot is silently skipped and the search continues to lower levels.

**Why**: Point Get previously held `ssTablesLock` (a plain `obj` monitor) for the entire `Seek` + `Read` I/O. This fully serialized concurrent point Gets even though reads are read-only, and a slow Get blocked flush/compaction (`addSSTable` L0 registration, `replaceLevelTables`, `triggerCompaction` flag ops). F# lists are immutable and array slots are only ever reassigned (never mutated), so copying the reference is a consistent, O(1) snapshot — there is no torn-list risk.

**Safety invariant**: This is correct only because compaction retains **all** entries with `seq >= minSnap` in the merged table at the next level (`pruneVersions`). When the search falls through a disposed L0 table, its data (at `seq >= minSnap`) is guaranteed present in the next level's merged table, and every registered snapshot is `>= minSnap`. Disposal after snapshot is safe because `SSTable.Get`'s read-lock acquisition throws `ObjectDisposedException` once `rwLock.Dispose()` has run — the `try/with` wraps the whole `withReadLock` call, so both the `EnterReadLock` failure and an in-flight read failure map to `NotFound`.

**Trade-off**:
- **Torn-snapshot window**: a Get can observe a state where an L0 table is already disposed but its merged replacement is not yet visible. This is harmless for point Get (data is found at the next level). Range scans cannot use this fallthrough — they use a separate snapshot + retry pattern (see the Range Scan section below).
- **`ObjectDisposedException` catch scope**: the catch also swallows a hypothetical reader-side disposal bug (a genuine double-dispose while reading). For point Get the cost is a false `NotFound`; compaction invariants keep the correct answer reachable at a lower level.
- **`NotFound` on disposed table**: `Get` on an explicitly disposed table (e.g., a caller bug) now returns `NotFound` instead of throwing — behavior change, masked by the same catch.

**Alternatives considered**:
- **Convert `ssTablesLock` to `ReaderWriterLockSlim`**: read-lock during I/O, write-lock for structure changes. Preserves structure safety without copying, but leaves the disposal lifecycle coupled to the lock — compaction's `cleanupSSTables` (dispose + file delete) would need to stay under the write lock or be deferred, and the lock still serializes the I/O critical section during long reads.
- **Snapshot + refcount**: track active readers per table and defer disposal until the count drops. Removes the `NotFound` fallback entirely, but adds per-table refcount bookkeeping and a second lock.
- **Docs only**: document the serialization as a known limit; no behavior change.

## Range Scan: `ssTablesLock` snapshot + retry on disposal

**Choice**: `LsmTree.collectRangeSources` copies the level-list array under `ssTablesLock`, then performs `SSTable.GetRange` I/O **outside** the lock. `SSTable.GetRange` returns `RangeDisposed` when a concurrent compaction disposes a table mid-read (catch of `ObjectDisposedException`); otherwise it returns `RangeOk entries`. If any table in the snapshot is detected as disposed, **or** the snapshot's list references no longer match the current ones (checked under `ssTablesLock`; F# lists are immutable, so reference equality suffices), the entire collection — MemTable + immutable + SSTable — is retried. After 8 retries it falls back to collecting under `ssTablesLock` (the pre-change behavior), which is safe because structure changes are blocked.

**Why**: Range scans cannot use the point-Get fallthrough: the merge over all levels is a union with no "next level" to continue to, so a table disposed after a snapshot would silently lose that table's data (torn read). But disposal only ever happens **after** the table is removed from the list (`replaceLevelTables` under lock → `cleanupSSTables` outside), so a re-collected snapshot is always the current truth and retries converge. Retrying the whole collection also keeps the snapshot consistent across mem/sst: a flush between the mem read and the sst read would otherwise duplicate entries (the immutable MemTable appears in both reads) — tolerated, but re-collecting avoids it. Reference-change validation catches a removed+disposed table that the reader never touched (e.g., disposed before its `GetRange` was attempted because it fell outside the requested range) — disposal detection alone is not a reliable signal in that case.

**Trade-off**:
- **Retry I/O amplification**: each retry re-reads all sources. Rare in practice (requires a compaction completing mid-scan), bounded by the retry cap + locked fallback.
- **`ObjectDisposedException` as control flow**: `GetRange` converts it to `RangeDisposed`. An empty result is a legitimate `RangeOk [||]` and must not be confused with disposal — this is why the retry signal lives in the DU, not in the array result.
- **Latency spike**: the locked fallback re-introduces the pre-change serialization, but only after 8 failed attempts (effectively never; requires continuous compaction churn during the scan).
- **Theoretical livelock**: under unbounded concurrent compaction, retries could repeat indefinitely — bounded by the cap, after which the locked fallback always succeeds because structure changes are blocked.

**Alternatives considered**:
- **Retry only on `RangeDisposed` (no reference validation)**: cheaper (no second lock pass), but misses a removed+disposed table the reader never touched → torn read. Reference validation is required for correctness.
- **Skip disposed tables like point Get**: unsafe for scans (union has no fallthrough) — rejected.
- **Swallow disposal inside `GetRange` (return empty)**: would silently produce a torn read — rejected.
- **ReaderWriterLockSlim / refcount / deferred disposal**: as in the point-Get section; rejected for the same reasons.
