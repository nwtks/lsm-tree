# Recurring Gotchas

### MemTable flush races with Put/Delete

See [trade-off.md](trade-off.md) (`MemTable flush: async, fire-and-forget, sequentialized`).
No data loss occurs, but an empty SSTable may be produced.
`MemTable.Put`/`Delete` increment `sizeBytes` before inserting into the SkipList, so a flush triggered between the increment and insert can produce an empty SSTable.
Be aware when asserting post-flush SSTable file counts in tests.

### `do...use` scoping avoids double-dispose

When testing restart (create engine → close → reopen), use `do...use tree = new LsmTree(...)` to scope the first engine's lifetime explicitly, then `use tree2 = new LsmTree(...)` for the second.
Calling `.Close()` before a `use` block ends triggers a double-dispose.
`LsmTree.Dispose()` guards against this with a `disposed` flag and `LockExtensions.disposeOf`, but relying on this is fragile and violates the `IDisposable` contract.

### WAL rename during flush: close the handle first (Windows)

`swapMemTableAndWal` renames `wal.log` → `wal_<guid>.old` on every flush.
The WAL `FileStream` is opened with `FileShare.Read` (no `FileShare.Delete`), so on **Windows** the rename must happen **after** `wal.Close()`.
Renaming while the handle is open throws `IOException` (sharing violation) — it only worked on Linux because POSIX `rename` ignores open handles.
Keep the order: `wal.Close()` then `File.Move`.
Any new code that renames an open file it owns must close it first (or open with `FileShare.Delete`).
This only affects Windows; existing tests run on Linux and do not exercise the failure.

### SSTable `ReaderWriterLockSlim` double-dispose safety

`SSTable.Dispose()` must dispose the `FileStream` (`fs`) under a write lock.
`ReaderWriterLockSlim.Dispose()` cannot be called while any lock is held.
The fix is the `shouldDispose` flag pattern:

```fsharp
let shouldDispose =
    LockExtensions.withWriteLock rwLock (fun () ->
        if not disposed then
            disposed <- true
            fs.Dispose()
            true
        else false)
if shouldDispose then rwLock.Dispose()
```

If you change this pattern, call `rwLock.Dispose()` **outside** the `withWriteLock` scope.
Calling it inside deadlocks or throws.

### Full-level cascade in tests

When testing compaction cascading across multiple levels, set `compactLevelLimits` to a short array (e.g., `[| 2 |]`) so a few L0 files trigger a cascade.
Populate keys with `Put` + `Flush()` in a loop, then call `WaitForCompaction()` **once at the end** to let the cascading compaction rounds drain.

### Background coordinator races on Dispose

`LsmTree.Dispose()` cancels compaction, waits for both compaction and flush, then disposes both `CompactionCoordinator` and `FlushCoordinator`.
The fire-and-forget `asyncFlushToSSTable` may call `triggerCompaction` or `flushCoordinator.SignalCompleted()` **after** `Dispose()` has already disposed the coordinator's `ManualResetEvent`.

**Fix** (see `LsmTreeFlush.fs`):
1. `triggerCompaction` checks `compaction.Token.IsCancellationRequested`.
   Prevents starting new compactions after `Cancel()` during dispose.
2. `CompactionCoordinator.SetCompleted()` and `FlushCoordinator.SignalCompleted()` have a `disposed` flag guard.
3. `FlushCoordinator.AcquireAndReset()` checks the `disposed` flag under `flushLock` and returns `false` if disposed, so a new flush cycle is skipped during shutdown.
4. `CompactionCoordinator.Token` is captured with `member val` (not `member _`) so the `CancellationToken` object survives CTS disposal.

If you change these coordinators or add a new background async operation that calls back into them, ensure the disposed-guard pattern is preserved.

### RangeIterator dispose releases snapshot

`NewIterator` registers its snapshot sequence **before** collecting range sources (see `LsmTree.NewIterator`) and `RangeIterator` releases it at `Dispose()`.
If you forget to call `Dispose()` (or don't use `use` in F#), the snapshot remains active in `SnapshotManager`, preventing compaction from pruning stale versions.
This can cause unbounded disk growth.
Always `use` iterators or call `Dispose()` explicitly.

### RangeIterator snapshot refcount: exactly one owner

`RangeIterator`'s constructor must **not** register the snapshot itself.
The single registration comes from `NewIterator` (before source collection); `Clone()` registers for each new enumerator, and `Dispose()` releases once.
A historical bug registered the snapshot **twice** per iterator (once in `NewIterator`, once in the `RangeIterator` constructor) but released only once — every `NewIterator`/`RangeScan` leaked one registration, permanently pinning `GetMinActiveSnapshot` at the iterator's seq and blocking compaction pruning (unbounded disk growth).
Regression test: `Disposed iterator releases its snapshot registration`.
If you change iterator construction, keep the invariant: **one acquire (in `NewIterator` or `Clone`) ↔ one release (in `Dispose`)**.

### Snapshot handle leak pins pruning

`Snapshot()` returns a registered `ISnapshotHandle`.
If the handle is never disposed, its sequence stays in the refcounted active-snapshot registry.
`GetMinActiveSnapshot()` never advances past it.
Compaction cannot prune **any** older versions — unbounded disk growth.
Use `use` (always `Dispose()` on every path).
Handles are **refcounted**: `NewIterator` re-registers the same sequence internally, so the same sequence may appear with count > 1.
Each acquire needs a matching release.
Double-dispose is safe (a missing entry is a no-op).

### SSTable reads use `RandomAccess.Read`, not a shared stream position

`Get`, `GetRange`, and `GetAll` all read via `RandomAccess.Read` at explicit file offsets.
There is no shared `FileStream` position or `BinaryReader` to corrupt.
Each read is position-independent and thread-safe, so concurrent readers do not interfere.
Calling `GetRange` twice returns identical results.
When changing these methods, always read at an explicit offset.
Never reintroduce a shared stream position (`Seek` + sequential `Read`) — that is the class of concurrency bug the `RandomAccess` refactor fixes.

### Shrinking `compactLevelLimits` refuses to start (fail-fast)

Reducing the **length** of `compactLevelLimits` (e.g., `[|4;10;100;1000;2000|]` → `[|4;10;100|]`) leaves existing SSTables at levels beyond the new configuration.
The loader previously **silently skipped** them: data was lost (the file's WAL was already deleted after flush), `currentSeq` regressed (a later restart with the original config could resurrect pruned old data over newer writes), and orphaned files leaked on disk.
Now `LsmTreeLoader.loadSSTableFiles` throws `InvalidDataException` naming the file and the minimum required `compactLevelLimits` length.
To intentionally shrink levels, remove (or move out) the orphaned files first.
Then restart with the new config.

### `RangeIterator.Current` before `MoveNext` throws

Accessing `IIterator.Current` before the first `MoveNext()` call (or after `MoveNext()` returns `false`) throws `InvalidOperationException`.
This matches standard .NET iterator conventions.
Use the `RangeScan` API (returns `seq`) to avoid manual `MoveNext` management.

### Async APIs now propagate coordinator errors

`FlushAsync()` and `WaitForCompactionAsync()` call `LockExtensions.checkCoordinatorError` inside the `async { }` workflow after awaiting completion.
A background flush/compaction failure raises `AggregateException` instead of being silently swallowed.
Callers must `try...with` around `do! db.FlushAsync()`.
The error is cleared on read (one-shot): only the first waiter sees it, so a failing background operation can be observed by at most one async caller (plus the sync APIs share the same one-shot error slot).
This matches the synchronous `Flush()`/`WaitForCompaction()` behavior — see [trade-off.md](trade-off.md).

### Materialized array memory for large ranges

`RangeIterator` materializes all entries within `[fromKey, toKey]` into in-memory arrays during construction (`NewIterator`).
For very large ranges (e.g., full database scan on a dataset with millions of keys), this consumes memory proportional to the number of entries in range.
Prefer bounded range scans.
If full-database iteration is needed, batch via multiple smaller range scans.

### Bloom filter probe spread: h2 forced odd

`BloomFilter.keyIndex` computes probe positions as `(h1 + seed * h2) % bitSize` with `h2` forced odd (`h2 ||| 1u`).
Without this, a key whose FNV-1a low 32 bits are 0 would set/check the same bit for all 7 probes (a 1-bit fingerprint), and an even `h2` keeps every probe at a fixed parity.
Half the bit space is never used.
**Compatibility caveat**: this changes bit placement relative to earlier builds.
Bloom data written by older code is probed at different positions by new code.
Since `SSTable.Get` treats a bloom miss as `NotFound`, keys that exist in old SSTables can be silently missed (false negative).
Regenerate SSTables (delete the data directory) after upgrading across this change.
The on-disk layout is unchanged, so old files still load — they just can't be trusted.

### Point Get = snapshot + skip; range scan = snapshot + retry

Point Get (`LsmTreeSearch.searchInTables`) copies the level list under `ssTablesLock` and reads **outside** the lock. `SSTable.Get` catches `ObjectDisposedException` → `NotFound` and the search falls through to the next level where compaction's merged table holds the same data (`minSnap` retention invariant).
Range scan (`LsmTree.tryCollectRangeSources`) also snapshots under `ssTablesLock`, but **must not skip** — a scan's merge over all levels is a union with no fallthrough, so a disposed table would silently lose data.
Instead it reads the snapshot outside the lock and **retries the whole collection** when `SSTable.GetRange` returns `RangeDisposed` or the snapshot's list references no longer match the current ones.
After 8 retries it falls back to collecting under `ssTablesLock`.
Both patterns are safe only because disposal happens strictly after list removal (`replaceLevelTables` under lock → `cleanupSSTables` outside).
See [trade-off.md](trade-off.md) (`Range Scan: ssTablesLock snapshot + retry on disposal`).

### `SSTable.Get` on a disposed table returns `NotFound`, not an exception

`SSTable.Get` catches `ObjectDisposedException` (thrown by `EnterReadLock` on a disposed `rwLock`, or by an in-flight read after disposal) and returns `NotFound`.
Callers can no longer rely on an exception to detect a use-after-dispose bug.
Tests that assert `Get` throws after `Dispose()` will now fail.
`SSTable.GetRange` does the same and returns `RangeDisposed` — the caller must retry the whole collection.
Never treat it as an empty range (`RangeOk [||]` is the legitimate empty result).
See [trade-off.md](trade-off.md) (`Point Get: ssTablesLock snapshot + skip disposed tables`).
