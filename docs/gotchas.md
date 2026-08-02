# Recurring Gotchas

### MemTable flush races with Put/Delete

See [trade-off.md](trade-off.md) (`MemTable flush: async, fire-and-forget, sequentialized`). No data loss occurs, but an empty SSTable may be produced — `MemTable.Put`/`Delete` increment `sizeBytes` before inserting into the SkipList, so a flush triggered between the increment and insert can produce an empty SSTable. Be aware when asserting post-flush SSTable file counts in tests.

### `do...use` scoping avoids double-dispose

When testing restart (create engine → close → reopen), use `do...use tree = new LsmTree(...)` to scope the first engine's lifetime explicitly, then `use tree2 = new LsmTree(...)` for the second. Calling `.Close()` before a `use` block ends triggers a double-dispose — `LsmTree.Dispose()` guards against this with a `disposed` flag and `LockExtensions.disposeOf`, but relying on this is fragile and violates the `IDisposable` contract.

### SSTable `ReaderWriterLockSlim` double‑dispose safety

`SSTable.Dispose()` must release the file handles (`br`/`fs`) under a write lock, but `ReaderWriterLockSlim.Dispose()` itself cannot be called while any lock is held. The fix is the `shouldDispose` flag pattern:

```fsharp
let shouldDispose =
    LockExtensions.withWriteLock rwLock (fun () ->
        if not disposed then
            disposed <- true
            br.Dispose()
            fs.Dispose()
            true
        else false)
if shouldDispose then rwLock.Dispose()
```

If you ever change this pattern, ensure `rwLock.Dispose()` is called **outside** the `withWriteLock` scope. Calling it inside will deadlock or throw.

### Full-level cascade in tests

When testing compaction cascading across multiple levels, set `memTableSizeLimit` large enough (e.g., `1024 * 1024`) to prevent auto-flushes during data loading. Create L0 files via manual `Flush()` calls followed by `WaitForCompaction()` to let each cascade round complete before the next.

### Background coordinator races on Dispose

`LsmTree.Dispose()` cancels compaction, waits for both compaction and flush, then disposes both `CompactionCoordinator` and `FlushCoordinator`. However, the fire-and-forget `asyncFlushToSSTable` may call `triggerCompaction` or `flushCoordinator.SignalCompleted()` **after** `Dispose()` has already disposed the coordinator's `ManualResetEvent`.

**Fix** (see `LsmTreeFlush.fs`):
1. `triggerCompaction` checks `compaction.Token.IsCancellationRequested` — prevents starting new compactions after `Cancel()` during dispose.
2. `CompactionCoordinator.SetCompleted()` and `FlushCoordinator.SignalCompleted()` have a `disposed` flag guard.
3. `FlushCoordinator.AcquireAndReset()` checks the `disposed` flag under `flushLock` and returns `false` if disposed, so a new flush cycle is skipped during shutdown.
4. `CompactionCoordinator.Token` is captured with `member val` (not `member _`) so the `CancellationToken` object survives CTS disposal.

If you ever change these coordinators or add a new background async operation that calls back into them, ensure the disposed-guard pattern is preserved.

### RangeIterator dispose releases snapshot

`NewIterator` registers its snapshot sequence **before** collecting range sources (see `LsmTree.NewIterator`) and `RangeIterator` releases it at `Dispose()`. If you forget to call `Dispose()` (or don't use `use` in F#), the snapshot remains active in `SnapshotManager`, preventing compaction from pruning stale versions. This can cause unbounded disk growth. Always `use` iterators or call `Dispose()` explicitly.

### Snapshot handle leak pins pruning

`Snapshot()` returns a registered `SnapshotHandle`. If the handle is never disposed, its sequence stays in the refcounted active-snapshot registry forever, so `GetMinActiveSnapshot()` never advances past it and compaction cannot prune **any** older versions — unbounded disk growth. Use `use` (or ensure `Dispose()` on every path). Handles are **refcounted**: `NewIterator` re-registers the same sequence internally, so the same sequence may appear with count > 1; each acquire needs a matching release. Double-dispose is safe (a missing entry is a no-op).

### Raw `int64` snapshot reads are best-effort

`Get(key, ?snapshot: int64)` (and `handle.Seq` passed directly) do **not** register the sequence. Compaction can prune the version between the snapshot read and the lookup, returning `None` for a version that existed moments earlier. This is the exact race Option 1 (snapshot handle API) was designed to fix — prefer `SnapshotHandle` in new code. The `int64` overload exists only for backward compatibility.

### Shrinking `compactLevelLimits` refuses to start (fail-fast)

Reducing the **length** of `compactLevelLimits` (e.g., `[|4;10;100;1000;2000|]` → `[|4;10;100|]`) leaves existing SSTables at levels beyond the new configuration. The loader previously **silently skipped** them: data was lost (the file's WAL was already deleted after flush), `currentSeq` regressed (a later restart with the original config could resurrect pruned old data over newer writes), and orphaned files leaked on disk. Now `LsmTreeLoader.loadSSTableFiles` throws `InvalidDataException` naming the file and the minimum required `compactLevelLimits` length. To intentionally shrink levels, remove (or move out) the orphaned files first, then restart with the new config.

### `RangeIterator.Current` before `MoveNext` throws

Accessing `IIterator.Current` before the first `MoveNext()` call (or after `MoveNext()` returns `false`) throws `InvalidOperationException`. This matches standard .NET iterator conventions. Use the `RangeScan` API (returns `seq`) to avoid manual `MoveNext` management.

### Materialized array memory for large ranges

`RangeIterator` materializes all entries within `[fromKey, toKey]` into in-memory arrays during construction (`NewIterator`). For very large ranges (e.g., full database scan on a dataset with millions of keys), this consumes memory proportional to the number of entries in range. Prefer bounded range scans. If full-database iteration is needed, consider batching via multiple smaller range scans.
