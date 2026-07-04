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

`RangeIterator` registers a snapshot at construction time and releases it at `Dispose()`. If you forget to call `Dispose()` (or don't use `use` in F#), the snapshot remains active in `SnapshotManager`, preventing compaction from pruning stale versions. This can cause unbounded disk growth. Always `use` iterators or call `Dispose()` explicitly.

### `RangeIterator.Current` before `MoveNext` throws

Accessing `IIterator.Current` before the first `MoveNext()` call (or after `MoveNext()` returns `false`) throws `InvalidOperationException`. This matches standard .NET iterator conventions. Use the `RangeScan` API (returns `seq`) to avoid manual `MoveNext` management.

### Materialized array memory for large ranges

`RangeIterator` materializes all entries within `[fromKey, toKey]` into in-memory arrays during construction (`NewIterator`). For very large ranges (e.g., full database scan on a dataset with millions of keys), this consumes memory proportional to the number of entries in range. Prefer bounded range scans. If full-database iteration is needed, consider batching via multiple smaller range scans.
