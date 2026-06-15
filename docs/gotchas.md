# Recurring Gotchas

### MemTable flush races with Put/Delete

See [trade-off.md](trade-off.md) (`MemTable flush races with Put/Delete`). No data loss occurs, but an empty SSTable may be produced. Be aware when asserting post-flush SSTable file counts in tests.

### `SSTable.Get` returns `SearchResult` (three cases, not two)

`SSTable.Get` now returns the `SearchResult` struct DU: `Found v` / `Tombstone` / `NotFound`. The old `(string option) option` encoding (`Some None` = tombstone) is eliminated. When changing `SSTable.Get`'s return type, ensure all callers in the search chain (`LsmTreeSearch.searchInTable`, `searchLevel`, `findValue`) are updated together — a single missed match arm causes a compiler error (exhaustiveness check).

The `searchInTable` helper recursively walks a `SSTable list`: on `NotFound` it tries the next table; on `Found` or `Tombstone` it short-circuits immediately. This is equivalent to the old `tryPick` behavior but expressed explicitly with a `SearchResult` return.

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

### CompactionCoordinator race on Dispose

`LsmTree.Dispose()` cancels the compaction (`compaction.Cancel()`) and waits for it (`waitForCompaction()`), then disposes the `CompactionCoordinator`. However, a fire-and-forget `asyncFlushToSSTable` may call `triggerCompaction` **after** `Dispose()` has already disposed the coordinator's `ManualResetEvent`. The new compaction's `finally` block then calls `SetCompleted()` on the disposed event → `ObjectDisposedException`.

**Fix** (two-fold, see `LsmTreeFlush.fs`):
1. `triggerCompaction` checks `compaction.Token.IsCancellationRequested` — prevents starting new compactions after `Cancel()` during dispose.
2. `CompactionCoordinator.SetCompleted()` has a `disposed` flag guard.
3. `Token` is captured with `member val` (not `member _`) so the `CancellationToken` object survives CTS disposal.

If you ever change the `CompactionCoordinator` or add a new background async operation that calls back into it, ensure the disposed-guard pattern is preserved.
