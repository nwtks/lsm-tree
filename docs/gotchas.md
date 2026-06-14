# Recurring Gotchas

### MemTable flush races with Put/Delete

See [trade-off.md](trade-off.md) (`MemTable flush races with Put/Delete`). No data loss occurs, but an empty SSTable may be produced. Be aware when asserting post-flush SSTable file counts in tests.

### `SSTable.Get` outer `Some` prevents tombstone masking

`SSTable.Get` wraps `readItem br` with `Some(...)` to return `(string option) option`. This ensures `tryPick` in `searchInTables` can distinguish "tombstone found" (`Some None`) from "key not found" (`None`). If the outer `Some` is missing, a tombstone returns `None` (of `string option`), `tryPick` treats it as "not found", and the search falls through to upper-level stale values — the key appears resurrected after deletion. Always verify the `Some(...)` wrapper exists in `SSTable.Get`.

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

### Benchmark syncOnCommit toggle

The benchmark uses `[<Params(false, true)>] member val Sync` to toggle `syncOnCommit`. When adding new benchmarks, always expose this parameter to capture the durability-vs-throughput tradeoff. Use `[<IterationSetup>]` to create a fresh engine for each run — never reuse a dirty engine across iterations.

### CompactionCoordinator race on Dispose

`LsmTree.Dispose()` cancels the compaction (`compaction.Cancel()`) and waits for it (`waitForCompaction()`), then disposes the `CompactionCoordinator`. However, a fire-and-forget `asyncFlushToSSTable` may call `triggerCompaction` **after** `Dispose()` has already disposed the coordinator's `ManualResetEvent`. The new compaction's `finally` block then calls `SetCompleted()` on the disposed event → `ObjectDisposedException`.

**Fix** (two-fold, see `LsmTreeFlush.fs`):
1. `triggerCompaction` checks `compaction.Token.IsCancellationRequested` — prevents starting new compactions after `Cancel()` during dispose.
2. `CompactionCoordinator.SetCompleted()` has a `disposed` flag guard.
3. `Token` is captured with `member val` (not `member _`) so the `CancellationToken` object survives CTS disposal.

If you ever change the `CompactionCoordinator` or add a new background async operation that calls back into it, ensure the disposed-guard pattern is preserved.
