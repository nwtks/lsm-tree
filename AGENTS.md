## AGENTS.md Editing Rules

When editing this file, strictly follow these two rules:

1. **Don't write what's in the codebase** — information that can be obtained by reading source code or project files (function names, class names, file structure, parameter types, etc.) must not be written in AGENTS.md.
2. **Don't duplicate README.md** — content already described in README.md (format specs, architecture diagrams, constraints, etc.) should only be referenced by a link (`See [README.md](...)`).
3. **Record design trade-offs** in the `Design Trade-offs` section whenever a significant architectural decision is made — include the rationale, the alternatives considered, and the pros/cons.

AGENTS.md should only record "implicit rules not obvious from code," "recurring gotchas in tests/implementation," and "background behind design decisions."

---

## Cross-Platform Compatibility

All code — including test code — must work on **both Windows and Linux**. Avoid:

- Hard-coded path separators; use `System.IO.Path.Combine`.
- Platform-specific APIs without fallback (e.g., `File.Flush(true)` is supported on both).
- Assumptions about case-sensitive file paths (tests use `getTestDir` with unique lowercase names).
- Process-level locks on files that outlive the test scope (`FileShare.Read` on SSTables, etc.).

---

##  Code Style

Prefer functional programming idioms over imperative ones throughout the codebase — including test code. These rules are preferences, not absolutes — use imperative style when it meaningfully improves readability or performance, but always start with the functional approach.

---

```bash
dotnet build
dotnet test
dotnet run -c Release --project benchmark
```

After any code change, run `dotnet test` and confirm **all tests pass**.

Maintain high unit test coverage (target: ≥ 80% line coverage).

---

## WAL & SSTable Format

See [README.md](README.md#️⃣-architecture--internals) for the WAL operation format and SSTable binary layout. **Do not change either format** without updating both write and recovery paths, plus adding regression tests.

Key format facts to remember:
- WAL delete verb is **`DEL`** (not `DELETE`).
- SSTable footer is four `int64` fields (32 bytes total); magic is `0x4C534D54L`.

---

## Compaction Rules

- Compaction runs on a background `Task`; all shared-state mutations must be guarded by `ssTablesLock`.
- `CompactionCoordinator` uses auto-properties (`IsCompacting`, `Error`) — always read/write under `ssTablesLock`.
- **Re-query `minActiveSnapshot` at the start of each merge** — never cache it across merge operations.
- **L0 compaction selects ALL files** (L0 files overlap in key range; partial compaction would shadow newer versions).
- **Ln (n>0) compaction also selects ALL files** — in this implementation, files in non-L0 levels can also overlap due to L0→L1 merges covering the entire key range, so partial compaction would shadow newer versions in higher levels.
- After merge, `Dispose()` old SSTable objects and `File.Delete` the files.
- Compaction respects `minActiveSnapshot` — versions still visible to active snapshots are never pruned.
- `mergeSortedEntries` uses a k-way streaming merge; helper functions `findMinKey`, `collectVersions`, and `pruneVersions` keep the main loop readable.

---

## Design Trade-offs

### MemTable flush races with Put/Delete

**Problem**: `MemTable.Put`/`Delete` increment `sizeBytes` **before** inserting into the SkipList. If `flushMemTable` checks `sizeBytes >= memTableLimit` between the increment and the SkipList insert, it may flush before all data is visible, potentially producing an empty SSTable. No data is lost — it's safely in the WAL and will be recovered.

| Mitigation | Drawback |
|---|---|
| **① Move `sizeBytes` update after SkipList insert** | Flush is delayed, allowing MemTable to grow beyond `memTableSizeLimit`. Risk of higher-than-expected memory consumption under burst writes |
| **② Guard `Put`/`Delete` with `mainLock` (read lock)** | Loses the lock-free SkipList performance advantage. Every write path pays `ReaderWriterLockSlim` overhead, reducing multi-threaded throughput |
| **③ Track size atomically inside SkipList** | Tightly couples MemTable and SkipList. Requires byte-length calculation before node construction, reducing generality. The race window still exists inside the CAS loop |
| **④ Keep as-is ✅** | Only consequence is an empty SSTable. Data is fully recoverable from WAL. Zero implementation cost |

**Rationale**: The damage is limited to creating an empty SSTable; data integrity is guaranteed by the WAL. Empty SSTables are removed by the next compaction cycle.

---

### WAL `AutoFlush` flushes to page cache, not to disk

**Problem**: `writer.AutoFlush <- true` calls `StreamWriter.Flush()` on every `WriteLine`, which is equivalent to `FileStream.Flush(false)` — data reaches the OS page cache but not the disk. The actual `fsync` (durable write) only happens inside `Commit` via `stream.Flush(true)`. Therefore, even with `sync=true`, a power loss between BEGIN and COMMIT loses that transaction (within ACID Durability guarantees).

| Mitigation | Drawback |
|---|---|
| **① Remove `AutoFlush`; buffer until `Commit` ✅** | On process crash (SegFault / SIGKILL), data in the StreamWriter's internal buffer is completely lost. With AutoFlush enabled, data at least reaches the OS page cache, so this is a regression for that scenario |
| **② Call `stream.Flush(true)` on every WAL operation** | Every `Put` triggers an `fsync`, limiting throughput to ~100–200 ops/s on HDDs. Flushing at `Begin` or individual `Put` is redundant (uncommitted data is discarded on recovery anyway) |
| **③ Batch writes into a single `Write` to reduce fsync count** | Requires changing the WAL interface. Inconvenient for single-Put use cases. Increased memory allocation for concatenating large batches |
| **④ Double-buffer WAL to hide latency** | Significantly more complex. Fundamentally changes the simple append-only WAL design. Overkill for a project of this scale |
| **⑤ Keep as-is** | AutoFlush provides crash-process protection. Data loss window is limited to one COMMIT and is ACID-compliant. `sync=false` is a user-chosen trade-off |

**Rationale**: AutoFlush was removed to eliminate redundant `StreamWriter.Flush(false)` calls on every `WriteLine` — the actual `fsync` (durable write) only happens inside `Commit`. To compensate, an explicit `writer.Flush()` was added before each `stream.Flush(true)`, ensuring the `StreamWriter` buffer is drained before the `fsync`. On process crash, unflushed data in the `StreamWriter` buffer may be lost, but this window is limited to writes between the last `Commit` and the crash — consistent with ACID durability guarantees. The `sync=false` mode skips `fsync` entirely.

---

### WAL final-flush errors are silently swallowed

**Problem**: Inside `WAL.Dispose()`, all I/O errors from `writer.Flush()`, `stream.Flush(true)`, `writer.Dispose()`, and `stream.Dispose()` are caught by `with _ -> eprintfn` and only printed to stderr. SSTable disposal errors inside `LsmTree.Dispose()` are similarly swallowed. Even if the last WAL entry isn't fully flushed, WAL recovery tolerates truncation so the engine starts normally — but monitoring tools cannot detect the error.

| Mitigation | Drawback |
|---|---|
| **① Log errors via a proper logging framework** | The project has no logging dependency. `eprintfn` is still visible if stderr is monitored |
| **② Propagate errors from `Dispose`** | Violates .NET `Dispose` guidelines (`Dispose` should not throw). Subsequent cleanup in `finally` chains would be skipped |
| **③ Require an explicit `Close()` that can return errors** | Breaking API change. The `use` pattern is no longer usable, increasing the risk of resource leaks |
| **④ Save error to a field in `Close()`, retrievable after `Dispose`** | Complicates state management. Potential race conditions under multi-threaded access. Higher cognitive load for callers |
| **⑤ Keep as-is ✅** | Data is recoverable from WAL, so engine integrity is preserved. `eprintfn` provides minimal visibility during debugging. `Dispose` not throwing ensures subsequent cleanup (e.g., SSTable disposal) runs to completion |

**Rationale**: `Dispose`'s responsibility is resource release, not data durability. If the final flush fails, WAL recovery tolerates truncation and restores correct state on the next startup. `Dispose` not throwing ensures the try/finally chain in `LsmTree.Dispose()` runs to completion.

---

### Transaction local Get is O(n)

**Problem**: `LsmTransaction.Get` scans pending writes (`ops` list) linearly with `Seq.tryFind`. With N uncommitted entries, each `Get` visits nodes from head to tail in the worst case (when the key does not exist or is the oldest entry). Inside `commitTransaction`, `ops` is reversed and passed as a batch, so only local reads within the transaction are affected — the write path is not impacted.

| Mitigation | Drawback |
|---|---|
| **① Replace `ops` list with `Map<string, string option>`** | `Get` becomes O(log N). Duplicate keys are automatically kept at the latest value. However, `Commit` must materialize a list (`Map.toSeq |> Seq.map`), adding allocation overhead for all transactions, even tiny ones. Operation ordering is lost, which would matter if future features depend on insertion order |
| **② Use a mutable `Dictionary`** | `Get` is O(1) average. Deviates from functional idioms. Dictionary resize overhead. Still requires list conversion on commit |
| **③ Lazily build an index on first `Get`** | Keeps `Put`/`Delete` fast (simple cons). Index is stale after subsequent writes; must be rebuilt or incrementally updated, adding complexity |
| **④ Rely on user convention: bulk-Put first, query later** | Zero code change. Works well in practice. Violations cannot be caught statically; new users may hit the performance trap unknowingly |
| **⑤ Forbid `Get` inside transactions entirely** | Eliminates the O(n) problem completely. Breaks read-your-writes semantics. Would require rewriting all transaction tests |
| **⑥ Keep as-is ✅** | Code remains simple and correct. Transactions are expected to be short-lived and small. For bulk workloads, the workaround (Put first, query after commit) costs nothing |

**Rationale**: The `ops` list keeps the transaction implementation trivially simple and correct. Transactions are designed as short-lived, small-scope units — the O(n) scan only becomes noticeable at thousands of uncommitted keys, which is an unlikely usage pattern. If large transactions become a first-class use case, switching to a `Map<string, string option>` (mitigation ①) is the most balanced improvement, trading minor commit-time overhead for logarithmic local reads.

---

### Temp file cleanup on failed SSTable writes

**Problem**: `SSTableWriter.writeCore` writes all data to a `.tmp` file first, then calls `File.Move(.tmp → .sst)` for an atomic rename. If the process crashes between the start of the write and the rename, a stale `.tmp` file remains in the data directory. The `finally` block attempts to delete the `.tmp` file but silently swallows any I/O errors. On startup, the engine only loads `*.sst` files, so stale `.tmp` files are invisible to the engine and accumulate indefinitely.

| Mitigation | Drawback |
|---|---|
| **① Clean up `*.sst.tmp` files on engine startup** | Simple addition to `loadSSTables`. Auto-removes stale files. Risk of racing with another process writing `.tmp` files (not applicable in single-process design). Silent if delete fails |
| **② Log `File.Delete` errors in `finally` via `eprintfn`** | Makes deletion failures visible on stderr. Still does not prevent `.tmp` accumulation; merely makes it observable |
| **③ Use a fixed `.tmp` filename per level** | Old `.tmp` is naturally overwritten by the next write. Loses GUID-based uniqueness, making crash-debugging harder. Race risk if flush and compaction run concurrently |
| **④ Write directly to `.sst` and validate on load** | Eliminates `.tmp` entirely. Risk of loading a partially written SSTable on crash-recovery; the magic-number footer check mitigates this but does not eliminate it |
| **⑤ Keep as-is ✅** | Stale `.tmp` files have zero impact on engine correctness or data integrity. Next write to the same `.tmp` path overwrites the stale content. Test isolation is handled by `getTestDir`. Zero implementation cost |

**Rationale**: Stale `.tmp` files are cosmetic — they never affect engine correctness because the startup path ignores non-`.sst` files, and data integrity is guaranteed by the WAL. The `.tmp` file is a temporary artifact of the atomic-rename pattern, and the cleanup in `finally` covers the common case. If the accumulation of `.tmp` files becomes a practical concern, mitigation ① (startup cleanup) is a trivial, low-risk addition.

---

### Put/Delete bypasses transaction for single-key writes

**Problem**: Every `Put`/`Delete` created a full transaction (`BeginTransaction` → `Put`/`Delete` → `Commit`) with snapshot registration, release, WAL `BEGIN`/`COMMIT` markers, and `ops` list allocation — all for a single-key write that doesn't need atomic multi-key semantics. This added unnecessary allocation and lock churn to the hottest write path.

| Mitigation | Drawback |
|---|---|
| **① Add `PutSingle`/`DeleteSingle` to WAL, `putDirect`/`deleteDirect` to LsmTree ✅** | WAL writes are no longer transactional for single-key ops. On crash recovery, orphaned `PUT`/`DEL` lines (without `BEGIN`/`COMMIT`) are recovered as committed. The engine is last-writer-wins, so this is safe — there are no partial-state concerns |
| **② Keep full transaction but pool/reuse objects** | Reduces allocation pressure but does not eliminate WAL `BEGIN`/`COMMIT` markers or snapshot registration overhead. More complex to implement correctly under concurrency |
| **③ Keep as-is** | Simple and correct, but every single-key `Put`/`Delete` pays 2× WAL lines, 1 snapshot register/release, and 1 `ops` list allocation |

**Rationale**: The engine's last-writer-wins semantics guarantee that orphaned `PUT`/`DEL` lines from a crash are safe to recover as committed. The WAL recovery already handles orphaned lines as a first-class case (see README.md). The direct path eliminates allocation, WAL marker overhead, and snapshot churn for the most common write pattern.

---

### Async MemTable flush

**Problem**: `flushMemTable` synchronously wrote SSTable data to disk on the calling thread. For large MemTables (e.g., 1 MB), the write could block the caller for tens of milliseconds, causing latency spikes in write-heavy workloads.

| Mitigation | Drawback |
|---|---|
| **① Move SSTable write to `Task.Run`; sequentialize via `FlushCoordinator` ✅** | Latency on the calling thread is reduced to the swap + `Task.Run` overhead (~microseconds). However, flushes are sequentialized — at most one in-flight flush at a time. If a flush is still running when the next MemTable fills up, the caller blocks on `flushDoneEvent.WaitOne()` until the previous flush completes |
| **② Allow parallel flushes (one per MemTable swap)** | Higher throughput under burst writes but increases peak memory and disk I/O. Race conditions on SSTable installation order (which flush's data is newest?) require careful sequence-number gating |
| **③ Keep synchronous flush** | Simple and predictable. Caller blocks for the full SSTable write duration — acceptable for low-write-throughput use cases |

**Rationale**: Sequentialized async flushes provide the best latency reduction for the common case (one flush at a time) without the complexity of parallel-flush ordering. The `FlushCoordinator` pattern (`AcquireAndReset` → `SignalCompleted`) is lightweight and composable with the existing `CompactionCoordinator`. If a flush is already in progress when the next swap triggers, the caller back-pressures naturally by waiting on the completion event.

---

### SSTable in-memory index trades memory for fewer disk seeks

**Problem**: `SSTable.binSearch` performed a `fs.Seek` + `br.Read` on every iteration of the binary search (≈log₂N disk seeks per lookup). Each seek requires a syscall and may cause a page fault if the file region is not cached by the OS.

| Mitigation | Drawback |
|---|---|
| **① Load `(key, seq, offset)` into an in-memory `IndexEntry[]` at open time** | Increases per-SSTable memory by 16+ bytes per entry (string key + int64 seq + int64 offset). Startup time increases slightly to read all entries sequentially. `Get` still does one Seek+Read for the value payload on a hit — the index only eliminates seeks during the binary search itself |
| **② Keep a fixed `IndexEntry` LRU cache of recent searches** | Reduces memory but adds cache-management complexity (eviction, invalidation on compaction). Worst-case performance still degrades to disk-seek-per-search on cache miss |
| **③ Use memory-mapped I/O (`MemoryMappedFile`)** | Lets the OS page in data lazily. Avoids explicit seeks but adds complexity in offset management. Cross-platform behavior varies (Windows page cache vs. Linux page cache) |
| **④ Prefetch the entire data region at open time** | Eliminates all subsequent seeks. Very high memory usage proportional to total SSTable data size. Startup time increases significantly. Not viable for large datasets |
| **⑤ Keep as-is ✅** | Simple, zero memory overhead for the index. OS page cache often keeps recently accessed pages in memory, so repeated lookups of the same region are fast |

**Rationale**: The in-memory index (①) was chosen after analyzing the workload pattern — SSTable lookups perform a `bloomFilter.MightContain` check first (fast, in-memory), and only when the bloom filter returns true does a binary search occur. Since bloom filter false positives are rare (≈1% at 10 bits/item), the index lookup path is exercised infrequently. The memory cost is proportional to the number of entries across all SSTables, which is acceptable for this project's scale. The fixed overhead of 16 bytes (two `int64` fields) per entry plus the key string reference is predictable and bounded.

**Critical implementation detail**: `SSTable.Get` must return `(string option) option` (not `string option`) to match the convention used by `MemTable.Get`/`SkipList.Find`. The outer `Some` indicates "entry found at this storage level" while the inner `option` indicates whether the entry is a tombstone (`None`) or a live value (`Some value`). `List.tryPick` in `searchInTables` wraps this in another `option`, producing the expected `(string option) option` for `searchLevel`. Forgetting the outer `Some` causes a type mismatch error at the call site because `tryPick` reduces the nesting by one level.

---

## Recurring Gotchas

### MemTable flush races with Put/Delete

See Design Trade-offs above. No data loss occurs, but an empty SSTable may be produced. Be aware when asserting post-flush SSTable file counts in tests.

### `do...use` scoping avoids double-dispose

When testing restart (create engine → close → reopen), use `do...use tree = new LsmTree(...)` to scope the first engine's lifetime explicitly, then `use tree2 = new LsmTree(...)` for the second. Calling `.Close()` before a `use` block ends can trigger a double-dispose on `ReaderWriterLockSlim`, which throws `SynchronizationLockException`.

### Full-level cascade in tests

When testing compaction cascading across multiple levels, set `memTableSizeLimit` large enough (e.g., `1024 * 1024`) to prevent auto-flushes during data loading. Create L0 files via manual `Flush()` calls followed by `WaitForCompaction()` to let each cascade round complete before the next.

### Benchmark syncOnCommit toggle

The benchmark uses `[<Params(false, true)>] member val Sync` to toggle `syncOnCommit`. When adding new benchmarks, always expose this parameter to capture the durability-vs-throughput tradeoff. Use `[<IterationSetup>]` to create a fresh engine for each run — never reuse a dirty engine across iterations.

---

## Testing Conventions

- Tests are top-level `[<Fact>]` functions in XUnit v3, split by component:
  - `BloomFilterTests.fs` — Bloom filter correctness and false-positive rate
  - `SkipListTests.fs` — SkipList sorting and concurrency stress
  - `SSTableTests.fs` — level parsing, dispose safety, short file, invalid magic
  - `WALTests.fs` — recovery, atomicity, orphaned ops, malformed entries
  - `TransactionTests.fs` — read own writes, commit visibility, rollback, snapshot isolation
  - `LsmTreeTests.fs` — CRUD, flush, compaction, MVCC, lifecycle, concurrency, error propagation
- Each test calls `getTestDir "<unique_name>"` to get an isolated temp directory (it deletes and recreates the dir).
- **Use a unique suffix** per test — tests may run in parallel.
- Use `assertEqual expected actual msg` (wraps `Assert.True`) for readable failure output.
- To simulate IO errors deterministically (e.g., for error propagation tests), use reflection to close private `FileStream` handles. Never use file truncation (`SetLength`), as .NET's `FileStream` internal buffer can mask the corruption.

---

## Adding a New Feature

1. Identify the owning layer: WAL, SkipList, MemTable, BloomFilter, SSTable, or LsmTree coordinator.
2. Respect the `LsmTree.fsproj` compilation order (insert new files after their dependencies).
3. Add `[<Fact>]` tests in the appropriate test file (`BloomFilterTests.fs`, `SkipListTests.fs`, etc.) with a unique `getTestDir` name.
4. Run `dotnet test` — all tests must pass.
5. If the feature changes the WAL or SSTable format, update both README.md and the recovery path, and add regression tests for backward compatibility.
