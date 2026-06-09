## AGENTS.md Editing Rules

When editing this file, strictly follow these two rules:

1. **Don't write what's in the codebase** — information that can be obtained by reading source code or project files (function names, class names, file structure, parameter types, etc.) must not be written in AGENTS.md.
2. **Don't duplicate README.md** — content already described in README.md (format specs, architecture diagrams, constraints, etc.) should only be referenced by a link (`See [README.md](...)`).

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

## Recurring Gotchas

### MemTable flush races with Put/Delete

`MemTable.Put`/`Delete` update `sizeBytes` **before** inserting into the SkipList. If `flushMemTable` checks `sizeBytes` between the increment and the SkipList insert, it may flush before all data is visible. This is benign (data isn't lost — it's in the WAL and will be recovered) but can produce an empty SSTable. Be aware when asserting post-flush state.

### WAL final-flush errors are silently swallowed

During `Dispose`, all I/O errors from `writer.Flush()`, `stream.Flush(true)`, `writer.Dispose()`, and `stream.Dispose()` are caught and only printed to stderr. If the last WAL entry isn't flushed, the engine still recovers correctly on restart (WAL recovery tolerates truncation), but monitoring tools won't see the error.

### Transaction local Get is O(n)

Inside `LsmTransaction.Get`, pending writes are stored in an `ops` list scanned linearly with `Seq.tryFind`. Avoid large transactions (thousands of keys) if you need fast point lookups within the transaction. For bulk loads, complete the `Put` calls first, then query.

### Temp file cleanup on failed SSTable writes

`SSTableWriter.writeCore` writes to a `.tmp` file first, then renames to `.sst`. If the process crashes mid-write, stale `.tmp` files may remain in the data directory. The engine ignores non-`.sst` files on startup, but cleanup is the caller's responsibility. Tests should never assert `Directory.GetFiles` contains only `.sst` files without accounting for potential `.tmp` leftovers.

### `do...use` scoping avoids double-dispose

When testing restart (create engine → close → reopen), use `do...use tree = new LsmTree(...)` to scope the first engine's lifetime explicitly, then `use tree2 = new LsmTree(...)` for the second. Calling `.Close()` before a `use` block ends can trigger a double-dispose on `ReaderWriterLockSlim`, which throws `SynchronizationLockException`.

### Benchmark syncOnCommit toggle

The benchmark uses `[<Params(false, true)>] member val Sync` to toggle `syncOnCommit`. When adding new benchmarks, always expose this parameter to capture the durability-vs-throughput tradeoff. Use `[<IterationSetup>]` to create a fresh engine for each run — never reuse a dirty engine across iterations.

### Full-level cascade in tests

When testing compaction cascading across multiple levels, set `memTableSizeLimit` large enough (e.g., `1024 * 1024`) to prevent auto-flushes during data loading. Create L0 files via manual `Flush()` calls followed by `WaitForCompaction()` to let each cascade round complete before the next.

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
