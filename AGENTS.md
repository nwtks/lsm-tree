# AGENTS.md — AI Agent Guide for F# LSM-Tree

## Build & Test Commands

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
- SSTable footer is three `int64` fields (24 bytes total); magic is `0x534D434CL`.

---

## Compaction Rules

- Compaction runs on a background `Task`; all shared-state mutations must be guarded by `ssTablesLock`.
- `CompactionCoordinator` uses auto-properties (`IsCompacting`, `Error`) — always read/write under `ssTablesLock`.
- **Re-query `minActiveSnapshot` at the start of each merge** — never cache it across merge operations.
- After merge, `Dispose()` old SSTable objects and `File.Delete` the files.
- Compaction respects `minActiveSnapshot` — versions still visible to active snapshots are never pruned.
- `mergeSortedEntries` uses a k-way streaming merge; helper functions `findMinKey`, `collectVersions`, and `pruneVersions` keep the main loop readable.

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

---

## Source File Structure

| File | Responsibility |
|---|---|
| `BloomFilter.fs` | `BloomFilter` class + `BloomFilter` module (FNV-1a 64-bit double-hashing, 10 bits/item, 7 hash functions) |
| `SkipList.fs` | `SkipListNode` + `SkipList` (lock-free CAS, MAX_LEVEL=16, P=0.5) |
| `MemTable.fs` | `MemTable` (wraps SkipList, UTF-8 byte-estimated `SizeBytes` via `Interlocked.Add`) |
| `WAL.fs` | `WALRecovery` module (`ReadAllLines`, fault-tolerant `recover`) + `WAL` class (append-only writer) |
| `SSTable.fs` | `SSTable` module (binary format read/load) + `SSTable` class (Get/GetAll over open `FileStream`) + `SSTableWriter` module (`write` for exact-count flush, `writeStream` for estimated-count merge) |
| `LsmTreeSnapshot.fs` | `LsmTreeSnapshot` class (`globalSeq` via `Interlocked`, `activeSnapshots` set under lock) |
| `LockExtensions.fs` | `withReadLock` / `withWriteLock` helpers for `ReaderWriterLockSlim` |
| `LsmTransaction.fs` | `ITransaction`, `ILsmTree` interfaces + `LsmTransaction` class |
| `LsmTreeSearch.fs` | `searchInTables`, `searchLevel`, `findValue` (read-path orchestration) |
| `LsmTreeFlush.fs` | `CompactionCoordinator` class, `swapMemTableAndWal`, `flushToSSTable`, `mergeSortedEntries` (k-way streaming merge with `findMinKey`/`collectVersions`/`pruneVersions` helpers), `compact`, `triggerCompaction`, `waitForCompaction` |
| `LsmTree.fs` | Main `LsmTree` class — constructor, public API, `IDisposable` |

## Adding a New Feature

1. Identify the owning layer: WAL, SkipList, MemTable, BloomFilter, SSTable, or LsmTree coordinator.
2. Respect the `LsmTree.fsproj` compilation order (insert new files after their dependencies).
3. Add `[<Fact>]` tests in the appropriate test file (`BloomFilterTests.fs`, `SkipListTests.fs`, etc.) with a unique `getTestDir` name.
4. Run `dotnet test` — all tests must pass.

---

## Known Constraints

See [README.md](README.md#️⃣-known-limitations) for the full list. Key points for agents:
- Keys and values are `string` only — no binary blobs.
- No public range-query API; `GetAll()` is compaction-internal only.
- WAL recovery treats orphaned `PUT`/`DEL` lines (no matching `BEGIN`) as auto-committed single ops.
