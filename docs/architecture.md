# Architecture

## WAL Format

One line per operation; keys and values are **base64-encoded** to avoid delimiter issues:

```
BEGIN <seq>
PUT <seq> <key_b64> <val_b64>
DEL <seq> <key_b64>
COMMIT <seq>
```

- **Committed transactions** are fully recovered.
- **Uncommitted transactions** (`BEGIN` without matching `COMMIT`) are discarded.
- **Orphaned `PUT`/`DEL`** (without a preceding `BEGIN`) are recovered as committed — their sequence number never appeared in a `BEGIN` line, so they fall through as visible entries.
- `WALRecovery.recover` handles malformed lines gracefully by returning `None` for unrecognized entries.

---

## SSTable Binary Format

The on-disk layout of an `.sst` file is:

```
[entry bytes...] [index: int32 count + int64[] offsets] [bloom filter: int32 byteCount + bytes] [index_offset: int64] [bloom_offset: int64] [max_seq: int64] [magic: int64]
```

Each **entry** is encoded as:

```
seq: int64 | key: int32 length + UTF-8 bytes | value: bool isTombstone + (if false: int32 length + UTF-8 bytes)
```

- `isTombstone = true` → deletion marker: no further bytes follow.
- `isTombstone = false` → live value: `int32` byte length + UTF-8 bytes follow.

- **Footer**: always 32 bytes (four `int64` fields).
- **Magic**: `0x4C534D54` (`"LSMT"` in ASCII). Wrong magic raises `InvalidDataException`.
- **Index**: packed `int32` count + `int64[]` offsets pointing to each entry.
- **Bloom filter**: packed `int32` byte count + raw bytes.
- **`max_seq`**: highest sequence number among all entries — enables O(1) startup without scanning.

**File naming convention:**

```
L{level}_{timestamp_ms}_{guid}.sst
```

| Source | Level |
|--------|-------|
| MemTable flush | L0 |
| Compaction of Ln → L(n+1) | L(n+1) |
| Legacy files (no `L` prefix) | L0 |

During SSTable writing, data is first written to a `.tmp` file and then atomically renamed to `.sst` (see `SSTableWriter.writeCore`). Stale `.tmp` files from a crash are automatically deleted on startup (`loadSSTables`).

---

## Concurrency Model & Lock Ordering

| Resource | Guard |
|---|---|
| `memTable` / `immutableMemTable` | `ReaderWriterLockSlim` (`mainLock`) |
| `ssTables` array | `lock ssTablesLock` |
| Per‑SSTable seek+read / GetAll / Dispose | `ReaderWriterLockSlim` (`rwLock` per SSTable) |
| `activeSnapshots` set | `lock activeSnapshotsLock` |
| `globalSeq` | `Interlocked.Increment` / `Interlocked.Read` / `Interlocked.CompareExchange` |
| SkipList nodes | Lock-free CAS (`Interlocked.CompareExchange`) |

**Lock ordering rules:**
1. You may hold `ssTablesLock` while acquiring `mainLock` (write), but **never the reverse** — this prevents deadlocks.
2. `CompactionCoordinator` auto-properties (`IsCompacting`, `Error`) are always read/written under `ssTablesLock`.
3. At most one compaction runs at a time; coordination uses `ManualResetEvent` (`CompactionCoordinator.AwaitCompletion()` returns `Async<unit>`).
4. The WAL instance is protected by its own `walLock` object; WAL operations are serialized.
5. Per‑SSTable `rwLock` is independent — do not acquire `mainLock` or `ssTablesLock` while holding a SSTable read/write lock (to avoid unexpected contention).

---

## Compaction

### Algorithm

1. **Trigger**: A MemTable flush calls `triggerCompaction`, which starts a background `async { } |> Async.Start` computation if no compaction is currently running.
2. **Level selection**: Starting from L0, if `ssTables[level].Length > compactLevelLimits[level]`, **all** files at that level are selected for compaction.
3. **Merge**: A k-way streaming merge (`mergeSortedEntries`) reads all entries from selected SSTables, deduplicates by key (highest sequence number wins), and applies snapshot pruning.
4. **Output**: A single new SSTable is written to the next level via `SSTableWriter.writeStream`.
5. **Cascade**: Compaction recursively proceeds to the next level if it now exceeds its limit (see `compact` in `LsmTreeFlush.fs`).
6. **Cleanup**: Old SSTable objects are disposed, files are deleted from disk, and references are removed from the in-memory list.

> **Why all files?** In this implementation, all levels (including Ln for n>0) may contain overlapping key ranges. Partial compaction would leave old versions shadowing newer ones in higher levels.

### Rules

- Compaction runs as a background F# `async { } |> Async.Start` computation; all shared-state mutations must be guarded by `ssTablesLock`.
- `CompactionCoordinator` uses auto-properties (`IsCompacting`, `Error`) — always read/write under `ssTablesLock`.
- **Re-query `minActiveSnapshot` at the start of each merge** — never cache it across merge operations.
- **Compaction at any level selects ALL files in that level** — L0 files overlap in key range; files in higher levels can also overlap due to L0→L1 merges covering the entire key range, so partial compaction would shadow newer versions.
- After merge, `Dispose()` old SSTable objects and `File.Delete` the files.
- Compaction respects `minActiveSnapshot` — versions still visible to active snapshots are never pruned.
- `mergeSortedEntries` uses a k-way streaming merge; helper functions `findMinKey`, `collectVersions`, and `pruneVersions` keep the main loop readable.

---

## MVCC & Snapshot Isolation

- Each write operation is assigned a globally incrementing sequence number (`globalSeq`).
- `LsmTree.Snapshot()` captures the current sequence number; subsequent reads with that snapshot see a consistent view.
- Compaction's `pruneVersions` preserves all entries with `seq >= minActiveSnapshot`, ensuring no visible version is removed.
- Transactions registered with the snapshot manager prevent compaction from pruning versions they might read.

---

## Type Convention (all Get methods)

The entire lookup chain uses `(string option) option` to distinguish three cases:

| Value | Meaning |
|---|---|
| `Some(Some "v")` | Live value found |
| `Some None` | Tombstone found (key was deleted — stops further search) |
| `None` | Key not found at this storage level |

- `SkipList.Find` returns `(string option) option` — inherent because `SkipListNode.Value` is `string option` and `Find` returns `Some current.Value`.
- `MemTable.Get` passes through the `(string option) option` from `SkipList.Find`.
- `SSTable.Get` wraps `readItem br` (which returns `string option`) with an outer `Some(...)`, producing `(string option) option`.
- `searchInTables`: `List.tryPick` on `(string option) option` returns `(string option) option` — a tombstone (`Some None`) stops `tryPick` because the match is `Some _`, correctly preventing fall-through to upper-level stale values.
- `searchLevel`: passes through `(string option) option` unchanged.
- `findValue` destructures MemTable/immutable results via pattern matching (`Some v → v`) and uses `Option.flatten` on the SSTable result from `searchLevel`, converting the entire chain to `string option`.
