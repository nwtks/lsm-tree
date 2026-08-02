# Architecture

## WAL Format

One line per operation; keys and values are **base64-encoded** to avoid delimiter issues:

```
BEGIN <seq>
PUT <seq> <key_b64> <val_b64>
DEL <seq> <key_b64>
COMMIT <seq>
ABORT <seq>
```

- **Committed transactions** are fully recovered.
- **Uncommitted transactions** (`BEGIN` without matching `COMMIT`) are discarded.
- **Orphaned `PUT`/`DEL`** (without a preceding `BEGIN`) are recovered as committed — recovery includes entries whose seq is in a `COMMIT` line or was never seen in a `BEGIN` line. The latter case covers `PutSingle`/`DeleteSingle` (fast-path single-key writes) which intentionally omit `BEGIN`/`COMMIT` markers. This is safe under the engine's last-writer-wins semantics.
- **Explicit rollbacks** (`ABORT <seq>`): Recognized in the parser but ignored during recovery — no entries are emitted for aborted sequences. This is consistent with the `BEGIN`-without-`COMMIT` discard logic. The `ABORT` record serves as an explicit audit trail.
- `WALRecovery.recover` handles malformed lines gracefully by returning `None` for unrecognized entries.
- **Streaming recovery**: Uses `File.ReadLines` (lazy enumeration, not `File.ReadAllLines`) in two passes — first to find committed/begun sequences, second to emit entries. Memory use is O(unique sequence count), not O(file size).

---

## SSTable Binary Format

The on-disk layout of an `.sst` file is:

```
[entry bytes...] [index: int32 count + IndexEntry[count]] [bloom filter: int32 byteCount + bytes] [index_offset: int64] [bloom_offset: int64] [max_seq: int64] [magic: int64]
```

Each **entry** in the data region is encoded as:

```
seq: int64 | key: int32 length + UTF-8 bytes | value: bool isTombstone + (if false: int32 length + UTF-8 bytes)
```

- `isTombstone = true` → deletion marker: no further bytes follow.
- `isTombstone = false` → live value: `int32` byte length + UTF-8 bytes follow.

Each **IndexEntry** in the index region is encoded as:

```
seq: int64 | offset: int64 | keyByteLen: int32 | keyBytes: byte[keyByteLen]
```

The `offset` field points to the start of the corresponding entry in the data region (i.e., the `seq` field of that entry). The key bytes are stored inline in the index, so the data region does **not** need to be accessed during SSTable open.

- **Footer**: always 32 bytes (four `int64` fields).
- **Magic**: `0x4C534D54` (`"LSMT"` in ASCII). Wrong magic raises `InvalidDataException`.
- **Index**: packed `int32` count followed by `count` inline `IndexEntry` records (seq + offset + key). All fields needed to build the in-memory `IndexEntry[]` are present here — the data region is read lazily only for value payloads on `Get`/`GetRange`.
- **Bloom filter**: packed `int32` byte count + raw bytes.
- **`max_seq`**: highest sequence number among all entries — enables O(1) startup without scanning.

### Open-time loading

`SSTable.load` performs three sequential reads:

1. **Footer** (32 B) — read from the end of the file to get `indexOffset`, `bloomOffset`, `maxSeq`, `magic`.
2. **Bloom filter** — one `Seek` + `ReadInt32` + `ReadBytes(byteCount)`.
3. **Index region** — one `Seek` + single `ReadExactly` of the entire region (`bloomOffset - indexOffset` bytes), parsed in memory with `BinaryPrimitives.ReadInt*LittleEndian`. No access to the data region is required.

The data region is only touched on demand: `Get` does one `Seek`+`Read` per hit (after in-memory binary search on the index), `GetRange` reads entries sequentially, and `GetAll` (used by compaction) reads the entire data region sequentially from `index.[0].Offset`.

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
| `activeSnapshots` (`Set<int64>`) | `lock activeSnapshotsLock` |
| `globalSeq` | `Interlocked.Increment` / `Interlocked.Read` / `Interlocked.CompareExchange` |
| SkipList nodes | Lock-free CAS (`Interlocked.CompareExchange`) |
| WAL writes (`StreamWriter`/`FileStream`) | `lock walLock` |
| `FlushCoordinator.completedEvent` | `lock flushLock` + disposed flag (`IDisposable`) |

**Lock ordering rules:**
1. You may hold `ssTablesLock` while acquiring `mainLock` (write), but **never the reverse** — this prevents deadlocks.
2. `CompactionCoordinator` auto-properties (`IsCompacting`, `Error`) are always read/written under `ssTablesLock`.
3. At most one compaction runs at a time. Both `CompactionCoordinator` and `FlushCoordinator` use a `ManualResetEvent` for completion signaling and implement `IDisposable` with a disposed-flag guard; they are disposed in `LsmTree.Dispose()` after waiting for in-flight operations.
4. The WAL instance is protected by its own `walLock` object; WAL operations are serialized.
5. Per‑SSTable `rwLock` is independent — do not acquire `mainLock` or `ssTablesLock` while holding a SSTable read/write lock (to avoid unexpected contention).
6. `activeSnapshotsLock` is independent — hold only while reading/writing the active snapshot set. Never acquire `mainLock` or `ssTablesLock` while holding `activeSnapshotsLock`.
7. `globalSeq` uses `Interlocked` operations exclusively — no lock is required to read or advance the sequence number.
8. `flushLock` is independent — `AcquireAndReset` performs `WaitOne()` outside the lock then `Reset()` inside with a `disposed` flag guard (returns `bool`: `false` if disposed); `SignalCompleted` and `Dispose` take the lock. `WaitForCompletion` catches `ObjectDisposedException` to tolerate the dispose race. Never acquire `flushLock` while holding `mainLock` or `ssTablesLock`.

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

### Key Rules

- **Re-query `minActiveSnapshot` at the start of each merge** — never cache it across merge operations.
- **All shared-state mutations must be guarded by `ssTablesLock`** — compaction runs on the thread pool.
- After merge, `Dispose()` old SSTable objects and `File.Delete` the files.

---

## Range Scan

The engine supports range scans via `LsmTree.RangeScan(fromKey, toKey, ?snapshot)` which returns `seq<string * string>`, and `LsmTree.NewIterator(fromKey, toKey, ?snapshot)` which returns `IIterator` (manual `MoveNext`/`Current`/`Dispose`).

### Algorithm: per-source materialize + in-memory merge

Each range scan proceeds in three phases:

1. **Source materialization**: Under appropriate locks, each storage layer (MemTable, immutable MemTable, per-SSTable) produces a `(string * int64 * string option)[]` of entries whose keys fall within `[fromKey, toKey]` (inclusive, `CompareOrdinal` ordering).
   - **MemTable**: `SkipList.EntriesRange` traverses the `Next.[0]` chain from `head`, skipping nodes with key < `fromKey`, collecting until key > `toKey`. No lock required — the SkipList is lock-free.
   - **SSTable**: `collectRangeSources` copies the level-list array under `ssTablesLock`, then calls `SSTable.GetRange` **outside** the lock. `GetRange` performs two binary searches on the in-memory index (`lowerBound`/`upperBound`) to find the offset range, then reads entries sequentially under a per-SSTable read lock. If a table was disposed mid-read (`GetRange` returns `RangeDisposed`) or the snapshot's list references drifted, the whole collection is retried (max 8 attempts), falling back to collecting under `ssTablesLock`. Each key appears at most once per SSTable.

2. **Merge**: `RangeIterator` holds a `SourceCursor` array (one per source). `MoveNext` repeatedly:
   - Picks the minimum key across all cursors via `pickMinKey` (O(K) where K = source count).
   - Drains all entries with that key from all cursors.
   - Among collected entries, selects the one with the highest `seq <= snapshot`.
   - If that entry is a live value (`Some v`), emits `(key, v)`; if tombstone or all seqs exceed snapshot, skips the key.

3. **Dispose**: Releases the registered snapshot so compaction can resume pruning.

### Lock strategy

| Phase | Lock held | Duration |
|---|---|---|
| Materialize MemTable sources | `mainLock` (ReadLock) | Reference capture + SkipList traversal |
| Materialize SSTable sources | `ssTablesLock` (snapshot copy only, then released) + per-SSTable `rwLock` (ReadLock) | Binary search + sequential iteration **outside** `ssTablesLock`; retry whole collection on disposal/drift (max 8), then locked fallback |
| Merge (`MoveNext`) | **None** | All data is in materialized arrays |
| Iterator construction | `snapshotManager.RegisterSnapshot` | Instant |
| Iterator dispose | `snapshotManager.ReleaseSnapshot` | Instant |

### Complexity

| Operation | Cost |
|---|---|
| `NewIterator` construction | O(K) + O(snapshot register) |
| `SSTable.GetRange` | O(log N + R_sst) per file |
| `SkipList.EntriesRange` | O(log N + R_mem) |
| `MoveNext` (amortized) | O(K + M) |
| Full range scan | O(Σ log N_i + R_total × (K + M)) |

K = total source count (MemTable + immutable + all SSTables), N = entries per source,
R = entries within range per source, M = versions per key (usually 1-2).

### Known trade-offs

See [trade-off.md](trade-off.md) for the design rationale.

---

## MVCC & Snapshot Isolation

- Each write operation is assigned a globally incrementing sequence number (`globalSeq`).
- `LsmTree.Snapshot()` captures the current sequence number; subsequent reads with that snapshot see a consistent view.
- Compaction's `pruneVersions` preserves all entries with `seq >= minActiveSnapshot`, ensuring no visible version is removed.
- Transactions registered with the snapshot manager prevent compaction from pruning versions they might read.

---

## Type Convention: `SearchResult` (struct DU)

The internal lookup chain uses `SearchResult` — a `[<Struct>]` discriminated union that distinguishes three cases without heap allocation:

| Value | Meaning |
|---|---|
| `Found "v"` | Live value found |
| `Tombstone` | Key was deleted (stops further search) |
| `NotFound` | Key not found at this storage level |

- `SkipList.Find` returns `SearchResult`. If the target node is not found (`isNull`), it returns `NotFound`. Otherwise it inspects `current.Value` (`string option`): `Some v → Found v`, `None → Tombstone`.
- `MemTable.Get` passes through the `SearchResult` from `data.Find`.
- `SSTable.Get` returns `SearchResult`: `SSTable.readItem br` returns `string option`; `None → Tombstone`, `Some v → Found v`. If the bloom filter rejects or binary search misses, it returns `NotFound`.
- `LsmTreeSearch.searchInTable` (internal helper) recurses on `NotFound` and short-circuits on `Found`/`Tombstone` within a single level's SSTable list.
- `LsmTreeSearch.searchLevel` iterates across levels: on `NotFound` at level N it proceeds to level N+1; on `Found`/`Tombstone` it short-circuits immediately.
- `LsmTreeSearch.findValue` matches on the three cases and converts to `string option` (`Found v → Some v`, `Tombstone → None`, `NotFound → None`) — this is the only public boundary.

### Benefits over nested `string option option`

- **Zero heap allocation**: `SearchResult` is a `[<Struct>]` — `Found "v"`/`Tombstone`/`NotFound` are all value types. The previous `Some(Some "v")` incurred 2 heap objects per `Get`.
- **Static safety**: The three cases are exhaustive; the compiler warns on missing patterns. `string option option` relied on runtime convention.
