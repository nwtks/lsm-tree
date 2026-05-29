# AGENTS.md — AI Agent Guide for F# LSM-Tree

## Build & Test Commands

```bash
dotnet build
dotnet test
dotnet test /p:CollectCoverage=true /p:CoverletOutputFormat=cobertura
dotnet run -c Release --project benchmark
```

After any code change, run `dotnet test` and confirm **all tests pass**.

Maintain high unit test coverage (at least line ~80%).

---

## WAL & SSTable Format

See [README.md](README.md#️⃣-architecture--internals) for the WAL operation format and SSTable binary layout. **Do not change either format** without updating both write and recovery paths, plus adding regression tests.

---

## Compaction Rules

- Compaction runs on a background `Task`; all shared-state mutations must be guarded by `ssTablesLock`.
- **Re-query `minActiveSnapshot` at the start of each merge** — never cache it across merge operations.
- After merge, `Dispose()` old SSTable objects and `File.Delete` the files.
- Compaction respects `minActiveSnapshot` — versions still visible to active snapshots are never pruned.

---

## Testing Conventions

- Tests are top-level `[<Fact>]` functions in `LsmTree.Tests/Tests.fs` (XUnit).
- Each test calls `getTestDir "<unique_name>"` to get an isolated temp directory (it deletes and recreates the dir).
- **Use a unique suffix** per test — tests may run in parallel.
- Use `assertEqual expected actual msg` (wraps `Assert.True`) for readable output.

---

## Adding a New Feature

1. Identify the owning layer: WAL, SkipList, MemTable, SSTable, or LsmTree coordinator.
2. Respect the `LsmTree.fsproj` compilation order.
3. Add `[<Fact>]` tests in `Tests.fs` with a unique `getTestDir` name.
4. Run `dotnet test` — all tests must pass.

---

## Known Constraints

See [README.md](README.md#️⃣-known-limitations) for the full list. Key points for agents:
- Keys and values are `string` only — no binary blobs.
- No public range-query API; `GetAll()` is compaction-internal only.
