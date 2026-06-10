## AGENTS.md Editing Rules

**Don't write what's in the codebase** — information that can be obtained by reading source code or project files must not be written in AGENTS.md.
**Don't duplicate README.md** — content already described in README.md should only be referenced by a link (`See [README.md](...)`).

### Documentation Location Rules

| Topic | Destination |
|---|---|
| Architecture and design discussions | `docs/architecture.md` |
| Design trade-offs | `docs/trade-off.md` |
| Common mistakes / gotchas | `docs/gotchas.md` |

- Only keep project-specific implicit rules in AGENTS.md. The topics above belong in their corresponding `docs/*.md` files.
- When a new trade-off or gotcha arises, first consider appending to the relevant `docs/` file. Only add to AGENTS.md if it's an "implicit rule not obvious from the codebase."

---

## Build & Test

```bash
dotnet build
dotnet test
dotnet run -c Release --project Benchmark
```

After any code change, run `dotnet test` and confirm **all tests pass**.
Maintain high unit test coverage (target: ≥ 80% line coverage).
If line coverage falls below 80%, add test code to restore it above the threshold before merging.

---

## Architecture

See [docs/architecture.md](docs/architecture.md).

---

## Design Trade-offs

See [docs/trade-off.md](docs/trade-off.md).

---

## Recurring Gotchas

See [docs/gotchas.md](docs/gotchas.md).

---
## Cross-Platform Compatibility

All code — including test code — must work on **both Windows and Linux**. Avoid:

- Hard-coded path separators; use `System.IO.Path.Combine`.
- Platform-specific APIs without fallback (e.g., `File.Flush(true)` is supported on both).
- Assumptions about case-sensitive file paths (tests use `getTestDir` with unique lowercase names).
- Process-level locks on files that outlive the test scope (`FileShare.Read` on SSTables, etc.).

---

## Code Style

Prefer functional programming idioms over imperative ones throughout the codebase — including test code. These rules are preferences, not absolutes — use imperative style when it meaningfully improves readability or performance, but always start with the functional approach.

---

## Testing Conventions

- Tests are top-level `[<Fact>]` functions in XUnit v3, split by component:
  - `BloomFilterTests.fs` — Bloom filter correctness and false positive rate
  - `SkipListTests.fs` — SkipList sorting and concurrency stress
  - `SSTableTests.fs` — level parsing, dispose safety, short file, invalid magic
  - `WALTests.fs` — recovery, atomicity, orphaned ops, malformed entries
  - `TransactionTests.fs` — read own writes, commit visibility, rollback, snapshot isolation
  - `LsmTreeTests.fs` — CRUD, flush, compaction, MVCC, lifecycle, concurrency, error propagation
- Each test calls `getTestDir "<unique_name>"` to get an isolated temp directory (it deletes and recreates the dir).
- **Use a unique suffix** per test — tests may run in parallel.
- **Test ordering matches source ordering**: Within each test file, the `[<Fact>]` functions must appear in the same order as the corresponding functions/methods/constructors in the source file under test. This makes it easy to locate the test for a given piece of code.
- Use `assertEqual expected actual msg` (wraps `Assert.True`) for readable failure output.
- To simulate IO errors deterministically (e.g., for error propagation tests), use reflection to close private `FileStream` handles. Never use file truncation (`SetLength`), as .NET's `FileStream` internal buffer can mask the corruption.

---

## Adding a New Feature

1. Identify the owning layer: WAL, SkipList, MemTable, BloomFilter, SSTable, or LsmTree coordinator.
2. Respect the `LsmTree.fsproj` compilation order (insert new files after their dependencies).
3. Add `[<Fact>]` tests in the appropriate test file (`BloomFilterTests.fs`, `SkipListTests.fs`, etc.) with a unique `getTestDir` name.
4. Run `dotnet test` — all tests must pass.
5. If the feature changes the WAL or SSTable format, update both README.md and the recovery path, and add regression tests for backward compatibility.
