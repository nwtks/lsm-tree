# AGENTS.md

This file provides guidance for AI agents working in this repository.

## AGENTS.md Editing Rules

- **Don't write what's in the codebase** — information that can be obtained by reading source code or project files must not be written in AGENTS.md.
- **Don't duplicate README.md** — content already described in README.md should only be referenced by a link (`See [README.md](...)`).

### Documentation Location Rules

| Topic | Destination |
|-------|-------------|
| Architecture and design discussions | `docs/architecture.md` |
| Design trade-offs | `docs/trade-off.md` |
| Common mistakes / gotchas | `docs/gotchas.md` |

- **When a design decision, trade-off, bug fix, or known issue occurs, update `docs/trade-off.md` or `docs/gotchas.md` immediately (in the same session) — do not defer.**
- When a new trade-off or gotcha arises, first consider appending to the relevant `docs/` file. Only add to AGENTS.md if it's an "implicit rule not obvious from the codebase."
- Only keep project-specific implicit rules in AGENTS.md. The topics above belong in their corresponding `docs/*.md` files.

---

## Documentation

| Document | Description |
|----------|-------------|
| [Architecture](docs/architecture.md) | Internal design |
| [Design Trade-offs](docs/trade-off.md) | Rationale for key decisions |
| [Recurring Gotchas](docs/gotchas.md) | Common pitfalls and non-obvious behaviors |

---

## Cross-Platform Compatibility

All code — including test code — must work on **both Windows and Linux**. Avoid:

- Hard-coded path separators; use `System.IO.Path.Combine`.
- Platform-specific APIs without fallback (e.g., `stream.Flush(true)` / `FileStream.Flush(bool)` is supported on both).
- Assumptions about case-sensitive file paths (tests use `withTestDir` with unique lowercase names).
- Process-level locks on files that outlive the test scope (`FileShare.Read` on SSTables, etc.).

---

## Coding Conventions

- Prefer functional programming idioms over imperative ones throughout the codebase — including test code.
- **Favor expressions over statements** — Use `match` expressions, `if`/`then`/`else`, and pattern matching instead of imperative control flow.
- **Leverage discriminated unions** — Model domain concepts (messages, roles, configuration phases, timer actions, pending reads) with DUs for exhaustiveness checking.
- **Use `[<TailCall>]` on recursive functions** that loop (e.g., `agentLoop`, `findFirstIdx`) to prevent stack overflows.
- Do not introduce new external NuGet packages without checking existing dependencies in the `.fsproj` files first.
- **Cyclomatic complexity** — Every function/method must keep its keyword-calculated complexity ≤ 15 (hard limit). Keep it ≤ 10 where practical. After `dotnet test`, the `scripts/check-complexity.fsx` script reads `coverage.cobertura.xml` and reports both a keyword-based estimate (calculated) and the Coverlet reference value — the error/warning thresholds apply to the **calculated** column. Configure thresholds in `Directory.Build.props`. If the check fails, split the function into smaller helpers or simplify branching.

---

## Testing Conventions

- After any code change, run `dotnet test` and confirm **all tests pass**.
- The `dotnet test` output includes a **Cyclomatic Complexity Report** (from coverage data via `scripts/check-complexity.fsx`). Address any warnings (above 10) and fix any errors (above 15).
- Maintain high unit test coverage (target: ≥ 90% line coverage). If line coverage falls below 90%, add test code to restore it above the threshold before merging.
- **Test ordering rules**:
  1. Within each test file, `[<Fact>]` functions must appear in the same order as the corresponding functions/methods/constructors in the source file under test.
  2. When multiple test cases target the same source function, order them by **test priority**: normal (happy path) → error cases → fault/failure scenarios.
- **Prefer data-driven tests** (`[<Theory>]` + `[<InlineData>]`) when multiple test cases share the same test logic but differ only in inputs or expected outputs. This reduces code duplication and makes it easy to add new cases.
- **Use a unique suffix** per test — tests may run in parallel.
- Each test uses `withTestDir "<unique_name>" (fun testDir -> ...)` to get an isolated temp directory (creates before use, cleans up after in `try`/`finally`).
- Use `assertEqual expected actual msg` (wraps `Assert.True`) for readable failure output.
- To simulate IO errors deterministically (e.g., for error propagation tests), use reflection to close private `FileStream` handles. Never use file truncation (`SetLength`), as .NET's `FileStream` internal buffer can mask the corruption.

---

## Adding a New Feature

1. Identify the owning layer: WAL, SkipList, MemTable, BloomFilter, SSTable, or LsmTree coordinator.
2. Respect the `LsmTree.fsproj` compilation order (insert new files after their dependencies).
3. Add `[<Fact>]` tests in the appropriate test file (`BloomFilterTests.fs`, `SkipListTests.fs`, etc.) with a unique `withTestDir` name.
4. Run `dotnet test` — all tests must pass.
5. If the feature changes the WAL or SSTable format, update both README.md and the recovery path, and add regression tests for backward compatibility.
