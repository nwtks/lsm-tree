#!/usr/bin/env dotnet fsi
// check-complexity.fsx
//
// Cyclomatic complexity checker for F# source files.
//
// Reads a coverlet coverage.cobertura.xml, matches IL-level methods to source
// functions, calculates complexity from source code (keyword-based), and
// reports methods exceeding configurable thresholds.
//
// Calculation rules:
//   Base: 1 per function. Increments for each:
//   - if / elif / for / while          (+1)
//   - match!/function `|` case         (+1 per case)
//   - try/with `|` handler case        (+1 per handler)
//   - when (pattern guard)             (+1)
//   - && / || in conditions            (+1 each)
//
// Note: `try` itself is NOT a decision point — only the exception
// handler branches in `with` contribute to complexity, matching
// Coverlet's IL-level behavior.
//
// Usage:
//   dotnet fsi scripts/check-complexity.fsx -- --help
//   dotnet fsi scripts/check-complexity.fsx -- --coverage-file <path> [options]
// Note: `--` separates FSI options from script arguments.

open System.IO
open System.Text.RegularExpressions
open System.Xml.Linq

// ---------------------------------------------------------------------------
// Argument parsing
// ---------------------------------------------------------------------------

type Args =
    { CoverageFile: string
      Threshold: int
      WarnThreshold: int
      Quiet: bool
      Verbose: bool }

    static member Default =
        { CoverageFile = "coverage.cobertura.xml"
          Threshold = 15
          WarnThreshold = 10
          Quiet = false
          Verbose = false }

let args = fsi.CommandLineArgs |> Array.skip 1

let rec parseArgs acc =
    function
    | "--coverage-file" :: v :: rest -> parseArgs { acc with CoverageFile = v } rest
    | "--threshold" :: v :: rest -> parseArgs { acc with Threshold = int v } rest
    | "--warn-threshold" :: v :: rest -> parseArgs { acc with WarnThreshold = int v } rest
    | "--quiet" :: rest -> parseArgs { acc with Quiet = true } rest
    | "--verbose" :: rest -> parseArgs { acc with Verbose = true } rest
    | "--help" :: _ ->
        printfn "Usage: dotnet fsi scripts/check-complexity.fsx -- [options]"
        printfn ""
        printfn "Reads cobertura XML and calculates cyclomatic complexity from source files."
        printfn ""
        printfn "NOTE: Use '--' before options to separate FSI flags from script arguments,"
        printfn "      e.g.: dotnet fsi scripts/check-complexity.fsx -- --help"
        printfn ""
        printfn "Options:"
        printfn "  --coverage-file <path>    cobertura XML file (default: coverage.cobertura.xml)"
        printfn "  --threshold <n>           error threshold for calculated value (default: 15)"
        printfn "  --warn-threshold <n>      warning threshold for calculated value (default: 10)"
        printfn "  --quiet                   only output errors"
        printfn "  --verbose                 log unmatched methods and match statistics"
        printfn "  --help                    show this help"
        exit 0
    | _ :: rest -> parseArgs acc rest
    | [] -> acc

let args' = parseArgs Args.Default (List.ofArray args)

// ===========================================================================
// Source-level cyclomatic complexity calculation
// ===========================================================================
// Each function starts at 1. Increments are added per keyword, per match/with
// case, per conditional boolean operator. Comments and string literals are
// stripped before counting to avoid false positives.
//
// IMPORTANT: Complexity is calculated SOLELY from source code text. Compiler-
// generated logic (e.g., closure dispatch, async state machines, pipeline
// combinators, pattern-match exhaustiveness checks, implicit null checks,
// or any IL that the F# compiler synthesizes without a corresponding source
// expression) is NOT present in the F# source and therefore excluded from the
// calculated value. For a reference measurement that includes compiler-
// generated complexity, see the "Ref" column from Coverlet (IL-level).
// ===========================================================================

/// Keyword patterns that match as whole words (using \b regex boundaries).
/// Each occurrence adds +1 to cyclomatic complexity.
/// Inspired by FSharpLint's AST-based counting:
///   - if/elif = +1 per decision point
///   - for/while = +1 per loop
///   - when = +1 per pattern guard
///
/// Note: `try` is intentionally excluded — `try` itself is not a decision
/// point. Exception handler branches in `with` are counted separately by
/// `countAllBranches`, matching Coverlet's IL-level behavior.
///
/// Note: match/function cases are counted separately via `|` pattern matching.
let wordKeywordPatterns: (string * int) list =
    [ "if", 1; "elif", 1; "for(?!\!)", 1; "while", 1; "when", 1 ]

/// Operators matched as literal substrings.
let literalKeywordPatterns: (string * int) list = []

/// Block context for the unified branch-counting state machine.
type private Ctx =
    | MatchBlock of startIndent: int
    | TryBeforeWith of tryIndent: int
    | TryInWithSection of tryIndent: int * withIndent: int

/// Count match/function cases and try/with handler cases in a single pass,
/// ensuring each `|` branch is counted exactly once — avoiding double-counting
/// when a try/with block is nested inside a match/function block.
/// Returns (matchCaseCount, handlerCaseCount).
let countAllBranches (text: string) : int * int =
    let lines = text.Split('\n')
    let mutable matchCount = 0
    let mutable handlerCount = 0

    // Stack of block contexts for nested blocks.
    let mutable ctxStack: Ctx list = []

    for line in lines do
        let trimmed = line.TrimStart()
        let indent = line.Length - trimmed.Length

        // Track whether this line opened a new block, so the block-popping
        // logic (step 4) does not immediately pop a block on the same line
        // that opened it — fixing premature MatchBlock/TryBeforeWith removal.
        let mutable openedBlock = false

        // ── 1. Detect block starts ──

        // match / function  (match! is also matched by "match" since \b matches between "h" and "!")
        if Regex.IsMatch(trimmed, @"\b(match|function)\b") then
            openedBlock <- true
            ctxStack <- MatchBlock indent :: ctxStack

            // Tuple match bonus: +1 for "match a, b with" or "match! a, b with"
            if Regex.IsMatch(trimmed, @"\bmatch(?:!|\b)") then
                let m = Regex.Match(trimmed, @"\bmatch(?:!|\b)")

                if m.Success then
                    let afterMatch = trimmed.Substring(m.Index + m.Length)
                    let withIdx = afterMatch.LastIndexOf("with")

                    if withIdx >= 0 then
                        let exprPart = afterMatch.Substring(0, withIdx)
                        let mutable depth = 0
                        let mutable foundTupleComma = false

                        for c in exprPart do
                            match c with
                            | '('
                            | '['
                            | '{' -> depth <- depth + 1
                            | ')'
                            | ']'
                            | '}' -> depth <- depth - 1
                            | ',' when depth = 0 -> foundTupleComma <- true
                            | _ -> ()

                        if foundTupleComma then
                            matchCount <- matchCount + 1

        // try keyword
        if Regex.IsMatch(trimmed, @"\btry\b") then
            openedBlock <- true
            ctxStack <- TryBeforeWith indent :: ctxStack

        // ── 2. Detect `with` — transition TryBeforeWith → TryInWithSection ──
        if trimmed.StartsWith("with") then
            // Search stack (top-down) for the innermost TryBeforeWith
            // whose indent is <= this `with`'s indent.
            let rec replaceTries stack =
                match stack with
                | TryBeforeWith ti :: rest when ti <= indent -> Some(TryInWithSection(ti, indent) :: rest)
                | x :: rest ->
                    match replaceTries rest with
                    | Some newRest -> Some(x :: newRest)
                    | None -> None
                | [] -> None

            match replaceTries ctxStack with
            | Some newStack ->
                ctxStack <- newStack
                openedBlock <- true // ← prevents immediate pop of TryInWithSection

                // Inline handler: "with ex -> ..." (no | on same line, has ->)
                let afterWith = trimmed.Substring(4).TrimStart()

                if afterWith <> "" && not (afterWith.StartsWith("|")) && afterWith.Contains("->") then
                    handlerCount <- handlerCount + 1
            | None -> ()

        // ── 3. Count `|` branches (exactly once) ──
        let isPipe =
            trimmed.StartsWith("|")
            && not (trimmed.StartsWith("||"))
            && not (trimmed.StartsWith("|>"))
            && not (trimmed.StartsWith("|]"))

        if isPipe then
            // Is this `|` inside a try/with's `with` section?
            let inWithSection =
                ctxStack
                |> List.exists (function
                    | TryInWithSection(_, wi) -> indent >= wi
                    | _ -> false)

            if inWithSection then
                handlerCount <- handlerCount + 1
            elif
                ctxStack
                |> List.exists (function
                    | MatchBlock _ -> true
                    | _ -> false)
            then
                matchCount <- matchCount + 1

        // ── 4. End of blocks: only for non-block-opening lines ──
        //       Without the `not openedBlock` guard, a `match`/`try` line
        //       would push a block in step 1 and immediately pop it here
        //       because `indent <= si` is true (same indent), causing all
        //       subsequent `|` branches on that block to be missed.
        // Guard: `with` as a keyword (as in `match ... with` split across lines)
        // should NOT pop a MatchBlock — the `with` belongs to the match, not
        // to a new outer block.  try/with's `with` is already handled by step 2
        // (openedBlock = true) so it never reaches step 4.
        let isWithKeyword = Regex.IsMatch(trimmed, @"^with\b")

        if
            not openedBlock
            && not (List.isEmpty ctxStack)
            && trimmed <> ""
            && not (trimmed.StartsWith("//"))
            && not (trimmed.StartsWith("#"))
            && not (trimmed.StartsWith("["))
            && not isPipe
            && not isWithKeyword
        then
            let rec pop stack =
                match stack with
                | [] -> []
                | MatchBlock si :: rest when indent <= si -> pop rest
                | TryBeforeWith ti :: rest when indent <= ti -> pop rest
                | TryInWithSection(ti, _) :: rest when indent <= ti -> pop rest
                | _ -> stack

            ctxStack <- pop ctxStack

    matchCount, handlerCount



/// Count `&&` and `||` on lines containing `if`/`elif`/`while`/`when` and
/// their continuation lines (multiline conditions), matching FSharpLint's
/// `countBooleanOperators` which only counts boolean operators inside
/// conditions, not in value expressions.
let countConditionalBooleanOperators (text: string) : int =
    let lines = text.Split('\n')
    let mutable count = 0
    let mutable inCondition = false

    for line in lines do
        let hasKeyword = Regex.IsMatch(line, @"\b(if|elif|while|when)\b")

        if hasKeyword then
            inCondition <- true

        if inCondition then
            count <- count + Regex.Matches(line, @"(?<!\|)\|\|(?!\|)|(?<!&)&&(?!&)").Count

        // End condition context on `then`/`do`/`->` — these mark the end of the
        // condition expression.  For example:
        //   if a &&
        //      b then
        // Line 1 starts the condition (inCondition = true), line 2 sees
        // `then` and resets inCondition = false.  Single-line conditions
        // like `if a then` handle both on the same line: keyword enters the
        // context, counting happens, then `then` exits it.
        //
        // `->` is needed for `when` guards in pattern matching:
        //   | pattern when cond1 &&
        //                   cond2 -> body
        // Without this, `inCondition` leaks past the guard.
        if
            Regex.IsMatch(line, @"\bthen\b")
            || Regex.IsMatch(line, @"\bdo\b")
            || Regex.IsMatch(line, @"->")
        then
            inCondition <- false

    count

/// Strip string literals, line comments, and block comments from source
/// text to prevent false keyword matches inside them. Handles "...",
/// triple-quoted """...""", // line comments, and (* ... *) block comments
/// (including nested block comments). Preserves newlines for line-tracking.
let stripNonCode (text: string) : string =
    let buf = System.Text.StringBuilder(text.Length)
    let mutable i = 0
    let len = text.Length

    while i < len do
        match text.[i] with
        | '"' when i + 2 < len && text.[i + 1] = '"' && text.[i + 2] = '"' ->
            // Triple-quoted string """..."""
            buf.Append("   ") |> ignore
            i <- i + 3

            while i + 2 < len && not (text.[i] = '"' && text.[i + 1] = '"' && text.[i + 2] = '"') do
                if text.[i] = '\n' then
                    buf.Append('\n') |> ignore
                else
                    buf.Append(' ') |> ignore

                i <- i + 1

            if i + 2 < len then
                buf.Append("   ") |> ignore
                i <- i + 3
        | '"' ->
            // Regular string "..." or interpolated string $"..."
            let isInterpolated = i > 0 && text.[i - 1] = '$'
            buf.Append(' ') |> ignore
            i <- i + 1

            let mutable braceDepth = if isInterpolated then 0 else -1

            while i < len
                  && not (
                      if braceDepth >= 0 then
                          text.[i] = '"' && braceDepth <= 0
                      else
                          text.[i] = '"' && text.[i - 1] <> '\\'
                  ) do
                if braceDepth >= 0 then
                    // Track interpolation braces { ... }
                    if text.[i] = '{' && (i + 1 >= len || text.[i + 1] <> '{') then
                        braceDepth <- braceDepth + 1
                    elif text.[i] = '}' && (i + 1 >= len || text.[i + 1] <> '}') then
                        braceDepth <- braceDepth - 1

                if text.[i] = '\n' then
                    buf.Append('\n') |> ignore
                else
                    buf.Append(' ') |> ignore

                i <- i + 1

            if i < len then
                buf.Append(' ') |> ignore
                i <- i + 1
        | '(' when i + 1 < len && text.[i + 1] = '*' ->
            // Block comment (* ... *), which may be nested
            let mutable depth = 1
            buf.Append("  ") |> ignore
            i <- i + 2

            while i < len && depth > 0 do
                if i + 1 < len && text.[i] = '(' && text.[i + 1] = '*' then
                    // Nested opening
                    depth <- depth + 1
                    buf.Append("  ") |> ignore
                    i <- i + 2
                elif i + 1 < len && text.[i] = '*' && text.[i + 1] = ')' then
                    // Closing
                    depth <- depth - 1

                    if depth > 0 then
                        buf.Append("  ") |> ignore

                    i <- i + 2
                else
                    if text.[i] = '\n' then
                        buf.Append('\n') |> ignore
                    else
                        buf.Append(' ') |> ignore

                    i <- i + 1
        | '/' when i + 1 < len && text.[i + 1] = '/' ->
            // Line comment // ...
            while i < len && text.[i] <> '\n' do
                buf.Append(' ') |> ignore
                i <- i + 1
        | c ->
            buf.Append(c) |> ignore
            i <- i + 1

    buf.ToString()

/// Strip multi-line lambda bodies (`fun ... ->`) from source code, preventing
/// closure complexity from being attributed to the parent function. Operates on
/// code that has already had comments and strings stripped.
///
/// Uses indentation-based detection: after `fun params ->` with an empty body on
/// the same line, all subsequent lines indented further than the `fun` line are
/// replaced with whitespace (preserving newlines). Single-line lambdas
/// (`fun x -> expr`) are not stripped, as their complexity is typically minimal.
///
/// `fun` keywords at or shallower than `baseIndent` are not stripped — this
/// preserves cases where `fun` defines the function itself
/// (e.g. `let f = fun x -> ...`).
let stripLambdaBodies (code: string) (baseIndent: int) : string =
    let lines = code.Split('\n')
    let result = System.Collections.Generic.List<string>()
    // Stack of (indent) for active multi-line lambda bodies
    let mutable skipStack: int list = []

    for lineIdx = 0 to lines.Length - 1 do
        let line = lines.[lineIdx]
        let trimmed = line.TrimStart()
        let indent = line.Length - trimmed.Length

        // Pop entries where current line is at or above the lambda's indent.
        // CRITICAL: Only pop on non-blank lines — blank lines (indent=0) inside
        // a lambda body would otherwise prematurely clear the stack, exposing
        // all content lines inside the block.
        if trimmed <> "" then
            skipStack <- skipStack |> List.filter (fun lambdaIndent -> indent > lambdaIndent)

        if not (List.isEmpty skipStack) then
            // Inside a lambda body — replace content with spaces, preserve newlines
            if trimmed = "" then
                result.Add(line)
            else
                result.Add(System.String(' ', line.Length))
        else
            // Check if this line starts a multi-line lambda: `fun` keyword followed by `->`
            let funMatch = Regex.Match(trimmed, @"\bfun\b")

            if funMatch.Success then
                let afterKw = trimmed.Substring(funMatch.Index + 3)
                let arrowIdx = afterKw.IndexOf("->")

                if arrowIdx >= 0 then
                    let afterArrow = afterKw.Substring(arrowIdx + 2).TrimStart()

                    if afterArrow = "" && indent > baseIndent then
                        // Multi-line lambda at nested indent — strip its body
                        skipStack <- indent :: skipStack

                result.Add(line)
            else
                result.Add(line)

    System.String.Join("\n", result)

/// Patterns that signal the start of a function/method binding.
/// Order matters: more specific patterns (e.g. "member val") must come
/// before their general prefix (e.g. "member") to match first.
let letPatterns =
    [| "and "
       "let rec "
       "let "
       "static member val "
       "override val "
       "member val "
       "static member "
       "override "
       "member " |]

/// Strip self-identifier prefix from member names.
/// e.g., "_.Put" -> "Put", "this.Close" -> "Close", "x.Key" -> "Key"
let private stripSelfIdentifier (name: string) =
    let dotIdx = name.IndexOf('.')
    if dotIdx > 0 then name.Substring(dotIdx + 1) else name

/// Access modifiers that can appear after `let`/`let rec`/`and`.
/// e.g. "let private foo ...", "let internal bar ..."
let private accessModifiers = set [ "private"; "internal"; "public" ]

/// Strip access modifier from the beginning of a trimmed binding tail.
/// e.g. "private handleBasicCommand input =" -> "handleBasicCommand input ="
let private stripAccessModifier (text: string) =
    let firstSpace = text.IndexOf(' ')

    if firstSpace >= 0 then
        let candidate = text.Substring(0, firstSpace)

        if Set.contains candidate accessModifiers then
            text.Substring(firstSpace + 1).TrimStart()
        else
            text
    else
        text

/// Strip F# attributes (e.g. [<TailCall>]) from the beginning of a binding tail.
/// e.g. "[<TailCall>] matchOnePair ..." -> "matchOnePair ..."
/// Handles multiple consecutive attributes via a while loop.
let private stripAttributes (text: string) =
    let attrPattern = Regex(@"^\[<.*?>\](?:\s*)")
    let mutable result = text
    let mutable m = attrPattern.Match(result)

    while m.Success do
        result <- result.Substring(m.Length).TrimStart()
        m <- attrPattern.Match(result)

    result

/// Check if a trimmed line represents a function binding (has parameters)
/// rather than a value binding. Used to distinguish `let f x = ...` from `let x = ...`.
let isFunctionBinding (trimmed: string) =
    letPatterns
    |> Array.exists (fun p ->
        if trimmed.StartsWith(p) then
            let after =
                trimmed.Substring(p.Length).TrimStart()
                |> stripAttributes
                |> stripAccessModifier

            let nameEnd = after.IndexOfAny([| ' '; '('; '=' |])

            if nameEnd >= 0 then
                let name = after.Substring(0, nameEnd)

                if name = "" || name.StartsWith("'") then
                    false
                else
                    let rest = after.Substring(nameEnd).TrimStart()

                    // Skip type annotation if present (e.g., "x : int = 42")
                    let afterType =
                        if rest.StartsWith(":") then
                            let eqIdx = rest.IndexOf('=')

                            if eqIdx >= 0 then
                                rest.Substring(eqIdx).TrimStart()
                            else
                                rest
                        else
                            rest

                    // If after the name (and optional type annotation) the next thing
                    // is '=', it's a value binding. Otherwise parameters follow.
                    not (afterType.StartsWith("="))
            else
                false
        else
            false)

/// Strip bodies of nested `let`-defined function bindings from parent function
/// code. Nested functions (like `entryAt`/`advance` inside `mergeSortedEntriesData`)
/// are already extracted as separate FunctionInfo entries by analyzeFile, so
/// their body keywords must not be counted in the parent's complexity.
///
/// Uses indent-based detection: lines matching a `let`/`let rec`/`member` etc.
/// binding pattern at a deeper indent than `baseIndent`, that also have parameters
/// (making them function bindings, not value bindings), have their subsequent
/// body lines replaced with whitespace. Value bindings (`let x = ...`) are
/// preserved — they are integral to the parent's logic.
let stripNestedFunctionBodies (code: string) (baseIndent: int) : string =
    let lines = code.Split('\n')
    let result = System.Collections.Generic.List<string>()
    // Stack of indent levels of active nested function definitions being stripped
    let mutable skipStack: int list = []

    for lineIdx = 0 to lines.Length - 1 do
        let line = lines.[lineIdx]
        let trimmed = line.TrimStart()
        let indent = line.Length - trimmed.Length

        // Pop entries when we return to the parent's indent level.
        // CRITICAL: Only pop on non-blank lines — blank lines (indent=0) inside
        // a nested function body would otherwise prematurely clear the stack.
        if trimmed <> "" then
            skipStack <- skipStack |> List.filter (fun fnIndent -> indent > fnIndent)

        if not (List.isEmpty skipStack) then
            // Inside a nested function body — replace content with spaces
            if trimmed = "" then
                result.Add(line) // preserve blank lines for line counting
            else
                result.Add(System.String(' ', line.Length))
        else
            // Check if this line starts a nested function binding
            let isNestedFnBinding =
                indent > baseIndent
                && letPatterns |> Array.exists (fun p -> trimmed.StartsWith(p))
                && isFunctionBinding trimmed

            if isNestedFnBinding then
                // Keep the definition line, but strip its body
                skipStack <- indent :: skipStack
                result.Add(line)
            else
                result.Add(line)

    System.String.Join("\n", result)

/// Strip bodies of multi-line computation expressions (`seq { }`, `task { }`,
/// `async { }`, `query { }`) from source code. The F# compiler compiles these
/// into separate enumerator/state-machine classes, so Coverlet attributes
/// their complexity to the generated class, not the enclosing function.
///
/// Uses indentation-based detection: after a line matching `ident {` at a deeper
/// indent than `baseIndent`, all subsequent lines indented further are replaced
/// with whitespace. Single-line computation expressions (`seq { ... }`) are
/// preserved — their content is on the same line as the opener.
///
/// Operates on code that has already had comments/strings stripped.
let stripComputationExpressions (code: string) (baseIndent: int) : string =
    let lines = code.Split('\n')
    let result = System.Collections.Generic.List<string>()
    // Stack of indent levels of active computation expression bodies being stripped
    let mutable skipStack: int list = []

    for lineIdx = 0 to lines.Length - 1 do
        let line = lines.[lineIdx]
        let trimmed = line.TrimStart()
        let indent = line.Length - trimmed.Length

        // Pop entries when we return to the opener's indent level or shallower.
        // CRITICAL: Only pop on non-blank lines — blank lines (indent=0) inside
        // a computation expression would otherwise prematurely clear the stack,
        // exposing all content lines inside the block.
        if trimmed <> "" then
            skipStack <- skipStack |> List.filter (fun ceIndent -> indent > ceIndent)

        if not (List.isEmpty skipStack) then
            // Inside a computation expression body — replace content
            if trimmed = "" then
                result.Add(line) // preserve blank lines for line counting
            else
                result.Add(System.String(' ', line.Length))
        else
            // Check if this line starts a multi-line computation expression:
            // `keyword {` with nothing else on the line after `{` (trailing whitespace ok)
            let ceMatch = Regex.Match(trimmed, @"\b(seq|task|async|query)\s*\{")

            if ceMatch.Success && indent > baseIndent then
                let afterBrace = trimmed.Substring(ceMatch.Index + ceMatch.Length).TrimEnd()

                if afterBrace = "" then
                    // Multi-line computation expression — strip its body
                    skipStack <- indent :: skipStack

                result.Add(line)
            else
                result.Add(line)

    System.String.Join("\n", result)

/// Calculate cyclomatic complexity for a single function body.
/// `bodyText` is the full text of the function (signature line + body lines).
/// Comments and string literals are stripped before keyword counting.
/// `baseIndent` is the indent of the function definition line, used to distinguish
/// top-level lambda-like definitions from nested closure lambdas.
let calculateFunctionComplexity (bodyText: string) (baseIndent: int) : int =
    let code = stripNonCode bodyText
    let code' = stripNestedFunctionBodies code baseIndent
    let code'' = stripComputationExpressions code' baseIndent
    let code''' = stripLambdaBodies code'' baseIndent
    let mutable count = 0

    // Count word-boundary keywords (regex \b to avoid false matches like "for" in "before")
    for kw, weight in wordKeywordPatterns do
        let pattern = @"\b" + kw + @"\b"
        count <- count + Regex.Matches(code''', pattern).Count * weight

    // Count literal substrings (operators etc.)
    for kw, weight in literalKeywordPatterns do
        let mutable idx = code'''.IndexOf(kw, 0)

        while idx >= 0 do
            count <- count + weight
            idx <- code'''.IndexOf(kw, idx + 1)

    // Count match/function cases and try/with handler cases (single pass,
    // no double-counting of nested branches)
    let matchCnt, handlerCnt = countAllBranches code'''
    count <- count + matchCnt
    count <- count + handlerCnt

    // Count && / || only in conditional contexts (if/elif/while/when lines)
    count <- count + countConditionalBooleanOperators code'''

    // Base = 1 (the function itself) + keyword count + tuple bonus
    1 + count

/// Extract function name from a let-like binding line.
let tryExtractName (line: string) =
    letPatterns
    |> Array.tryPick (fun lp ->
        if line.StartsWith(lp) then
            let afterLet =
                line.Substring(lp.Length).TrimStart() |> stripAttributes |> stripAccessModifier

            // Handle backtick-quoted identifiers like ``My Function``
            if afterLet.StartsWith("``") then
                let closeIdx = afterLet.IndexOf("``", 2)

                if closeIdx >= 0 && afterLet.Contains("=") then
                    Some(afterLet.Substring(0, closeIdx + 2))
                else
                    None
            else
                let nameEnd = afterLet.IndexOfAny([| ' '; '('; '=' |])

                let name =
                    if nameEnd >= 0 then
                        afterLet.Substring(0, nameEnd)
                    else
                        afterLet

                if afterLet.Contains("=") && name <> "" && not (name.StartsWith("'")) then
                    // For "and " pattern: skip type definitions like "and BoundingBox = "
                    // or "and BoundingBox<'T> =". These have no arguments before '='.
                    if lp = "and " then
                        let afterName = afterLet.Substring(name.Length).TrimStart()

                        let beforeEq =
                            let eqIdx = afterName.IndexOf('=')

                            if eqIdx >= 0 then
                                afterName.Substring(0, eqIdx).Trim()
                            else
                                afterName.Trim()
                        // Type definitions: nothing or only type params <...> before '='
                        if beforeEq = "" || beforeEq.StartsWith("<") then
                            None
                        else
                            Some(stripSelfIdentifier name)
                    else
                        Some(stripSelfIdentifier name)
                else
                    None
        else
            None)

/// Result of analyzing a single function.
type FunctionInfo =
    { Name: string
      Line: int
      Complexity: int
      BodyText: string }

/// Analyze a single F# source file and return per-function complexity results.
let analyzeFile (filePath: string) : FunctionInfo list =
    let lines = File.ReadAllLines filePath
    let results = ResizeArray<FunctionInfo>()

    let mutable i = 0

    while i < lines.Length do
        let lineNum = i + 1
        let line = lines.[i]
        let trimmed = line.TrimStart()

        // Skip comments and blank lines
        if trimmed.StartsWith("//") || trimmed = "" then
            i <- i + 1
        else
            // Check if this line could start a binding
            let isBindingStart = letPatterns |> Array.exists (fun p -> trimmed.StartsWith(p))

            // Also detect implicit constructors: type Foo(args) = ...
            // Supports both single-line: "type Foo(args) =" and multi-line:
            //   type Foo
            //       (...)
            //   =
            let isCtorStart =
                not isBindingStart
                && trimmed.StartsWith("type ")
                && (
                // Single-line: type Foo(args) =
                (trimmed.Contains("=")
                 && (let eqIdx = trimmed.IndexOf("=")
                     let parenIdx = trimmed.IndexOf("(")
                     parenIdx >= 0 && parenIdx < eqIdx))
                ||
                // Multi-line: type Foo\n    (...\n    ) =
                (not (trimmed.Contains("="))
                 && (let maxPeek = min (i + 15) lines.Length
                     let mutable found = false
                     let mutable peekIdx = i + 1

                     while peekIdx < maxPeek && not found do
                         let pt = lines.[peekIdx].TrimEnd()

                         if pt.EndsWith(") =") then
                             found <- true
                         elif pt <> "" && not (pt.StartsWith("//")) then
                             peekIdx <- peekIdx + 1
                         else
                             peekIdx <- peekIdx + 1

                     found)))

            if not isBindingStart && not isCtorStart then
                i <- i + 1
            else
                // Build effective signature line, merging continuation lines for multi-line signatures.
                // e.g., "let swapMemTableAndWal\n    (arg1: ...)\n    =" -> "let swapMemTableAndWal (arg1: ...) ="
                let sigLine =
                    if trimmed.Contains("=") then
                        trimmed
                    else
                        let maxScan = min (i + 20) lines.Length
                        let mutable merged = trimmed
                        let mutable idx = i + 1

                        while idx < maxScan && not (merged.Contains("=")) do
                            let nl = lines.[idx].TrimStart()

                            if nl <> "" && not (nl.StartsWith("//")) then
                                merged <- merged + " " + nl

                            idx <- idx + 1

                        merged

                let nameOpt =
                    if isCtorStart then
                        // Extract class name from "type ClassName(args) ="
                        let afterType = trimmed.Substring("type ".Length).TrimStart()
                        // Handle backtick names
                        let nameStart =
                            if afterType.StartsWith("``") then
                                let closeIdx = afterType.IndexOf("``", 2)
                                if closeIdx >= 0 then closeIdx + 2 else 0
                            else
                                0

                        let afterName =
                            if nameStart > 0 then
                                afterType.Substring(nameStart).TrimStart()
                            else
                                afterType

                        let parenIdx = afterName.IndexOf("(")

                        let rawName =
                            if parenIdx >= 0 then
                                afterName.Substring(0, parenIdx).Trim()
                            else
                                afterName
                        // Strip generic args: Foo<'T> -> Foo
                        let genericIdx = rawName.IndexOf("<")

                        let name =
                            if genericIdx >= 0 then
                                rawName.Substring(0, genericIdx).Trim()
                            else
                                rawName

                        Some(name, lineNum, trimmed)
                    else
                        tryExtractName sigLine |> Option.map (fun n -> n, lineNum, trimmed)

                match nameOpt with
                | Some(name, _, _) ->
                    let startIndent = line.Length - trimmed.Length
                    let mutable j = i + 1
                    let mutable bodyEnd = lines.Length - 1

                    if isCtorStart then
                        // ── Implicit constructor body ──
                        // Find class body indent from first non-blank line after type declaration.
                        // Body stops at first member/override/interface at the class body indent,
                        // or at a function let binding (which compiles as a separate method, not
                        // part of the constructor IL). Value let bindings ARE part of the constructor.
                        let mutable foundBodyIndent = false
                        let mutable classBodyIndent = startIndent + 4

                        while j < lines.Length do
                            let l = lines.[j]
                            let lt = l.TrimStart()
                            let li = l.Length - lt.Length

                            if lt <> "" && not (lt.StartsWith("//")) && not (lt.StartsWith("[<")) then
                                if not foundBodyIndent && li > startIndent then
                                    classBodyIndent <- li
                                    foundBodyIndent <- true

                                if li <= startIndent then
                                    // Next top-level declaration
                                    bodyEnd <- j - 1
                                    j <- lines.Length
                                elif
                                    foundBodyIndent
                                    && li = classBodyIndent
                                    && (lt.StartsWith("member ")
                                        || lt.StartsWith("override ")
                                        || lt.StartsWith("interface ")
                                        || (letPatterns |> Array.exists (fun p -> lt.StartsWith(p))
                                            && isFunctionBinding lt))
                                then
                                    // First member or function let binding at class body
                                    // indent → constructor ends here
                                    bodyEnd <- j - 1
                                    j <- lines.Length

                            j <- j + 1
                    else
                        // ── Regular function/method body ──
                        // Scan ahead until we hit a binding at same or lesser indent
                        while j < lines.Length do
                            let l = lines.[j]
                            let lt = l.TrimStart()
                            let li = l.Length - lt.Length

                            if
                                li <= startIndent
                                && lt <> ""
                                && not (lt.StartsWith("//"))
                                && not (lt.StartsWith("[<"))
                            then
                                let isNextBinding =
                                    letPatterns |> Array.exists (fun p -> lt.StartsWith(p))
                                    || lt.StartsWith("interface ")
                                    || lt.StartsWith("type ")

                                if isNextBinding then
                                    bodyEnd <- j - 1
                                    j <- lines.Length // break

                            j <- j + 1

                    let bodyLines = lines.[i .. min bodyEnd (lines.Length - 1)]
                    let bodyText = bodyLines |> String.concat "\n"
                    let complexity = calculateFunctionComplexity bodyText startIndent

                    results.Add
                        { Name = name
                          Line = lineNum
                          Complexity = complexity
                          BodyText = bodyText }

                    // For constructors, move past just the type line so that
                    // individual let/member bindings inside the class are still
                    // extracted as separate functions. For regular bindings,
                    // skip the entire body (already captured).
                    if isCtorStart then i <- i + 1 else i <- bodyEnd + 1
                | None -> i <- i + 1

    results |> Seq.toList

// ===========================================================================
// Cobertura XML parsing
// ===========================================================================

let xname s = XName.Get s

/// A method entry from cobertura with its IL-level complexity and source line.
type CoberturaMethod =
    { File: string
      Class: string
      Method: string
      Line: int
      RefCplx: int }

let parseCobertura (filePath: string) : CoberturaMethod list * string list =
    let doc = XDocument.Load filePath

    // Read source base directories from <sources>/<source>
    let sourceDirs =
        doc.Descendants(xname "source")
        |> Seq.map (fun e -> e.Value.TrimEnd('/').TrimEnd('\\'))
        |> Seq.toList

    let methods =
        [ for pkg in doc.Descendants(xname "package") do
              for cls in pkg.Descendants(xname "class") do
                  let filename =
                      cls.Attribute(xname "filename") |> fun a -> if a = null then "?" else a.Value

                  // Resolve to full path using source dirs
                  let fullPath =
                      if Path.IsPathRooted filename then
                          filename
                      else
                          match sourceDirs with
                          | [] -> filename
                          | dir :: _ -> Path.GetFullPath(Path.Combine(dir, filename))

                  let className =
                      cls.Attribute(xname "name") |> fun a -> if a = null then "?" else a.Value

                  for method in cls.Descendants(xname "method") do
                      let name =
                          method.Attribute(xname "name") |> fun a -> if a = null then "?" else a.Value

                      let complexityAttr = method.Attribute(xname "complexity")

                      let complexity =
                          if complexityAttr = null then
                              0
                          else
                              int complexityAttr.Value

                      let firstLine =
                          method.Descendants(xname "line")
                          |> Seq.map (fun l -> l.Attribute(xname "number"))
                          |> Seq.tryPick (fun a -> if a <> null then Some(int a.Value) else None)
                          |> Option.defaultValue 0

                      yield
                          { File = fullPath
                            Class = className
                            Method = name
                            Line = firstLine
                            RefCplx = complexity } ]

    // Collect unique source file paths referenced in cobertura
    let sourceFiles =
        methods
        |> List.map (fun m -> m.File)
        |> List.distinct
        |> List.filter (fun f -> File.Exists f)

    methods, sourceFiles

// ===========================================================================
// Unified record for reporting
// ===========================================================================

/// A method with both calculated complexity and reference (cobertura) complexity.
type ReportItem =
    {
        File: string
        FunctionName: string
        Line: int
        /// Calculated complexity (keyword-based, primary value)
        CalcCplx: int
        /// Reference complexity from cobertura (Coverlet IL-level)
        RefCplx: int
    }

    member this.JudgmentCplx = this.CalcCplx

/// Deduplicate report items so each (File, FunctionName) pair appears only once.
/// When multiple Coverlet methods map to the same source function (e.g. async
/// state machines in Transport.fs), keeps the entry with the highest RefCplx.
let deduplicateItems (items: ReportItem list) : ReportItem list =
    items
    |> List.groupBy (fun m -> m.File, m.FunctionName)
    |> List.map (fun (_, group) -> group |> List.maxBy (fun m -> m.RefCplx))
    |> List.sortByDescending (fun m -> m.JudgmentCplx)

// ===========================================================================
// Report generation
// ===========================================================================

let generateReport (threshold: int) (warnThreshold: int) (items: ReportItem list) =
    let sorted = items |> List.sortByDescending (fun m -> m.JudgmentCplx)
    let errors = sorted |> List.filter (fun m -> m.JudgmentCplx > threshold)

    let warnings =
        sorted
        |> List.filter (fun m -> m.JudgmentCplx > warnThreshold && m.JudgmentCplx <= threshold)

    let totalCalc = items |> List.sumBy (fun m -> m.CalcCplx)
    let totalRef = items |> List.sumBy (fun m -> m.RefCplx)

    if not args'.Quiet then
        printfn ""
        printfn "══════════════════════════════════════════════════════════════"
        printfn "  Cyclomatic Complexity Report"
        printfn "  Threshold (calculated): error > %d, warning > %d" threshold warnThreshold
        printfn "══════════════════════════════════════════════════════════════"

        if List.isEmpty items then
            printfn "  (no methods found)"
        else
            let maxCalc = (List.head sorted).JudgmentCplx
            let avgCalc = float totalCalc / float (List.length items)
            let avgRef = float totalRef / float (List.length items)
            printfn ""
            printfn "  %d methods analyzed" (List.length items)
            printfn "  Calculated (keyword): total %d, max %d, avg %.1f" totalCalc maxCalc avgCalc
            printfn "  Reference  (Coverlet): total %d, avg %.1f" totalRef avgRef
            printfn ""

        if not (List.isEmpty errors) then
            printfn "  ❌ ERROR — Calculated complexity above %d:" threshold
            printfn ""
            printfn "  %-4s %-38s %-22s %5s %5s" "#" "Function" "File" "Calc" "Ref"
            printfn "  %s" (String.replicate 78 "-")

            errors
            |> List.iteri (fun i m ->
                printfn "  %-4d %-38s %-22s %5d %5d" (i + 1) m.FunctionName m.File m.CalcCplx m.RefCplx)

            printfn ""

        if not (List.isEmpty warnings) then
            printfn "  ⚠️  WARNING — Calculated complexity above %d:" warnThreshold
            printfn ""
            printfn "  %-4s %-38s %-22s %5s %5s" "#" "Function" "File" "Calc" "Ref"
            printfn "  %s" (String.replicate 78 "-")

            warnings
            |> List.iteri (fun i m ->
                printfn "  %-4d %-38s %-22s %5d %5d" (i + 1) m.FunctionName m.File m.CalcCplx m.RefCplx)

            printfn ""

        if List.isEmpty errors && List.isEmpty warnings then
            printfn "  ✅ All methods within complexity thresholds."
        else
            printfn "  Summary: %d error(s), %d warning(s)" (List.length errors) (List.length warnings)

    // Top 10 listing (sorted by calculated complexity)
    if not args'.Quiet && not (List.isEmpty sorted) then
        printfn ""
        printfn "  ── Top 10 complex methods (calculated) ──"
        printfn ""
        printfn "  %-6s %-37s %-20s %5s %5s" "#" "Function" "File" "Calc" "Ref"
        printfn "  %s" (String.replicate 78 "-")

        for i, m in sorted |> List.truncate 10 |> List.indexed do
            let icon =
                if m.JudgmentCplx > threshold then "❌"
                elif m.JudgmentCplx > warnThreshold then "⚠️"
                else "✅"

            let label = $"{icon} {i + 1}"
            printfn "  %-6s %-37s %-20s %5d %5d" label m.FunctionName m.File m.CalcCplx m.RefCplx

        printfn ""

    errors, warnings

// ===========================================================================
// Main
// ===========================================================================

let errors, warnings =
    if not (File.Exists args'.CoverageFile) then
        if not args'.Quiet then
            eprintfn "[complexity] Coverage file not found: %s" args'.CoverageFile
            eprintfn "[complexity] Run 'dotnet test' first to generate coverage data."

        exit 1

    // Parse cobertura
    let coberturaMethods, sourceFiles = parseCobertura args'.CoverageFile

    if List.isEmpty sourceFiles then
        if not args'.Quiet then
            eprintfn "[complexity] No source files found. Check the <sources> path in cobertura XML."

        exit 1

    // Analyze all source files referenced in cobertura (paths already resolved in parseCobertura)
    let analyzed =
        sourceFiles
        |> List.collect (fun file ->
            if File.Exists file then
                analyzeFile file |> List.map (fun f -> (file, f))
            else
                [])

    // Build lookup: (file, simple method name) -> FunctionInfo list
    // When multiple functions share the same name in one file (e.g. module
    // function + class member, or duplicated names across scopes), all
    // are kept. tryMatchMethod picks the best match (line proximity)
    // during disambiguation.
    let calcLookup =
        analyzed
        |> List.groupBy (fun (file, f) -> (file, f.Name))
        |> List.map (fun ((file, name), group) -> (file, name), (group |> List.map snd))
        |> Map.ofList

    /// Pick the candidate whose line is closest to the Coverlet method's source line.
    /// When no Coverlet line is available (0), falls back to highest complexity.
    let pickByLineProximity (coverletLine: int) (candidates: FunctionInfo list) =
        if coverletLine <= 0 then
            candidates |> List.maxBy (fun f -> f.Complexity)
        else
            candidates |> List.minBy (fun f -> abs (f.Line - coverletLine))

    // Try to match a single cobertura method to a calculated function.
    // Returns None when the method cannot be resolved (compiler-internal, closures, etc.)
    let tryMatchMethod (cm: CoberturaMethod) : ReportItem option =
        // Extract the simple method name from cobertura's fully-qualified name
        // e.g. "WriteScheme.Builtins.Helper.eqv" -> "eqv"
        // e.g. "unaryMathDomain@119.Invoke" -> "unaryMathDomain@119.Invoke"
        let simpleName =
            let lastDot = cm.Method.LastIndexOf '.'

            if lastDot >= 0 then
                cm.Method.Substring(lastDot + 1)
            else
                cm.Method

        // Check whether a name exists in the lookup map (list may be non-empty).
        let nameExists name =
            Map.tryFind (cm.File, name) calcLookup |> Option.exists (not << List.isEmpty)

        // Try to find a name in calcLookup, with fallback variants for properties.
        let tryFind name =
            if nameExists name then
                Some name
            elif name.StartsWith("get_") || name.StartsWith("set_") then
                let bare = name.Substring(4)
                if nameExists bare then Some bare else None
            else
                None

        let matchedName =
            match tryFind simpleName with
            | Some n -> Some n
            | None ->
                // Method not matched by simple name — try the full method name
                match tryFind cm.Method with
                | Some n -> Some n
                | None ->
                    // Try to match as a compiler-generated closure (e.g. "recover@77-1")
                    // by extracting the parent function name before "@"
                    let atIdx = cm.Method.IndexOf '@'

                    match atIdx with
                    | i when i >= 0 ->
                        let parentName = cm.Method.Substring(0, i)
                        tryFind parentName
                    | _ ->
                        // Fallback 1: .ctor / .cctor → extract simple class name from cm.Class
                        // Note: simpleName is "ctor" (not ".ctor") because LastIndexOf('.') on ".ctor" returns 0,
                        // and Substring(1) strips the leading dot.
                        match simpleName with
                        | "ctor"
                        | "cctor" ->
                            let lastDot = cm.Class.LastIndexOf '.'

                            let className =
                                if lastDot >= 0 then
                                    cm.Class.Substring(lastDot + 1)
                                else
                                    cm.Class

                            // Strip generic suffix like `1, `2, etc.
                            let backtickIdx = className.IndexOf '`'

                            let noGenerics =
                                if backtickIdx >= 0 then
                                    className.Substring(0, backtickIdx)
                                else
                                    className

                            // Strip $ prefix from module class names like "$Log", "$Serialization"
                            let noDollar =
                                if noGenerics.StartsWith("$") then
                                    noGenerics.Substring(1)
                                else
                                    noGenerics

                            match tryFind noDollar with
                            | Some _ as r -> r
                            | None ->
                                // For .cctor (module static init): try filename stem as final fallback
                                match simpleName with
                                | "cctor" ->
                                    let stem = System.IO.Path.GetFileNameWithoutExtension(cm.File)
                                    Some stem
                                | _ -> None
                        | _ ->
                            // Fallback 2: "Invoke" closure → extract parent function from class name
                            match simpleName with
                            | "Invoke" ->
                                let slashIdx = cm.Class.LastIndexOf '/'

                                let afterSlash =
                                    if slashIdx >= 0 then
                                        cm.Class.Substring(slashIdx + 1)
                                    else
                                        cm.Class

                                // Handle Pipe # closures from F# pipeline (|>) expressions.
                                // Class name format: "Pipe #1 input at line 284@287-2"
                                // Extract the source line and find the enclosing function.
                                let pipePattern = Regex @"Pipe #\d+ input at line (\d+)"

                                let pipeMatch = pipePattern.Match afterSlash

                                if pipeMatch.Success && pipeMatch.Groups.[1].Success then
                                    let pipeLine = int pipeMatch.Groups.[1].Value

                                    let enclosing =
                                        analyzed
                                        |> List.filter (fun (f, _) -> f = cm.File)
                                        |> List.map snd
                                        |> List.filter (fun fi -> fi.Line <= pipeLine)
                                        |> List.sortByDescending (fun fi -> fi.Line)
                                        |> List.tryHead

                                    match enclosing with
                                    | Some fi -> tryFind fi.Name
                                    | None -> None
                                else
                                    let atIdx = afterSlash.IndexOf '@'

                                    if atIdx >= 0 then
                                        let parentName = afterSlash.Substring(0, atIdx)

                                        // Strip dotted namespace/module prefix (e.g.
                                        // "CodingAgent.LlmClient.callApiOnce" -> "callApiOnce")
                                        // as a fallback when the class name has no '/'
                                        // separator. The primary '/' splitting above
                                        // already handles the common case.
                                        let simpleParentName =
                                            let lastDot = parentName.LastIndexOf '.'

                                            if lastDot >= 0 then
                                                parentName.Substring(lastDot + 1)
                                            else
                                                parentName

                                        tryFind simpleParentName
                                    else
                                        None
                            | _ -> None

        match matchedName with
        | Some mn ->
            match Map.tryFind (cm.File, mn) calcLookup with
            | Some candidates when not (List.isEmpty candidates) ->
                let fi = pickByLineProximity cm.Line candidates

                Some
                    { File = Path.GetFileName cm.File
                      FunctionName = mn
                      Line = fi.Line
                      CalcCplx = fi.Complexity
                      RefCplx = cm.RefCplx }
            | _ ->
                // Synthetic match: function name not in extracted source functions
                // (e.g. .cctor for module-level code, or .ctor for unmatched constructors)
                Some
                    { File = Path.GetFileName cm.File
                      FunctionName = mn
                      Line = cm.Line
                      CalcCplx = 0
                      RefCplx = cm.RefCplx }
        | None -> None

    // Single pass: call tryMatchMethod once per method, then partition
    let pairs = coberturaMethods |> List.map (fun cm -> cm, tryMatchMethod cm)
    let items = pairs |> List.choose snd

    let unmatched =
        pairs
        |> List.choose (fun (cm, result) -> if result.IsNone then Some cm else None)

    let matchedCount = items.Length
    let totalCount = coberturaMethods.Length

    if args'.Verbose && not args'.Quiet then
        let matchedPct =
            if totalCount > 0 then
                float matchedCount / float totalCount * 100.0
            else
                100.0

        printfn ""
        printfn "  ── Match statistics ──"
        printfn "  Coverlet methods:  %d" totalCount
        printfn "  Source functions:  %d" (Map.count calcLookup)
        printfn "  Matched:           %d (%5.1f%%)" matchedCount matchedPct

        let uniqueCount = items |> deduplicateItems |> List.length

        if uniqueCount < matchedCount then
            printfn
                "  Unique functions:  %d (%.1f%% deduplicated)"
                uniqueCount
                (float (matchedCount - uniqueCount) / float matchedCount * 100.0)

        printfn "  Unmatched:         %d (%5.1f%%)" (totalCount - matchedCount) (100.0 - matchedPct)

        if not (List.isEmpty unmatched) then
            printfn ""
            printfn "  Unmatched methods (no corresponding source function found):"

            for cm in unmatched do
                let simpleClass =
                    let afterSlash =
                        let slashIdx = cm.Class.LastIndexOf '/'

                        if slashIdx >= 0 then
                            cm.Class.Substring(slashIdx + 1)
                        else
                            cm.Class

                    let dotIdx = afterSlash.LastIndexOf '.'

                    if dotIdx >= 0 then
                        afterSlash.Substring(dotIdx + 1)
                    else
                        afterSlash

                printfn "    %s :: %s" (Path.GetFileName cm.File) cm.Method
                printfn "      → class: %s" simpleClass

        printfn ""

    generateReport args'.Threshold args'.WarnThreshold (deduplicateItems items)

// ---------------------------------------------------------------------------
// Exit code
// ---------------------------------------------------------------------------

if not (List.isEmpty errors) then
    if not args'.Quiet then
        printfn "  ❌ FAILED: %d method(s) exceed complexity threshold %d." (List.length errors) args'.Threshold

    exit 1
else
    if not args'.Quiet then
        printfn "  ✅ PASSED."

    exit 0
