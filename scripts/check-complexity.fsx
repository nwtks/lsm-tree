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

/// Result of argument parsing: either successfully parsed Args, or a request to show help.
type private ParseResult =
    | Parsed of Args
    | ShowHelp

/// Print usage information to stdout.
let private printHelp () : unit =
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

/// Parse command-line arguments into Args. Returns ShowHelp when --help is
/// encountered, allowing the caller to handle display and termination.
[<TailCall>]
let rec private parseArgs (acc: Args) (argv: string list) : ParseResult =
    match argv with
    | "--coverage-file" :: v :: rest -> parseArgs { acc with CoverageFile = v } rest
    | "--threshold" :: v :: rest -> parseArgs { acc with Threshold = int v } rest
    | "--warn-threshold" :: v :: rest -> parseArgs { acc with WarnThreshold = int v } rest
    | "--quiet" :: rest -> parseArgs { acc with Quiet = true } rest
    | "--verbose" :: rest -> parseArgs { acc with Verbose = true } rest
    | "--help" :: _ -> ShowHelp
    | _ :: rest -> parseArgs acc rest
    | [] -> Parsed acc

// ===========================================================================
// Compiled regex patterns (performance: compiled once, reused in hot loops)
// ===========================================================================

module private CompiledRegex =
    let matchOrFunction = Regex(@"\b(match|function)\b", RegexOptions.Compiled)
    let matchKeyword = Regex(@"\bmatch(?:!|\b)", RegexOptions.Compiled)
    let tryKeyword = Regex(@"\btry\b", RegexOptions.Compiled)
    let withLineStart = Regex(@"^with\b", RegexOptions.Compiled)
    let conditionalKeywords = Regex(@"\b(if|elif|while|when)\b", RegexOptions.Compiled)

    let booleanOperators =
        Regex(@"(?<!\|)\|\|(?!\|)|(?<!&)&&(?!&)", RegexOptions.Compiled)

    let thenKeyword = Regex(@"\bthen\b", RegexOptions.Compiled)
    let doKeyword = Regex(@"\bdo\b", RegexOptions.Compiled)
    let arrow = Regex(@"->", RegexOptions.Compiled)
    let funKeyword = Regex(@"\bfun\b", RegexOptions.Compiled)
    let compExpr = Regex(@"\b(seq|task|async|query)\s*\{", RegexOptions.Compiled)
    let attrPattern = Regex(@"^\[<.*?>\](?:\s*)", RegexOptions.Compiled)
    let pipePattern = Regex(@"Pipe #\d+ input at line (\d+)", RegexOptions.Compiled)

    /// Pre-compiled word-boundary keyword patterns for complexity counting.
    let wordKeywords: (Regex * int) list =
        [ Regex(@"\bif\b", RegexOptions.Compiled), 1
          Regex(@"\belif\b", RegexOptions.Compiled), 1
          Regex(@"\bfor(?!\!)\b", RegexOptions.Compiled), 1
          Regex(@"\bwhile\b", RegexOptions.Compiled), 1
          Regex(@"\bwhen\b", RegexOptions.Compiled), 1 ]

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

/// Block context for the unified branch-counting state machine.
type private Ctx =
    | MatchBlock of startIndent: int
    | TryBeforeWith of tryIndent: int
    | TryInWithSection of tryIndent: int * withIndent: int

/// Check if a trimmed line represents a `|` branch pattern (not ||, |>, or |]).
let private isPipe (trimmed: string) =
    trimmed.StartsWith("|")
    && not (trimmed.StartsWith("||"))
    && not (trimmed.StartsWith("|>"))
    && not (trimmed.StartsWith("|]"))

/// Pop blocks from the context stack at or below the given indent.
/// These blocks have ended because the code returned to a shallower indent.
[<TailCall>]
let rec private popStack (ctxStack: Ctx list) (indent: int) : Ctx list =
    match ctxStack with
    | [] -> []
    | MatchBlock si :: rest when indent <= si -> popStack rest indent
    | TryBeforeWith ti :: rest when indent <= ti -> popStack rest indent
    | TryInWithSection(ti, _) :: rest when indent <= ti -> popStack rest indent
    | _ -> ctxStack

/// Replace the innermost TryBeforeWith with TryInWithSection.
[<TailCall>]
let rec private replaceTryInStack (ctxStack: Ctx list) (indent: int) : Ctx list option =
    match ctxStack with
    | TryBeforeWith ti :: rest when ti <= indent -> Some(TryInWithSection(ti, indent) :: rest)
    | x :: rest ->
        match replaceTryInStack rest indent with
        | Some newRest -> Some(x :: newRest)
        | None -> None
    | [] -> None

/// Single-character state transition for tuple detection in match expressions.
/// Tracks parenthesis depth and flags when a comma is found at depth 0.
let private scanCharForTupleInMatch (depth: int, found: bool) (c: char) : int * bool =
    if found then
        depth, found
    else
        match c with
        | '('
        | '['
        | '{' -> (depth + 1, false)
        | ')'
        | ']'
        | '}' -> (depth - 1, false)
        | ',' when depth = 0 -> (depth, true)
        | _ -> (depth, false)

/// Check if a match expression after the `match` keyword contains a tuple
/// (comma at depth 0), which adds an extra complexity point.
let private hasTupleInMatchExpr (afterMatch: string) : bool =
    let withIdx = afterMatch.LastIndexOf("with")

    if withIdx < 0 then
        false
    else
        let _, found =
            ((0, false), afterMatch.Substring(0, withIdx))
            ||> Seq.fold scanCharForTupleInMatch

        found

/// Detect a match/function keyword that starts a MatchBlock.
let private detectMatchOrFunction
    (trimmed: string)
    (indent: int)
    (ctxStack: Ctx list, matchCount: int)
    : Ctx list * int * bool =
    if CompiledRegex.matchOrFunction.IsMatch(trimmed) then
        let ctxStack' = MatchBlock indent :: ctxStack

        let matchCount' =
            if CompiledRegex.matchKeyword.IsMatch(trimmed) then
                let m = CompiledRegex.matchKeyword.Match(trimmed)

                if m.Success && hasTupleInMatchExpr (trimmed.Substring(m.Index + m.Length)) then
                    matchCount + 1
                else
                    matchCount
            else
                matchCount

        ctxStack', matchCount', true
    else
        ctxStack, matchCount, false

/// Detect a try keyword that starts a TryBeforeWith block.
let private detectTryKeyword (trimmed: string) (indent: int) (ctxStack: Ctx list) : Ctx list * bool =
    if CompiledRegex.tryKeyword.IsMatch(trimmed) then
        let ctxStack' = TryBeforeWith indent :: ctxStack

        let ctxStack'' =
            if trimmed.Contains(" with ") then
                match ctxStack' with
                | TryBeforeWith ti :: rest when ti = indent -> TryInWithSection(ti, indent) :: rest
                | _ -> ctxStack'
            else
                ctxStack'

        ctxStack'', true
    else
        ctxStack, false

/// Detect a `with` keyword transitioning TryBeforeWith → TryInWithSection.
let private detectWithTransition
    (trimmed: string)
    (indent: int)
    (ctxStack: Ctx list, handlerCount: int)
    : Ctx list * int * bool =
    if trimmed.StartsWith("with") then
        match replaceTryInStack ctxStack indent with
        | Some newStack ->
            let inc =
                let afterWith = trimmed.Substring(4).TrimStart()
                afterWith <> "" && not (afterWith.StartsWith("|")) && afterWith.Contains("->")

            newStack, handlerCount + (if inc then 1 else 0), true
        | None -> ctxStack, handlerCount, false
    else
        ctxStack, handlerCount, false

/// Check if context stack indicates we are inside a `with` handler section.
let private isInsideWithSection (indent: int) (ctxStack: Ctx list) : bool =
    ctxStack
    |> List.exists (function
        | TryInWithSection(_, wi) -> indent >= wi
        | _ -> false)

/// Check if context stack indicates we are inside a match block.
let private isInsideMatchBlock (ctxStack: Ctx list) : bool =
    ctxStack
    |> List.exists (function
        | MatchBlock _ -> true
        | _ -> false)

/// Count a `|` branch as either a match case or a handler case.
let private classifyPipe
    (trimmed: string)
    (indent: int)
    (ctxStack: Ctx list)
    (matchCount: int, handlerCount: int)
    : int * int =
    if isPipe trimmed then
        if isInsideWithSection indent ctxStack then
            matchCount, handlerCount + 1
        elif isInsideMatchBlock ctxStack then
            matchCount + 1, handlerCount
        else
            matchCount, handlerCount
    else
        matchCount, handlerCount

/// Pop closed blocks when a non-blank, non-comment, non-pipe line appears
/// at or shallower than the block's indent.
let private endBlockIfNeeded (trimmed: string) (indent: int) (openedBlock: bool) (ctxStack: Ctx list) : Ctx list =
    let isWithKeyword = CompiledRegex.withLineStart.IsMatch(trimmed)

    if
        not openedBlock
        && not (List.isEmpty ctxStack)
        && trimmed <> ""
        && not (trimmed.StartsWith("//"))
        && not (trimmed.StartsWith("#"))
        && not (trimmed.StartsWith("["))
        && not (isPipe trimmed)
        && not isWithKeyword
    then
        popStack ctxStack indent
    else
        ctxStack

/// Folder helper for countAllBranches: processes a single line through the
/// branch-counting state machine and returns updated state.
let private countAllBranchesFolder
    (ctxStack: Ctx list, matchCount: int, handlerCount: int)
    (line: string)
    : Ctx list * int * int =
    let trimmed = line.TrimStart()
    let indent = line.Length - trimmed.Length

    // Step 1a: Match/function block start
    let ctxStack1, matchCount1, opened1 =
        detectMatchOrFunction trimmed indent (ctxStack, matchCount)

    // Step 1b: Try block start
    let ctxStack2, opened2 = detectTryKeyword trimmed indent ctxStack1

    // Step 2: `with` transition
    let ctxStack3, handlerCount1, opened3 =
        detectWithTransition trimmed indent (ctxStack2, handlerCount)

    let openedBlock = opened1 || opened2 || opened3

    // Step 3: Count `|` branches in context
    let matchCount2, handlerCount2 =
        classifyPipe trimmed indent ctxStack3 (matchCount1, handlerCount1)

    // Step 4: End blocks if needed
    let ctxStack4 = endBlockIfNeeded trimmed indent openedBlock ctxStack3

    (ctxStack4, matchCount2, handlerCount2)

/// Count match/function cases and try/with handler cases in a single pass,
/// ensuring each `|` branch is counted exactly once — avoiding double-counting
/// when a try/with block is nested inside a match/function block.
/// Returns (matchCaseCount, handlerCaseCount).
let countAllBranches (text: string) : int * int =
    let lines = text.Split('\n')

    let (_, matchCount, handlerCount) =
        Array.fold countAllBranchesFolder ([], 0, 0) lines

    matchCount, handlerCount


/// Fold state: update whether we are inside a multi-line condition and count
/// boolean operators (&& / ||) on the current line. Returns (stillInCondition, totalCount).
let private countBooleanOpsOnLine (inCondition: bool, count: int) (line: string) : bool * int =
    let inCondition' = inCondition || CompiledRegex.conditionalKeywords.IsMatch(line)

    let count' =
        count
        + (if inCondition' then
               CompiledRegex.booleanOperators.Matches(line).Count
           else
               0)

    let inCondition'' =
        if
            CompiledRegex.thenKeyword.IsMatch(line)
            || CompiledRegex.doKeyword.IsMatch(line)
            || CompiledRegex.arrow.IsMatch(line)
        then
            false
        else
            inCondition'

    inCondition'', count'

/// Count `&&` and `||` on lines containing `if`/`elif`/`while`/`when` and
/// their continuation lines (multiline conditions), matching FSharpLint's
/// `countBooleanOperators` which only counts boolean operators inside
/// conditions, not in value expressions.
let countConditionalBooleanOperators (text: string) : int =
    let lines = text.Split('\n')
    let _, total = ((false, 0), lines) ||> Array.fold countBooleanOpsOnLine
    total

/// Append whitespace (preserving newlines) to `buf` for the given character.
let private appendWhitespace (c: char) (buf: System.Text.StringBuilder) =
    buf.Append(if c = '\n' then '\n' else ' ') |> ignore

/// Recursive helper that advances through triple-quoted string content.
[<TailCall>]
let rec private skipTripleQuotedStringLoop (text: string) (buf: System.Text.StringBuilder) (j: int) : int =
    if
        j + 2 >= text.Length
        || (text.[j] = '"' && text.[j + 1] = '"' && text.[j + 2] = '"')
    then
        j
    else
        appendWhitespace text.[j] buf
        skipTripleQuotedStringLoop text buf (j + 1)

/// Skip a triple-quoted string """...""" starting at `i` (the opening """).
/// Returns the index after the closing """.
let private skipTripleQuotedString (text: string) (i: int) (buf: System.Text.StringBuilder) : int =
    buf.Append("   ") |> ignore
    let j = skipTripleQuotedStringLoop text buf (i + 3)
    buf.Append("   ") |> ignore
    j + 3

/// Recursive helper that advances through regular/interpolated string content.
/// `braceDepth` tracks nesting in $"..." interpolation braces (-1 = not interpolated).
[<TailCall>]
let rec private skipRegularStringLoop (text: string) (buf: System.Text.StringBuilder) (j: int) (braceDepth: int) : int =
    if j >= text.Length then
        j
    elif braceDepth >= 0 then
        // Interpolated string: stop at closing " when at top level
        if text.[j] = '"' && braceDepth <= 0 then
            j
        else
            let braceDepth' =
                if text.[j] = '{' && (j + 1 >= text.Length || text.[j + 1] <> '{') then
                    braceDepth + 1
                elif text.[j] = '}' && (j + 1 >= text.Length || text.[j + 1] <> '}') then
                    braceDepth - 1
                else
                    braceDepth

            appendWhitespace text.[j] buf
            skipRegularStringLoop text buf (j + 1) braceDepth'
    else if
        // Regular string: stop at unescaped "
        text.[j] = '"' && text.[j - 1] <> '\\'
    then
        j
    else
        appendWhitespace text.[j] buf
        skipRegularStringLoop text buf (j + 1) braceDepth

/// Skip a regular string "..." or interpolated string $"...".
/// Returns the index after the closing ".
let private skipRegularString (text: string) (i: int) (buf: System.Text.StringBuilder) : int =
    let isInterpolated = i > 0 && text.[i - 1] = '$'
    buf.Append(' ') |> ignore
    let braceDepth = if isInterpolated then 0 else -1
    let j = skipRegularStringLoop text buf (i + 1) braceDepth
    buf.Append(' ') |> ignore
    j + 1

/// Recursive helper for block comment content, tracking nesting depth.
[<TailCall>]
let rec private skipBlockCommentLoop (text: string) (buf: System.Text.StringBuilder) (j: int) (depth: int) : int =
    if j >= text.Length || depth <= 0 then
        j
    elif j + 1 < text.Length && text.[j] = '(' && text.[j + 1] = '*' then
        buf.Append("  ") |> ignore
        skipBlockCommentLoop text buf (j + 2) (depth + 1)
    elif j + 1 < text.Length && text.[j] = '*' && text.[j + 1] = ')' then
        let depth' = depth - 1

        if depth' > 0 then
            buf.Append("  ") |> ignore

        skipBlockCommentLoop text buf (j + 2) depth'
    else
        appendWhitespace text.[j] buf
        skipBlockCommentLoop text buf (j + 1) depth

/// Skip a block comment (* ... *), handling nesting.
/// Returns the index after the closing *).
let private skipBlockComment (text: string) (i: int) (buf: System.Text.StringBuilder) : int =
    buf.Append("  ") |> ignore
    skipBlockCommentLoop text buf (i + 2) 1

/// Recursive helper that replaces line comment content with spaces.
[<TailCall>]
let rec private skipLineCommentLoop (text: string) (buf: System.Text.StringBuilder) (j: int) : int =
    if j >= text.Length || text.[j] = '\n' then
        j
    else
        buf.Append(' ') |> ignore
        skipLineCommentLoop text buf (j + 1)

/// Skip a line comment // ... starting at `i`.
/// Returns the index after the newline (or end of text).
let private skipLineComment (text: string) (i: int) (buf: System.Text.StringBuilder) : int =
    skipLineCommentLoop text buf i

/// Recursive loop that strips string literals, comments, and block comments
/// from source text to prevent false keyword matches inside them.
[<TailCall>]
let rec private stripNonCodeLoop (text: string) (buf: System.Text.StringBuilder) (i: int) : unit =
    if i < text.Length then
        match text.[i] with
        | '"' when i + 2 < text.Length && text.[i + 1] = '"' && text.[i + 2] = '"' ->
            stripNonCodeLoop text buf (skipTripleQuotedString text i buf)
        | '"' -> stripNonCodeLoop text buf (skipRegularString text i buf)
        | '(' when i + 1 < text.Length && text.[i + 1] = '*' -> stripNonCodeLoop text buf (skipBlockComment text i buf)
        | '/' when i + 1 < text.Length && text.[i + 1] = '/' -> stripNonCodeLoop text buf (skipLineComment text i buf)
        | c ->
            buf.Append(c) |> ignore
            stripNonCodeLoop text buf (i + 1)

/// Strip string literals, line comments, and block comments from source
/// text to prevent false keyword matches inside them. Handles "...",
/// triple-quoted """...""", // line comments, and (* ... *) block comments
/// (including nested block comments). Preserves newlines for line-tracking.
let stripNonCode (text: string) : string =
    let buf = System.Text.StringBuilder(text.Length)
    stripNonCodeLoop text buf 0
    buf.ToString()

/// Folder helper for stripBlockBodies: processes a single line and returns the
/// updated (skipStack, accumulatedLines).
let private stripBlockBodiesFolder
    (baseIndent: int)
    (isBlockStart: int -> string -> bool)
    (skipStack: int list, acc: string list)
    (line: string)
    : int list * string list =
    let trimmed = line.TrimStart()
    let indent = line.Length - trimmed.Length

    // Pop entries when returning to the block's indent or shallower.
    // CRITICAL: Only pop on non-blank lines — blank lines (indent=0) inside
    // a block would prematurely clear the stack, exposing the block's content.
    let skipStack' =
        if trimmed <> "" then
            skipStack |> List.filter (fun blockIndent -> indent > blockIndent)
        else
            skipStack

    let lineOut, skipStack'' =
        if not (List.isEmpty skipStack') then
            // Inside a block — replace content with spaces, preserve newlines
            if trimmed = "" then
                line, skipStack'
            else
                System.String(' ', line.Length), skipStack'
        elif indent > baseIndent && isBlockStart indent trimmed then
            line, indent :: skipStack'
        else
            line, skipStack'

    skipStack'', lineOut :: acc

/// Generic body stripper: replaces bodies of blocks identified by `isBlockStart`
/// with whitespace (preserving newlines). `isBlockStart indent trimmedLine`
/// returns true when the line starts a block whose body should be stripped.
let stripBlockBodies (code: string) (baseIndent: int) (isBlockStart: int -> string -> bool) : string =
    let lines = code.Split('\n') |> Array.toList
    let folder = stripBlockBodiesFolder baseIndent isBlockStart
    let (_, acc) = List.fold folder ([], []) lines
    acc |> List.rev |> String.concat "\n"

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
/// Check if a trimmed line starts a multi-line lambda block:
/// `fun params ->` followed by empty body (body on subsequent lines).
let private isLambdaBlockFn (_indent: int) (trimmed: string) : bool =
    let m = CompiledRegex.funKeyword.Match(trimmed)

    if not m.Success then
        false
    else
        let afterKw = trimmed.Substring(m.Index + 3)
        let arrowIdx = afterKw.IndexOf("->")
        arrowIdx >= 0 && afterKw.Substring(arrowIdx + 2).TrimStart() = ""

let stripLambdaBodies (code: string) (baseIndent: int) : string =
    stripBlockBodies code baseIndent isLambdaBlockFn

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
/// Handles multiple consecutive attributes via recursion.
[<TailCall>]
let rec private stripAttributes (text: string) =
    let m = CompiledRegex.attrPattern.Match(text)

    if m.Success then
        stripAttributes (text.Substring(m.Length).TrimStart())
    else
        text

/// Skip optional type annotation (e.g. ": int =") from the rest of a binding,
/// returning the portion after any type annotation, or the original text.
let private skipTypeAnnotation (rest: string) : string =
    if rest.StartsWith(":") then
        let eqIdx = rest.IndexOf('=')

        if eqIdx >= 0 then
            rest.Substring(eqIdx).TrimStart()
        else
            rest
    else
        rest

/// Check if a trimmed line (after a pattern prefix) starts a function binding.
/// Returns true when the name portion is valid and what follows is not '=' (value binding).
let private isFnBindingAfterPattern (trimmed: string) (p: string) : bool =
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
                let afterType = skipTypeAnnotation rest

                // If after the name (and optional type annotation) the next thing
                // is '=', it's a value binding. Otherwise parameters follow.
                not (afterType.StartsWith("="))
        else
            false
    else
        false

/// Check if a trimmed line represents a function binding (has parameters)
/// rather than a value binding. Used to distinguish `let f x = ...` from `let x = ...`.
let isFunctionBinding (trimmed: string) =
    letPatterns |> Array.exists (isFnBindingAfterPattern trimmed)

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
/// Check if a trimmed line at a deeper indent than baseIndent starts a
/// nested function binding (e.g. `let f x = ...` inside another function).
/// Value bindings (`let x = ...`) are excluded — they are part of the parent.
let private isNestedFnPred (_indent: int) (trimmed: string) : bool =
    letPatterns |> Array.exists (fun p -> trimmed.StartsWith(p))
    && isFunctionBinding trimmed

let stripNestedFunctionBodies (code: string) (baseIndent: int) : string =
    stripBlockBodies code baseIndent isNestedFnPred

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
/// Check if a trimmed line starts a multi-line computation expression
/// (`seq {`, `task {`, `async {`, `query {`) with empty body on the same line.
let private isCompExprPred (_indent: int) (trimmed: string) : bool =
    let m = CompiledRegex.compExpr.Match(trimmed)
    m.Success && trimmed.Substring(m.Index + m.Length).TrimEnd() = ""

let stripComputationExpressions (code: string) (baseIndent: int) : string =
    stripBlockBodies code baseIndent isCompExprPred

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

    // Count word-boundary keywords (regex \b to avoid false matches like "for" in "before")
    let keywordCount =
        CompiledRegex.wordKeywords
        |> List.sumBy (fun (regex, weight) -> regex.Matches(code''').Count * weight)

    // Count match/function cases and try/with handler cases (single pass,
    // no double-counting of nested branches)
    let matchCnt, handlerCnt = countAllBranches code'''

    // Count && / || only in conditional contexts (if/elif/while/when lines)
    let boolOpCount = countConditionalBooleanOperators code'''

    // Base = 1 (the function itself) + keyword count + match/handler + boolean operators
    1 + keywordCount + matchCnt + handlerCnt + boolOpCount

/// Try to extract a backtick-quoted identifier (``like this``) from the binding tail.
let private tryExtractBacktickName (afterLet: string) : string option =
    if afterLet.StartsWith("``") then
        let closeIdx = afterLet.IndexOf("``", 2)

        if closeIdx >= 0 && afterLet.Contains("=") then
            Some(afterLet.Substring(0, closeIdx + 2))
        else
            None
    else
        None

/// Check whether an "and " line is a type definition (e.g. "and BoundingBox<'T> =")
/// rather than a function binding. Returns Some name when it IS a function binding.
let private tryExtractNameFromAndPattern (afterLet: string) (name: string) : string option =
    let afterName = afterLet.Substring(name.Length).TrimStart()

    let beforeEq =
        let eqIdx = afterName.IndexOf('=')

        if eqIdx >= 0 then
            afterName.Substring(0, eqIdx).Trim()
        else
            afterName.Trim()
    // Type definitions have nothing or only type params <...> before '='
    if beforeEq = "" || beforeEq.StartsWith("<") then
        None
    else
        Some(stripSelfIdentifier name)

/// Try to extract a plain (non-backtick) identifier from the binding tail.
let private tryExtractPlainName (afterLet: string) (lp: string) : string option =
    let nameEnd = afterLet.IndexOfAny([| ' '; '('; '=' |])

    let name =
        if nameEnd >= 0 then
            afterLet.Substring(0, nameEnd)
        else
            afterLet

    if afterLet.Contains("=") && name <> "" && not (name.StartsWith("'")) then
        if lp = "and " then
            tryExtractNameFromAndPattern afterLet name
        else
            Some(stripSelfIdentifier name)
    else
        None

/// Try to extract a function name from a line starting with a given pattern prefix.
let private tryExtractNameFromPattern (line: string) (lp: string) : string option =
    if line.StartsWith(lp) then
        let afterLet =
            line.Substring(lp.Length).TrimStart() |> stripAttributes |> stripAccessModifier

        match tryExtractBacktickName afterLet with
        | Some _ as r -> r
        | None -> tryExtractPlainName afterLet lp
    else
        None

/// Extract function name from a let-like binding line.
let tryExtractName (line: string) =
    letPatterns |> Array.tryPick (tryExtractNameFromPattern line)

/// Result of analyzing a single function.
type FunctionInfo =
    { Name: string
      Line: int
      Complexity: int
      BodyText: string }

/// Recursive loop that scans continuation lines building up a full signature string.
[<TailCall>]
let rec private buildSignatureLineLoop
    (lines: string array)
    (maxScan: int)
    (sb: System.Text.StringBuilder)
    (pos: int)
    : string =
    if pos >= maxScan then
        sb.ToString()
    else
        let nl = lines.[pos].TrimStart()
        let hasContent = nl <> "" && not (nl.StartsWith("//"))

        if hasContent then
            sb.Append(' ').Append(nl) |> ignore

        if hasContent && nl.Contains("=") then
            sb.ToString()
        else
            buildSignatureLineLoop lines maxScan sb (pos + 1)

/// Build an effective signature line by merging continuation lines
/// for multi-line signatures, e.g.:
///   "let swapMemTableAndWal\n    (arg1: ...)\n    ="
///   → "let swapMemTableAndWal (arg1: ...) ="
let private buildSignatureLine (lines: string array) (idx: int) (trimmed: string) =
    if trimmed.Contains("=") then
        trimmed
    else
        let maxScan = min (idx + 20) lines.Length
        buildSignatureLineLoop lines maxScan (System.Text.StringBuilder(trimmed)) (idx + 1)

/// Recursively scan lines for ") =" to detect multi-line constructor form.
[<TailCall>]
let rec private scanForCtorClose (lines: string array) (maxPeek: int) (peekIdx: int) : bool =
    if peekIdx >= maxPeek then
        false
    else
        let pt = lines.[peekIdx].TrimEnd()

        if pt.EndsWith(") =") then
            true
        elif pt <> "" && not (pt.StartsWith("//")) then
            scanForCtorClose lines maxPeek (peekIdx + 1)
        else
            scanForCtorClose lines maxPeek (peekIdx + 1)

/// Check if a line starts an implicit constructor: type Foo(args) = ...
/// Supports both single-line and multi-line forms:
///   type Foo(args) =
///   type Foo
///       (...)
///       =
let private isConstructorStart (lines: string array) (idx: int) (trimmed: string) =
    trimmed.StartsWith("type ")
    && (
    // Single-line: type Foo(args) =
    (trimmed.Contains("=")
     && (let eqIdx = trimmed.IndexOf("=")
         let parenIdx = trimmed.IndexOf("(")
         parenIdx >= 0 && parenIdx < eqIdx))
    ||
    // Multi-line: type Foo\n    (...\n    ) =
    (not (trimmed.Contains("="))
     && scanForCtorClose lines (min (idx + 15) lines.Length) (idx + 1)))

/// Extract class name from a constructor line like "type Foo(args) ="
let private extractCtorName (line: string) =
    let stripBacktick (s: string) =
        if s.StartsWith("``") then
            let ci = s.IndexOf("``", 2)
            if ci >= 0 then s.Substring(ci + 2).TrimStart() else s
        else
            s

    let takeBeforeParen (s: string) =
        let pi = s.IndexOf("(")
        if pi >= 0 then s.Substring(0, pi).Trim() else s

    let stripGenerics (s: string) =
        let gi = s.IndexOf("<")
        if gi >= 0 then s.Substring(0, gi).Trim() else s

    line.Substring("type ".Length).TrimStart()
    |> stripBacktick
    |> takeBeforeParen
    |> stripGenerics

/// Recursive loop scanning a constructor body for where it ends.
[<TailCall>]
let rec private scanCtorBodyLoop
    (lines: string array)
    (startIndent: int)
    (j: int)
    (foundBodyIndent: bool)
    (classBodyIndent: int)
    : int =
    if j >= lines.Length then
        lines.Length - 1
    else
        let lt = lines.[j].TrimStart()
        let li = lines.[j].Length - lt.Length

        if lt = "" || lt.StartsWith("//") || lt.StartsWith("[<") then
            scanCtorBodyLoop lines startIndent (j + 1) foundBodyIndent classBodyIndent
        else
            let foundBodyIndent', classBodyIndent' =
                if not foundBodyIndent && li > startIndent then
                    true, li
                else
                    foundBodyIndent, classBodyIndent

            if li <= startIndent then
                // Next top-level declaration
                j - 1
            elif
                foundBodyIndent'
                && li = classBodyIndent'
                && (lt.StartsWith("member ")
                    || lt.StartsWith("override ")
                    || lt.StartsWith("interface ")
                    || (letPatterns |> Array.exists (fun p -> lt.StartsWith(p)) && isFunctionBinding lt))
            then
                // First member or function let binding → constructor ends here
                j - 1
            else
                scanCtorBodyLoop lines startIndent (j + 1) foundBodyIndent' classBodyIndent'

/// Scan a constructor body from `startIdx` to find where it ends.
/// The body stops at the first member/override/interface at the class body indent,
/// or at a function let binding (which compiles as a separate method).
/// Value let bindings ARE part of the constructor.
let private scanCtorBody (lines: string array) (startIdx: int) (startIndent: int) =
    scanCtorBodyLoop lines startIndent (startIdx + 1) false (startIndent + 4)

/// Recursive loop scanning a function/method body for where it ends.
[<TailCall>]
let rec private scanFunctionBodyLoop (lines: string array) (startIndent: int) (j: int) : int =
    if j >= lines.Length then
        lines.Length - 1
    else
        let lt = lines.[j].TrimStart()
        let li = lines.[j].Length - lt.Length

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
                j - 1
            else
                scanFunctionBodyLoop lines startIndent (j + 1)
        else
            scanFunctionBodyLoop lines startIndent (j + 1)

/// Scan a regular function/method body from `startIdx` to find where it ends.
/// Stops at the next binding at the same or lesser indent.
let private scanFunctionBody (lines: string array) (startIdx: int) (startIndent: int) =
    scanFunctionBodyLoop lines startIndent (startIdx + 1)

/// Create a FunctionInfo from a matched binding line, returning the record
/// and the next line index to continue scanning from.
let private createFunctionInfo
    (lines: string array)
    (i: int)
    (line: string)
    (trimmed: string)
    (lineNum: int)
    (isCtorStart: bool)
    (name: string)
    : FunctionInfo * int =
    let startIndent = line.Length - trimmed.Length

    let bodyEnd =
        if isCtorStart then
            scanCtorBody lines i startIndent
        else
            scanFunctionBody lines i startIndent

    let bodyLines = lines.[i .. min bodyEnd (lines.Length - 1)]
    let bodyText = bodyLines |> String.concat "\n"
    let complexity = calculateFunctionComplexity bodyText startIndent

    { Name = name
      Line = lineNum
      Complexity = complexity
      BodyText = bodyText },
    // For constructors, move past just the type line so that
    // individual let/member bindings inside the class are still
    // extracted as separate functions. For regular bindings,
    // skip the entire body.
    if isCtorStart then i + 1 else bodyEnd + 1

/// Result of classifying a source line: either a function/method boundary
/// with extracted metadata, or an uninteresting line to skip.
type private LineClassification =
    | FunctionStart of lineIdx: int * line: string * trimmed: string * lineNum: int * name: string * isCtor: bool
    | NotRelevant

/// Try to classify the current line as a function/method definition start.
/// Returns FunctionStart with extracted metadata, or NotRelevant to skip.
let private tryClassifyLine (lines: string array) (i: int) : LineClassification =
    let trimmed = lines.[i].TrimStart()

    if trimmed.StartsWith("//") || trimmed = "" then
        NotRelevant
    else
        let isBindingStart = letPatterns |> Array.exists (fun p -> trimmed.StartsWith(p))
        let isCtorStart = not isBindingStart && isConstructorStart lines i trimmed

        if not isBindingStart && not isCtorStart then
            NotRelevant
        else
            let sigLine = buildSignatureLine lines i trimmed
            let lineNum = i + 1

            let nameOpt =
                if isCtorStart then
                    Some(extractCtorName trimmed)
                else
                    tryExtractName sigLine

            match nameOpt with
            | Some name -> FunctionStart(i, lines.[i], trimmed, lineNum, name, isCtorStart)
            | None -> NotRelevant

/// Recursive main loop over lines, building a result list with
/// tail-recursive accumulation (F# list prepend, reversed at the end).
[<TailCall>]
let rec private analyzeLoop (lines: string array) (acc: FunctionInfo list) (i: int) : FunctionInfo list =
    if i >= lines.Length then
        List.rev acc
    else
        match tryClassifyLine lines i with
        | FunctionStart(idx, line, trimmed, lineNum, name, isCtor) ->
            let fi, nextI = createFunctionInfo lines idx line trimmed lineNum isCtor name
            analyzeLoop lines (fi :: acc) nextI
        | NotRelevant -> analyzeLoop lines acc (i + 1)

/// Analyze a single F# source file and return per-function complexity results.
let analyzeFile (filePath: string) : FunctionInfo list =
    let lines = File.ReadAllLines filePath
    analyzeLoop lines [] 0

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

/// Parse a single <method> XML element into a CoberturaMethod record.
let private parseXmlMethod (fullPath: string) (className: string) (method: XElement) : CoberturaMethod =
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

    { File = fullPath
      Class = className
      Method = name
      Line = firstLine
      RefCplx = complexity }

/// Resolve a filename to a full path using source directories from cobertura.
let private resolveSourcePath (sourceDirs: string list) (filename: string) : string =
    if Path.IsPathRooted filename then
        filename
    else
        match sourceDirs with
        | [] -> filename
        | dir :: _ -> Path.GetFullPath(Path.Combine(dir, filename))

/// Collect source base directories from <sources>/<source> elements.
let private collectSourceDirs (doc: XDocument) : string list =
    doc.Descendants(xname "source")
    |> Seq.map (fun e -> e.Value.TrimEnd('/').TrimEnd('\\'))
    |> Seq.toList

/// Parse all <method> elements from cobertura XML into CoberturaMethod records.
let private parseCoberturaMethods (doc: XDocument) (sourceDirs: string list) : CoberturaMethod list =
    doc.Descendants(xname "package")
    |> Seq.collect (fun pkg -> pkg.Descendants(xname "class"))
    |> Seq.collect (fun cls ->
        let filename =
            cls.Attribute(xname "filename") |> fun a -> if a = null then "?" else a.Value

        let fullPath = resolveSourcePath sourceDirs filename

        let className =
            cls.Attribute(xname "name") |> fun a -> if a = null then "?" else a.Value

        cls.Descendants(xname "method") |> Seq.map (parseXmlMethod fullPath className))
    |> Seq.toList

let parseCobertura (filePath: string) : CoberturaMethod list * string list =
    let doc = XDocument.Load filePath
    let sourceDirs = collectSourceDirs doc
    let methods = parseCoberturaMethods doc sourceDirs

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

/// Print the table header for a complexity listing.
let private printTableHeader (quiet: bool) =
    if not quiet then
        printfn ""
        printfn "  %-4s %-38s %-22s %5s %5s" "#" "Function" "File" "Calc" "Ref"
        printfn "  %s" (String.replicate 78 "-")

/// Print a single item row in the complexity table.
let private printTableRow (quiet: bool) (i: int) (m: ReportItem) =
    if not quiet then
        printfn "  %-4d %-38s %-22s %5d %5d" (i + 1) m.FunctionName m.File m.CalcCplx m.RefCplx

/// Print a section of items with a header label, or nothing if the list is empty.
let private printItemSection (quiet: bool) (header: string) (items: ReportItem list) =
    if not (List.isEmpty items) then
        printfn ""
        printfn "  %s" header
        printTableHeader quiet
        items |> List.iteri (printTableRow quiet)
        printfn ""

/// Print the top-level report header lines.
let private printReportHeader (threshold: int) (warnThreshold: int) =
    printfn ""
    printfn "══════════════════════════════════════════════════════════════"
    printfn "  Cyclomatic Complexity Report"
    printfn "  Threshold (calculated): error > %d, warning > %d" threshold warnThreshold
    printfn "══════════════════════════════════════════════════════════════"

/// Print method count and average/max statistics.
let private printMethodStats (items: ReportItem list) (sorted: ReportItem list) (totalCalc: int) (totalRef: int) =
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

/// Print the report header, statistics, error/warning sections.
let private printReportStatistics
    (quiet: bool)
    (threshold: int)
    (warnThreshold: int)
    (items: ReportItem list)
    (sorted: ReportItem list)
    (errors: ReportItem list)
    (warnings: ReportItem list)
    (totalCalc: int)
    (totalRef: int)
    =
    printReportHeader threshold warnThreshold
    printMethodStats items sorted totalCalc totalRef

    let errorHeader = $"❌ ERROR — Calculated complexity above {threshold}:"
    let warnHeader = $"⚠️  WARNING — Calculated complexity above {warnThreshold}:"
    printItemSection quiet errorHeader errors
    printItemSection quiet warnHeader warnings

    if List.isEmpty errors && List.isEmpty warnings then
        printfn "  ✅ All methods within complexity thresholds."
    else
        printfn "  Summary: %d error(s), %d warning(s)" (List.length errors) (List.length warnings)

/// Print a single row in the Top 10 listing.
let private printTopRow (threshold: int) (warnThreshold: int) (i: int) (m: ReportItem) =
    let icon =
        if m.JudgmentCplx > threshold then "❌"
        elif m.JudgmentCplx > warnThreshold then "⚠️"
        else "✅"

    printfn "  %-6s %-37s %-20s %5d %5d" $"{icon} {i + 1}" m.FunctionName m.File m.CalcCplx m.RefCplx

/// Print the Top 10 listing by calculated complexity.
let private printTop10List (threshold: int) (warnThreshold: int) (sorted: ReportItem list) =
    if not (List.isEmpty sorted) then
        printfn ""
        printfn "  ── Top 10 complex methods (calculated) ──"
        printfn ""
        printfn "  %-6s %-37s %-20s %5s %5s" "#" "Function" "File" "Calc" "Ref"
        printfn "  %s" (String.replicate 78 "-")

        sorted |> List.truncate 10 |> List.iteri (printTopRow threshold warnThreshold)
        printfn ""

/// Compute sorted list, error/warning groups, and totals from report items.
let private computeReportStats
    (threshold: int)
    (warnThreshold: int)
    (items: ReportItem list)
    : ReportItem list * ReportItem list * ReportItem list * int * int =
    let sorted = items |> List.sortByDescending (fun m -> m.JudgmentCplx)
    let errors = sorted |> List.filter (fun m -> m.JudgmentCplx > threshold)

    let warnings =
        sorted
        |> List.filter (fun m -> m.JudgmentCplx > warnThreshold && m.JudgmentCplx <= threshold)

    let totalCalc = items |> List.sumBy (fun m -> m.CalcCplx)
    let totalRef = items |> List.sumBy (fun m -> m.RefCplx)
    sorted, errors, warnings, totalCalc, totalRef

let generateReport (quiet: bool) (threshold: int) (warnThreshold: int) (items: ReportItem list) =
    let sorted, errors, warnings, totalCalc, totalRef =
        computeReportStats threshold warnThreshold items

    if not quiet then
        printReportStatistics quiet threshold warnThreshold items sorted errors warnings totalCalc totalRef
        printTop10List threshold warnThreshold sorted

    errors, warnings

/// Print a single unmatched method entry.
let private printUnmatchedMethod (cm: CoberturaMethod) =
    let afterSlash =
        let slashIdx = cm.Class.LastIndexOf '/'

        if slashIdx >= 0 then
            cm.Class.Substring(slashIdx + 1)
        else
            cm.Class

    let simpleClass =
        let dotIdx = afterSlash.LastIndexOf '.'

        if dotIdx >= 0 then
            afterSlash.Substring(dotIdx + 1)
        else
            afterSlash

    printfn "    %s :: %s" (Path.GetFileName cm.File) cm.Method
    printfn "      → class: %s" simpleClass

/// Print verbose match statistics including unmatched method details.
let private printMatchStatistics
    (totalCount: int)
    (matchedCount: int)
    (items: ReportItem list)
    (calcLookup: Map<(string * string), FunctionInfo list>)
    (unmatched: CoberturaMethod list)
    =
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
        unmatched |> List.iter printUnmatchedMethod

    printfn ""

// ===========================================================================
// Main
// ===========================================================================

/// Pick candidate whose line is closest to the Coverlet method's source line.
let private pickByLineProximity (coverletLine: int) (candidates: FunctionInfo list) =
    if coverletLine <= 0 then
        candidates |> List.maxBy (fun f -> f.Complexity)
    else
        candidates |> List.minBy (fun f -> abs (f.Line - coverletLine))

/// Check if a name exists in the calcLookup map for the given file.
let private nameExistsInLookup
    (calcLookup: Map<(string * string), FunctionInfo list>)
    (file: string)
    (n: string)
    : bool =
    Map.tryFind (file, n) calcLookup |> Option.exists (not << List.isEmpty)

/// Try to find a function name in the lookup, falling back from get_/set_ prefix
/// to the bare name.
let private tryFindNameInLookup
    (calcLookup: Map<(string * string), FunctionInfo list>)
    (file: string)
    (n: string)
    : string option =
    let candidates =
        n
        :: (if n.StartsWith("get_") || n.StartsWith("set_") then
                [ n.Substring 4 ]
            else
                [])

    candidates |> List.tryFind (nameExistsInLookup calcLookup file)

/// Try to match a Coverlet method to a source function by simple name.
let private matchByName
    (calcLookup: Map<(string * string), FunctionInfo list>)
    (file: string)
    (name: string)
    : string option =
    tryFindNameInLookup calcLookup file name

/// Try to match a compiler-generated closure: "functionName@lineNumber"
let private matchAsClosure
    (calcLookup: Map<(string * string), FunctionInfo list>)
    (file: string)
    (methodName: string)
    : string option =
    let atIdx = methodName.IndexOf '@'

    if atIdx >= 0 then
        matchByName calcLookup file (methodName.Substring(0, atIdx))
    else
        None

/// Extract the simple class name from a fully-qualified class name.
/// Handles backtick generics ("Foo`1" → "Foo") and compiler prefix ("$Foo" → "Foo").
let private extractSimpleClassName (className: string) : string =
    let lastSegment (s: string) =
        let dotIdx = s.LastIndexOf '.'
        if dotIdx >= 0 then s.Substring(dotIdx + 1) else s

    let stripGenericArgs (s: string) =
        let btIdx = s.IndexOf '`'
        if btIdx >= 0 then s.Substring(0, btIdx) else s

    let stripCompilerPrefix (s: string) =
        if s.StartsWith("$") then s.Substring(1) else s

    className |> lastSegment |> stripGenericArgs |> stripCompilerPrefix

/// Try to match .ctor / .cctor constructor methods.
let private matchAsCtor
    (calcLookup: Map<(string * string), FunctionInfo list>)
    (file: string)
    (className: string)
    (simpleName: string)
    : string option =
    match simpleName with
    | "ctor"
    | "cctor" ->
        let noGenerics = extractSimpleClassName className

        match matchByName calcLookup file noGenerics with
        | Some _ as r -> r
        | None when simpleName = "cctor" -> Some(Path.GetFileNameWithoutExtension(file))
        | None -> None
    | _ -> None

/// Find enclosing function for Pipe # closure (F# pipeline |>).
let private tryMatchPipeClosure
    (calcLookup: Map<(string * string), FunctionInfo list>)
    (analyzed: (string * FunctionInfo) list)
    (file: string)
    (pipeLine: int)
    : string option =
    let enclosing =
        analyzed
        |> List.filter (fun (f, _) -> f = file)
        |> List.map snd
        |> List.filter (fun fi -> fi.Line <= pipeLine)
        |> List.sortByDescending (fun fi -> fi.Line)
        |> List.tryHead

    match enclosing with
    | Some fi -> matchByName calcLookup file fi.Name
    | None -> None

/// Extract the simple parent function name from a compiler-generated Invoke class name.
/// Handles patterns like "ClassName@123" → "ClassName" and "Outer.Type@123" → "Type".
let private extractInvokeParentName (afterSlash: string) : string option =
    let atIdx = afterSlash.IndexOf '@'

    if atIdx >= 0 then
        let parentName = afterSlash.Substring(0, atIdx)
        let lastDot = parentName.LastIndexOf '.'

        let simpleParentName =
            if lastDot >= 0 then
                parentName.Substring(lastDot + 1)
            else
                parentName

        Some simpleParentName
    else
        None

/// Try to match an "Invoke" closure (compiler-generated for lambdas).
let private matchAsInvoke
    (calcLookup: Map<(string * string), FunctionInfo list>)
    (analyzed: (string * FunctionInfo) list)
    (file: string)
    (className: string)
    : string option =
    let slashIdx = className.LastIndexOf '/'

    let afterSlash =
        if slashIdx >= 0 then
            className.Substring(slashIdx + 1)
        else
            className

    let pipeMatch = CompiledRegex.pipePattern.Match afterSlash

    if pipeMatch.Success && pipeMatch.Groups.[1].Success then
        tryMatchPipeClosure calcLookup analyzed file (int pipeMatch.Groups.[1].Value)
    else
        match extractInvokeParentName afterSlash with
        | Some simpleParentName -> matchByName calcLookup file simpleParentName
        | None -> None

/// Extract the simple method name from a fully-qualified method name.
let private extractSimpleMethodName (methodName: string) : string =
    let lastDot = methodName.LastIndexOf '.'

    if lastDot >= 0 then
        methodName.Substring(lastDot + 1)
    else
        methodName

/// Chain multiple fallback strategies to match a Coverlet method name to a source function.
let private tryMatchFallback
    (calcLookup: Map<(string * string), FunctionInfo list>)
    (analyzed: (string * FunctionInfo) list)
    (file: string)
    (className: string)
    (methodName: string)
    (simpleName: string)
    : string option =
    let strategies: (unit -> string option) list =
        [ fun () -> matchByName calcLookup file simpleName
          fun () -> matchByName calcLookup file methodName
          fun () -> matchAsClosure calcLookup file methodName
          fun () -> matchAsCtor calcLookup file className simpleName
          fun () -> matchAsInvoke calcLookup analyzed file className ]

    strategies |> List.tryPick (fun strat -> strat ())

/// Build a ReportItem from a matched source function name.
let private buildMethodReportItem
    (calcLookup: Map<(string * string), FunctionInfo list>)
    (cm: CoberturaMethod)
    (matchedName: string)
    : ReportItem =
    let fileShort = Path.GetFileName cm.File

    match Map.tryFind (cm.File, matchedName) calcLookup with
    | Some candidates when not (List.isEmpty candidates) ->
        let fi = pickByLineProximity cm.Line candidates

        { File = fileShort
          FunctionName = matchedName
          Line = fi.Line
          CalcCplx = fi.Complexity
          RefCplx = cm.RefCplx }
    | _ ->
        { File = fileShort
          FunctionName = matchedName
          Line = cm.Line
          CalcCplx = 0
          RefCplx = cm.RefCplx }

/// Try to match a single cobertura method to a calculated function.
let private tryMatchMethod
    (calcLookup: Map<(string * string), FunctionInfo list>)
    (analyzed: (string * FunctionInfo) list)
    (cm: CoberturaMethod)
    : ReportItem option =
    let simpleName = extractSimpleMethodName cm.Method

    tryMatchFallback calcLookup analyzed cm.File cm.Class cm.Method simpleName
    |> Option.map (buildMethodReportItem calcLookup cm)

/// Validate that the coverage file exists, printing error and exiting if not.
let private validateCoverageFile (quiet: bool) (coverageFile: string) : unit =
    if not (File.Exists coverageFile) then
        if not quiet then
            eprintfn "[complexity] Coverage file not found: %s" coverageFile
            eprintfn "[complexity] Run 'dotnet test' first to generate coverage data."

        exit 1

/// Validate that source files list is not empty, printing error and exiting if so.
let private validateSourceFilesNotEmpty (quiet: bool) (sourceFiles: string list) : unit =
    if List.isEmpty sourceFiles then
        if not quiet then
            eprintfn "[complexity] No source files found. Check the <sources> path in cobertura XML."

        exit 1

/// Analyze a single source file and return (file, FunctionInfo) pairs.
let private analyzeSingleSource (file: string) : (string * FunctionInfo)[] =
    if File.Exists file then
        analyzeFile file |> List.map (fun f -> (file, f)) |> List.toArray
    else
        [||]

/// Analyze all source files in parallel, returning (file, FunctionInfo) pairs.
let private analyzeAllSources (sourceFiles: string list) : (string * FunctionInfo) list =
    sourceFiles
    |> List.toArray
    |> Array.Parallel.collect analyzeSingleSource
    |> Array.toList

/// Build a lookup map: (file, simple method name) -> FunctionInfo list.
let private buildCalcLookup (analyzed: (string * FunctionInfo) list) : Map<(string * string), FunctionInfo list> =
    analyzed
    |> List.groupBy (fun (file, f) -> (file, f.Name))
    |> List.map (fun ((file, name), group) -> (file, name), (group |> List.map snd))
    |> Map.ofList

/// Match all cobertura methods against analyzed source functions,
/// returning (matched items, unmatched methods).
let private matchAllMethods
    (calcLookup: Map<(string * string), FunctionInfo list>)
    (analyzed: (string * FunctionInfo) list)
    (coberturaMethods: CoberturaMethod list)
    : ReportItem list * CoberturaMethod list =
    let matched, unmatched =
        coberturaMethods
        |> List.map (fun cm -> cm, tryMatchMethod calcLookup analyzed cm)
        |> List.partition (fun (_, result) -> result.IsSome)

    let items = matched |> List.map (fun (_, result) -> result.Value)
    let unmatchedMethods = unmatched |> List.map fst
    items, unmatchedMethods

/// Run the full analysis pipeline: parse cobertura, analyze sources, match
/// methods, generate report. Returns (errors, warnings) lists.
let private runAnalysis (args: Args) : ReportItem list * ReportItem list =
    let coverageFile = args.CoverageFile
    let threshold = args.Threshold
    let warnThreshold = args.WarnThreshold
    let quiet = args.Quiet

    validateCoverageFile quiet coverageFile

    // Parse cobertura
    let coberturaMethods, sourceFiles = parseCobertura coverageFile
    validateSourceFilesNotEmpty quiet sourceFiles

    // Analyze all source files referenced in cobertura (paths already resolved in parseCobertura).
    // Uses parallel processing since file analysis is CPU-bound and independent per file.
    let analyzed = analyzeAllSources sourceFiles

    // Build lookup: (file, simple method name) -> FunctionInfo list
    let calcLookup = buildCalcLookup analyzed

    let items, unmatched = matchAllMethods calcLookup analyzed coberturaMethods

    let matchedCount = items.Length
    let totalCount = coberturaMethods.Length

    if args.Verbose && not quiet then
        printMatchStatistics totalCount matchedCount items calcLookup unmatched

    generateReport quiet threshold warnThreshold (deduplicateItems items)

// ===========================================================================
// Entry point
// ===========================================================================

let args = fsi.CommandLineArgs |> Array.skip 1

match parseArgs Args.Default (List.ofArray args) with
| ShowHelp ->
    printHelp ()
    exit 0
| Parsed args' ->
    let errors, warnings = runAnalysis args'

    // -----------------------------------------------------------------------
    // Exit code
    // -----------------------------------------------------------------------

    if not (List.isEmpty errors) then
        if not args'.Quiet then
            printfn "  ❌ FAILED: %d method(s) exceed complexity threshold %d." (List.length errors) args'.Threshold

        exit 1
    else
        if not args'.Quiet then
            printfn "  ✅ PASSED."

        exit 0
