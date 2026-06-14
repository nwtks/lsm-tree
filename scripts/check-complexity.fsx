#!/usr/bin/env dotnet fsi
// check-complexity.fsx
//
// Parses a coverlet coverage.cobertura.xml file and reports methods
// whose cyclomatic complexity exceeds the specified threshold.
//
// Usage:
//   dotnet fsi scripts/check-complexity.fsx --coverage-file <path> [--threshold <n>] [--warn-threshold <n>]
//
// Defaults:
//   --coverage-file coverage.cobertura.xml
//   --threshold      15   (error level)
//   --warn-threshold 10   (warning level)

open System.IO
open System.Xml.Linq

// ---------------------------------------------------------------------------
// Argument parsing
// ---------------------------------------------------------------------------

type Args =
    { CoverageFile: string
      Threshold: int
      WarnThreshold: int
      Quiet: bool }

    static member Default =
        { CoverageFile = "coverage.cobertura.xml"
          Threshold = 15
          WarnThreshold = 10
          Quiet = false }

let args = fsi.CommandLineArgs |> Array.skip 1

let rec parseArgs acc =
    function
    | "--coverage-file" :: v :: rest -> parseArgs { acc with CoverageFile = v } rest
    | "--threshold" :: v :: rest -> parseArgs { acc with Threshold = int v } rest
    | "--warn-threshold" :: v :: rest -> parseArgs { acc with WarnThreshold = int v } rest
    | "--quiet" :: rest -> parseArgs { acc with Quiet = true } rest
    | _ :: rest -> parseArgs acc rest
    | [] -> acc

let args' = parseArgs Args.Default (List.ofArray args)

// ---------------------------------------------------------------------------
// XML Parsing
// ---------------------------------------------------------------------------

if not (File.Exists args'.CoverageFile) then
    if not args'.Quiet then
        eprintfn "[complexity] Coverage file not found: %s" args'.CoverageFile
        eprintfn "[complexity] Run 'dotnet test' first to generate coverage data."

    exit 0 // Not an error — just no data to check

let xname s = XName.Get s
let doc = XDocument.Load args'.CoverageFile

// Collect all methods with their complexity
let methods =
    [ for pkg in doc.Descendants(xname "package") do
          for cls in pkg.Descendants(xname "class") do
              let filename =
                  cls.Attribute(xname "filename") |> fun a -> if a = null then "?" else a.Value

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

                  yield
                      {| Complexity = complexity
                         File = filename
                         Class = className
                         Method = name |} ]

let sorted = methods |> List.sortByDescending (fun m -> m.Complexity)

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

let errors = sorted |> List.filter (fun m -> m.Complexity > args'.Threshold)

let warnings =
    sorted
    |> List.filter (fun m -> m.Complexity > args'.WarnThreshold && m.Complexity <= args'.Threshold)

if not args'.Quiet then
    printfn ""
    printfn "══════════════════════════════════════════════════════════════"
    printfn "  Cyclomatic Complexity Report"
    printfn "  Threshold: error > %d, warning > %d" args'.Threshold args'.WarnThreshold
    printfn "══════════════════════════════════════════════════════════════"

    if List.isEmpty methods then
        printfn "  (no methods found)"
    else
        printfn ""

        printfn
            "  %d methods analyzed | Max: %d | Avg: %.1f"
            (List.length methods)
            (if List.isEmpty sorted then
                 0
             else
                 (List.head sorted).Complexity)
            (float (sorted |> List.sumBy (fun m -> m.Complexity))
             / float (max 1 (List.length methods)))

        printfn ""

    if not (List.isEmpty errors) then
        printfn "  ❌ ERROR — Complexities above %d:" args'.Threshold
        printfn ""

        for m in errors do
            printfn "    %3d  %-30s  %s" m.Complexity m.File m.Method

        printfn ""

    if not (List.isEmpty warnings) then
        printfn "  ⚠️  WARNING — Complexities above %d:" args'.WarnThreshold
        printfn ""

        for m in warnings do
            printfn "    %3d  %-30s  %s" m.Complexity m.File m.Method

        printfn ""

    if List.isEmpty errors && List.isEmpty warnings then
        printfn "  ✅ All methods within complexity thresholds."
    else
        printfn "  Summary: %d error(s), %d warning(s)" (List.length errors) (List.length warnings)

// Top 10 listing
if not args'.Quiet && not (List.isEmpty sorted) then
    printfn ""
    printfn "  ── Top 10 complex methods ──"

    for m in sorted |> List.truncate 10 do
        let icon =
            if m.Complexity > args'.Threshold then "❌"
            elif m.Complexity > args'.WarnThreshold then "⚠️"
            else "✅"

        printfn "    %s %3d  %-30s  %s" icon m.Complexity m.File m.Method

    printfn ""

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
