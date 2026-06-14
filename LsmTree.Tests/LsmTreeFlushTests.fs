module LsmTree.Tests.LsmTreeFlushTests

open Xunit
open LsmTree

[<Fact>]
let ``findMinKey returns None for all-None array`` () =
    let current: (string * int64 * string option) option[] = [| None; None |]
    let result = LsmTreeFlush.findMinKey current
    assertEqual None result "All-None array -> None"

[<Fact>]
let ``findMinKey returns key for single entry`` () =
    let current: (string * int64 * string option) option[] =
        [| Some("b", 1L, Some "v") |]

    let result = LsmTreeFlush.findMinKey current
    assertEqual (Some "b") result "Single entry -> its key"

[<Fact>]
let ``findMinKey returns lexicographically smallest key`` () =
    let current: (string * int64 * string option) option[] =
        [| Some("z", 3L, Some "z"); Some("a", 1L, Some "a"); Some("m", 2L, Some "m") |]

    let result = LsmTreeFlush.findMinKey current
    assertEqual (Some "a") result "Lexicographically smallest among entries"

[<Fact>]
let ``findMinKey ignores None slots`` () =
    let current: (string * int64 * string option) option[] =
        [| None; Some("c", 1L, Some "c"); None |]

    let result = LsmTreeFlush.findMinKey current
    assertEqual (Some "c") result "Only key among None slots"

[<Fact>]
let ``findMinKey empty array returns None`` () =
    let current: (string * int64 * string option) option[] = [||]
    let result = LsmTreeFlush.findMinKey current
    assertEqual None result "Empty array -> None"

[<Fact>]
let ``collectVersions yields all versions for the given key in order`` () =
    let tableData =
        [| [| "a", 1L, Some "a1"; "c", 3L, Some "c1" |]
           [| "b", 2L, Some "b1"; "b", 4L, Some "b2" |]
           [| "b", 5L, Some "b3" |] |]

    let mutable pos = [| 0; 0; 0 |]

    let advance i = pos.[i] <- pos.[i] + 1

    let entryAt i =
        if pos.[i] < tableData.[i].Length then
            Some tableData.[i].[pos.[i]]
        else
            None

    let current = Array.init 3 entryAt
    let versions = LsmTreeFlush.collectVersions advance current "b"
    let result = versions |> Seq.toList
    let expected = [ 2L, Some "b1"; 4L, Some "b2"; 5L, Some "b3" ]
    assertEqual expected result "All versions of 'b' across tables"

[<Fact>]
let ``collectVersions returns empty when key not present`` () =
    let tableData = [| [| "a", 1L, Some "a1" |]; [| "c", 2L, Some "c1" |] |]

    let mutable pos = [| 0; 0 |]

    let advance i = pos.[i] <- pos.[i] + 1

    let entryAt i =
        if pos.[i] < tableData.[i].Length then
            Some tableData.[i].[pos.[i]]
        else
            None

    let current = Array.init 2 entryAt
    let versions = LsmTreeFlush.collectVersions advance current "missing"
    let result = versions |> Seq.toList
    assertEqual [] result "Non-existent key -> empty"

[<Fact>]
let ``collectVersions includes tombstone version`` () =
    let tableData = [| [| "k", 1L, None; "k", 2L, Some "v" |] |]
    let mutable pos = [| 0 |]

    let advance i = pos.[i] <- pos.[i] + 1

    let entryAt i =
        if pos.[i] < tableData.[i].Length then
            Some tableData.[i].[pos.[i]]
        else
            None

    let current = Array.init 1 entryAt
    let versions = LsmTreeFlush.collectVersions advance current "k"
    let result = versions |> Seq.toList
    let expected = [ 1L, None; 2L, Some "v" ]
    assertEqual expected result "Tombstone and value both collected"

[<Fact>]
let ``pruneVersions keeps all versions above minSnap`` () =
    let versions = ResizeArray<int64 * string option>()
    versions.Add(10L, Some "v1")
    versions.Add(20L, Some "v2")
    versions.Add(30L, Some "v3")

    let result = LsmTreeFlush.pruneVersions false 15L versions
    let expected = [ 30L, Some "v3"; 20L, Some "v2" ]
    assertEqual expected result "Versions with seq >= 15 kept"

[<Fact>]
let ``pruneVersions keeps newest older version as tombstone bridge`` () =
    let versions = ResizeArray<int64 * string option>()
    versions.Add(5L, Some "old")
    versions.Add(10L, Some "mid")
    versions.Add(20L, Some "new")

    let result = LsmTreeFlush.pruneVersions false 8L versions
    let expected = [ 20L, Some "new"; 10L, Some "mid"; 5L, Some "old" ]
    assertEqual expected result "Newest older version kept as tombstone bridge"

[<Fact>]
let ``pruneVersions removes tombstones at last level`` () =
    let versions = ResizeArray<int64 * string option>()
    versions.Add(5L, None)
    versions.Add(10L, Some "v1")
    versions.Add(20L, Some "v2")

    let result = LsmTreeFlush.pruneVersions true 8L versions
    let expected = [ 20L, Some "v2"; 10L, Some "v1" ]
    assertEqual expected result "Tombstone pruned at last level"

[<Fact>]
let ``pruneVersions keeps tombstone at non-last level`` () =
    let versions = ResizeArray<int64 * string option>()
    versions.Add(5L, None)

    let result = LsmTreeFlush.pruneVersions false 0L versions
    let expected = [ 5L, None ]
    assertEqual expected result "Tombstone kept at non-last level"

[<Fact>]
let ``pruneVersions all newer returns all`` () =
    let versions = ResizeArray<int64 * string option>()
    versions.Add(10L, Some "a")
    versions.Add(20L, Some "b")

    let result = LsmTreeFlush.pruneVersions false 5L versions
    let expected = [ 20L, Some "b"; 10L, Some "a" ]
    assertEqual expected result "All versions are newer than minSnap"

[<Fact>]
let ``pruneVersions keeps older live value at last level`` () =
    let versions = ResizeArray<int64 * string option>()
    versions.Add(5L, Some "old_live")
    versions.Add(10L, Some "newer")

    let result = LsmTreeFlush.pruneVersions true 8L versions
    let expected = [ 10L, Some "newer"; 5L, Some "old_live" ]
    assertEqual expected result "Live older value kept even at last level"

[<Fact>]
let ``pruneVersions empty list returns empty`` () =
    let versions = ResizeArray<int64 * string option>()
    let result = LsmTreeFlush.pruneVersions false 0L versions
    assertEqual [] result "Empty versions -> empty"

[<Fact>]
let ``mergeSortedEntries merges non-overlapping keys in order`` () =
    let tableData =
        [| [| "a", 2L, Some "a2"; "c", 4L, Some "c1" |]
           [| "b", 3L, Some "b1"; "d", 5L, Some "d1" |] |]

    let result =
        LsmTreeFlush.mergeSortedEntriesData tableData false System.Int64.MaxValue
        |> Seq.toList

    let expected =
        [ "a", 2L, Some "a2"
          "b", 3L, Some "b1"
          "c", 4L, Some "c1"
          "d", 5L, Some "d1" ]

    assertEqual expected result "Non-overlapping keys merged in sorted order"

[<Fact>]
let ``mergeSortedEntries picks highest seq for duplicate keys`` () =
    let tableData =
        [| [| "k", 1L, Some "v1" |]
           [| "k", 3L, Some "v3" |] |]

    let result =
        LsmTreeFlush.mergeSortedEntriesData tableData false System.Int64.MaxValue
        |> Seq.toList

    let expected = [ "k", 3L, Some "v3"; "k", 1L, Some "v1" ]
    assertEqual expected result "Higher seq wins, lower seq kept as older version"

[<Fact>]
let ``mergeSortedEntries with isLastLevel prunes tombstones`` () =
    let tableData =
        [| [| "k", 1L, Some "v1"; "k", 2L, None; "k", 3L, Some "v3" |] |]

    let result = LsmTreeFlush.mergeSortedEntriesData tableData true 0L |> Seq.toList

    let expected = [ "k", 3L, Some "v3"; "k", 1L, Some "v1" ]
    assertEqual expected result "Last level prunes tombstones, keeps only live values"

[<Fact>]
let ``mergeSortedEntries snapshot pruning preserves older version`` () =
    let tableData =
        [| [| "k", 1L, Some "v1"; "k", 10L, Some "v10" |] |]

    let result = LsmTreeFlush.mergeSortedEntriesData tableData false 5L |> Seq.toList

    let expected = [ "k", 10L, Some "v10"; "k", 1L, Some "v1" ]
    assertEqual expected result "Older version kept as tombstone bridge"
