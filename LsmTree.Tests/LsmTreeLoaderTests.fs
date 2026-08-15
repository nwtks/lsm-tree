module LsmTree.Tests.LsmTreeLoaderTests

open Xunit
open LsmTree

[<Fact>]
let ``LsmTreeLoader validateCompactLevelLimits throws for empty array`` () =
    Assert.Throws<System.ArgumentException>(fun () -> LsmTreeLoader.validateCompactLevelLimits [||] |> ignore)

[<Fact>]
let ``LsmTreeLoader validateCompactLevelLimits throws for negative values`` () =
    Assert.Throws<System.ArgumentException>(fun () ->
        LsmTreeLoader.validateCompactLevelLimits [| 4; -1; 100 |] |> ignore)

[<Fact>]
let ``LsmTreeLoader parseSstLevel parses L0 prefix as level 0`` () =
    assertEqual 0 (LsmTreeLoader.parseSstLevel "L0_data.sst") "L0 prefix -> 0"

[<Fact>]
let ``LsmTreeLoader parseSstLevel parses L1 prefix as level 1`` () =
    assertEqual 1 (LsmTreeLoader.parseSstLevel "L1_data.sst") "L1 prefix -> 1"

[<Fact>]
let ``LsmTreeLoader parseSstLevel parses L10 prefix as level 10`` () =
    assertEqual 10 (LsmTreeLoader.parseSstLevel "L10_data.sst") "L10 prefix -> 10"

[<Fact>]
let ``LsmTreeLoader parseSstLevel returns 0 for files without L prefix`` () =
    assertEqual 0 (LsmTreeLoader.parseSstLevel "legacy.sst") "No L prefix -> 0"

[<Fact>]
let ``LsmTreeLoader parseSstLevel returns 0 for files with lowercase l prefix`` () =
    assertEqual 0 (LsmTreeLoader.parseSstLevel "l0_data.sst") "Lowercase l prefix -> 0"

[<Fact>]
let ``LsmTreeLoader parseSstLevel handles path with directory prefix`` () =
    assertEqual 2 (LsmTreeLoader.parseSstLevel "/some/dir/L2_data.sst") "Full path with L2 -> 2"

[<Fact>]
let ``LsmTreeLoader parseSstLevel treats non-numeric level as level 0`` () =
    assertEqual 0 (LsmTreeLoader.parseSstLevel "Lx_1.sst") "Non-numeric level -> 0"

[<Fact>]
let ``LsmTreeLoader parseSstLevel treats underscore after L without digits as level 0`` () =
    assertEqual 0 (LsmTreeLoader.parseSstLevel "L_1.sst") "Underscore immediately after L -> 0"

[<Fact>]
let ``LsmTreeLoader loadSSTableFiles loads tables at configured levels`` () =
    withTestDir "load_sst_in_range" (fun testDir ->
        let p0 = System.IO.Path.Combine(testDir, "L0_a.sst")
        let p1 = System.IO.Path.Combine(testDir, "L1_a.sst")
        SSTableWriter.write p0 [ "k0", 5L, Some "v0" ] |> ignore
        SSTableWriter.write p1 [ "k1", 10L, Some "v1" ] |> ignore

        let ssTables = Array.init 2 (fun _ -> list<SSTable>.Empty)
        let maxSeq = LsmTreeLoader.loadSSTableFiles testDir ssTables

        assertEqual 10L maxSeq "maxSeq is the highest across loaded tables"
        assertEqual 1 ssTables.[0].Length "one table loaded at L0"
        assertEqual 1 ssTables.[1].Length "one table loaded at L1")

[<Theory>]
[<InlineData("L2_orphan.sst", 2, 2)>]
[<InlineData("L100_orphan.sst", 3, 100)>]
let ``LsmTreeLoader loadSSTableFiles throws when a table exceeds the configured level count``
    (fileName: string)
    (levelCount: int)
    (fileLevel: int)
    =
    withTestDir "load_sst_out_of_range" (fun testDir ->
        let path = System.IO.Path.Combine(testDir, fileName)
        SSTableWriter.write path [ "k", 1L, Some "v" ] |> ignore

        let ssTables = Array.init levelCount (fun _ -> list<SSTable>.Empty)

        let ex =
            Assert.Throws<System.IO.InvalidDataException>(fun () ->
                LsmTreeLoader.loadSSTableFiles testDir ssTables |> ignore)

        Assert.True(ex.Message.Contains fileName, "message names the offending file")
        Assert.True(ex.Message.Contains(string fileLevel), "message states the required compactLevelLimits length"))

[<Fact>]
let ``LsmTreeLoader compareSSTables orders by MaxSeq descending then by path name`` () =
    withTestDir "cmp_sst" (fun testDir ->
        let p1 = System.IO.Path.Combine(testDir, "L0_a.sst")
        let p2 = System.IO.Path.Combine(testDir, "L0_b.sst")
        SSTableWriter.write p1 [ "k", 10L, Some "v" ] |> ignore
        SSTableWriter.write p2 [ "k", 20L, Some "v" ] |> ignore

        use sst1 = new SSTable(p1)
        use sst2 = new SSTable(p2)

        let cmp = LsmTreeLoader.compareSSTables sst1 sst2
        Assert.True(cmp > 0, "sst1 (MaxSeq=10) should compare after sst2 (MaxSeq=20)"))

[<Fact>]
let ``LsmTreeLoader compareSSTables breaks ties by path lexicographic order`` () =
    withTestDir "cmp_sst_tie" (fun testDir ->
        let p1 = System.IO.Path.Combine(testDir, "L0_a.sst")
        let p2 = System.IO.Path.Combine(testDir, "L0_b.sst")
        SSTableWriter.write p1 [ "k", 10L, Some "v" ] |> ignore
        SSTableWriter.write p2 [ "k", 10L, Some "v" ] |> ignore

        use sst1 = new SSTable(p1)
        use sst2 = new SSTable(p2)

        let cmp = LsmTreeLoader.compareSSTables sst1 sst2
        Assert.True(cmp < 0, "Path 'a' should come before path 'b' when MaxSeq ties"))
