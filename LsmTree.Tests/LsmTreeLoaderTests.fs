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
let ``LsmTreeLoader compareSSTables orders by MaxSeq descending then by path`` () =
    let testDir = getTestDir "cmp_sst"
    let p1 = System.IO.Path.Combine(testDir, "L0_a.sst")
    let p2 = System.IO.Path.Combine(testDir, "L0_b.sst")
    SSTableWriter.write p1 [ "k", 10L, Some "v" ] |> ignore
    SSTableWriter.write p2 [ "k", 20L, Some "v" ] |> ignore

    use sst1 = new SSTable(p1)
    use sst2 = new SSTable(p2)

    let cmp = LsmTreeLoader.compareSSTables sst1 sst2
    Assert.True(cmp > 0, "sst1 (MaxSeq=10) should compare after sst2 (MaxSeq=20)")

[<Fact>]
let ``LsmTreeLoader compareSSTables ties broken by path`` () =
    let testDir = getTestDir "cmp_sst_tie"
    let p1 = System.IO.Path.Combine(testDir, "L0_a.sst")
    let p2 = System.IO.Path.Combine(testDir, "L0_b.sst")
    SSTableWriter.write p1 [ "k", 10L, Some "v" ] |> ignore
    SSTableWriter.write p2 [ "k", 10L, Some "v" ] |> ignore

    use sst1 = new SSTable(p1)
    use sst2 = new SSTable(p2)

    let cmp = LsmTreeLoader.compareSSTables sst1 sst2
    Assert.True(cmp < 0, "Path 'a' should come before path 'b' when MaxSeq ties")
