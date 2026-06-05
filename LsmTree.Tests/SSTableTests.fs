module LsmTree.Tests.SSTableTests

open Xunit
open LsmTree

[<Fact>]
let ``SSTable_Level_Parsing_and_Recovery_Ordering`` () =
    let testDataDir = getTestDir "sst_levels"
    let l1Path = System.IO.Path.Combine(testDataDir, "L1_data.sst")
    let l0Path = System.IO.Path.Combine(testDataDir, "L0_data.sst")
    let legacyPath = System.IO.Path.Combine(testDataDir, "legacy.sst")
    SSTableWriter.write l1Path [ "k1", 1L, Some "v1_L1" ]
    SSTableWriter.write l0Path [ "k1", 200L, Some "v1_L0" ]
    SSTableWriter.write legacyPath [ "k9", 10L, Some "v9" ]

    use tree = new LsmTree(testDataDir)

    assertEqual
        (Some "v1_L0")
        (tree.Get("k1", 300L))
        "Should prefer L0 over L1 (using high snapshot for manual recovery)"

    assertEqual (Some "v9") (tree.Get("k9", 300L)) "legacy.sst should be at level 0"

[<Fact>]
let ``SSTable_Double_Dispose`` () =
    let testDataDir = getTestDir "sst_double_dispose"
    let sstPath = System.IO.Path.Combine(testDataDir, "double_dispose.sst")
    SSTableWriter.flush sstPath [] |> ignore

    use sst = new SSTable(sstPath)
    (sst :> System.IDisposable).Dispose()

    (sst :> System.IDisposable).Dispose()
    Assert.True(true, "Should not throw")

[<Fact>]
let ``SSTable_Load_Short_File_Handling`` () =
    let testDataDir = getTestDir "sst_short"
    let sstPath = System.IO.Path.Combine(testDataDir, "L0_short.sst")
    System.IO.File.WriteAllBytes(sstPath, [| 1uy; 2uy; 3uy |])

    use sst = new SSTable(sstPath)
    assertEqual None (sst.Get("any", 0L)) "Should handle short/invalid SSTable file gracefully"

[<Fact>]
let ``SSTable_Invalid_Magic`` () =
    let testDataDir = getTestDir "sst_bad_magic"
    let sstPath = System.IO.Path.Combine(testDataDir, "bad.sst")

    do
        use fs =
            new System.IO.FileStream(sstPath, System.IO.FileMode.Create, System.IO.FileAccess.Write)

        use bw = new System.IO.BinaryWriter(fs)
        bw.Write [| 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy |] // data
        bw.Write 0L // index offset
        bw.Write 0L // bloom offset
        bw.Write 0xFEEDFACEL // bad magic

    Assert.Throws<System.IO.InvalidDataException>(fun () -> new SSTable(sstPath) |> ignore)
