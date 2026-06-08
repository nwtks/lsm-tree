module LsmTree.Tests.SSTableTests

open Xunit
open LsmTree

[<Fact>]
let ``SSTable level parsing prefers L0 over L1`` () =
    let testDataDir = getTestDir "sst_levels"
    let l1Path = System.IO.Path.Combine(testDataDir, "L1_data.sst")
    let l0Path = System.IO.Path.Combine(testDataDir, "L0_data.sst")
    let legacyPath = System.IO.Path.Combine(testDataDir, "legacy.sst")
    SSTableWriter.write l1Path [ "k1", 1L, Some "v1_L1" ] |> ignore
    SSTableWriter.write l0Path [ "k1", 200L, Some "v1_L0" ] |> ignore
    SSTableWriter.write legacyPath [ "k9", 10L, Some "v9" ] |> ignore

    use tree = new LsmTree(testDataDir)

    assertEqual
        (Some "v1_L0")
        (tree.Get("k1", 300L))
        "Should prefer L0 over L1 (using high snapshot for manual recovery)"

    assertEqual (Some "v9") (tree.Get("k9", 300L)) "legacy.sst should be at level 0"

[<Fact>]
let ``SSTable double dispose does not throw`` () =
    let testDataDir = getTestDir "sst_double_dispose"
    let sstPath = System.IO.Path.Combine(testDataDir, "double_dispose.sst")
    SSTableWriter.write sstPath [] |> ignore

    use sst = new SSTable(sstPath)
    (sst :> System.IDisposable).Dispose()

    (sst :> System.IDisposable).Dispose()
    Assert.True(true, "Should not throw")

[<Fact>]
let ``SSTable handles short or invalid files gracefully`` () =
    let testDataDir = getTestDir "sst_short"
    let sstPath = System.IO.Path.Combine(testDataDir, "L0_short.sst")
    System.IO.File.WriteAllBytes(sstPath, [| 1uy; 2uy; 3uy |])

    use sst = new SSTable(sstPath)
    assertEqual None (sst.Get("any", 0L)) "Should handle short/invalid SSTable file gracefully"

[<Fact>]
let ``SSTable invalid magic number throws InvalidDataException`` () =
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
