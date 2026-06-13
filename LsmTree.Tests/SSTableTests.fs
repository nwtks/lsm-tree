module LsmTree.Tests.SSTableTests

open Xunit
open LsmTree

let writeSst dataDir name entries =
    let path = System.IO.Path.Combine(dataDir, name)
    SSTableWriter.write path entries |> ignore
    path

[<Fact>]
let ``SSTable validateSSTableMagic throws for invalid magic number`` () =
    Assert.Throws<System.IO.InvalidDataException>(fun () -> SSTable.validateSSTableMagic 0xFEEDFACEL)

[<Fact>]
let ``SSTable validateIndexOffset throws for out-of-range offset`` () =
    Assert.Throws<System.IO.InvalidDataException>(fun () -> SSTable.validateIndexOffset 100L 100L SSTable.FOOTER_SIZE)

[<Fact>]
let ``SSTable validateBloomOffset throws for bloom before index`` () =
    Assert.Throws<System.IO.InvalidDataException>(fun () ->
        SSTable.validateBloomOffset 100L 50L 30L SSTable.FOOTER_SIZE)

[<Fact>]
let ``SSTable validateIndexCount throws for negative count`` () =
    Assert.Throws<System.IO.InvalidDataException>(fun () -> SSTable.validateIndexCount 100L 50L -1 SSTable.FOOTER_SIZE)

[<Fact>]
let ``SSTable validateIndexCount throws when count exceeds remaining space`` () =
    Assert.Throws<System.IO.InvalidDataException>(fun () ->
        SSTable.validateIndexCount 100L 50L 100000 SSTable.FOOTER_SIZE)

[<Fact>]
let ``SSTable validateEntryOffsets throws for negative offset`` () =
    Assert.Throws<System.IO.InvalidDataException>(fun () -> SSTable.validateEntryOffsets 100L [| -1L |])

[<Fact>]
let ``SSTable validateEntryOffsets throws for offset at index position`` () =
    Assert.Throws<System.IO.InvalidDataException>(fun () -> SSTable.validateEntryOffsets 100L [| 100L |])

[<Fact>]
let ``SSTable validateBloomCount throws for negative byte count`` () =
    Assert.Throws<System.IO.InvalidDataException>(fun () -> SSTable.validateBloomCount 100L 60L -1 SSTable.FOOTER_SIZE)

[<Fact>]
let ``SSTable validateBloomCount throws when count exceeds remaining space`` () =
    Assert.Throws<System.IO.InvalidDataException>(fun () ->
        SSTable.validateBloomCount 100L 60L 10000 SSTable.FOOTER_SIZE)

[<Fact>]
let ``SSTable short file below footer size returns zero entries`` () =
    let testDataDir = getTestDir "sst_short_file"
    let path = System.IO.Path.Combine(testDataDir, "short.sst")
    System.IO.File.WriteAllBytes(path, [| 0uy .. 9uy |])

    use sst = new SSTable(path)
    assertEqual 0 sst.Count "Short file should have 0 entries"
    assertEqual None (sst.Get("any", 0L)) "Short file Get should return None"
    assertEqual [||] (sst.GetAll()) "Short file GetAll should return empty array"

[<Fact>]
let ``SSTable handles empty entries correctly`` () =
    let testDataDir = getTestDir "sst_empty"
    let path = writeSst testDataDir "L0_empty.sst" []

    use sst = new SSTable(path)
    assertEqual 0 sst.Count "Empty SSTable should have 0 entries"
    assertEqual None (sst.Get("any", 0L)) "Get on empty SSTable should return None"
    assertEqual [||] (sst.GetAll()) "GetAll on empty SSTable should return empty array"

[<Fact>]
let ``SSTable loadIndex handles tombstone entries correctly`` () =
    let testDataDir = getTestDir "sst_idx_tomb"
    let path = System.IO.Path.Combine(testDataDir, "idx_tomb.sst")
    SSTableWriter.write path [ "gone", 2L, None; "keep", 1L, Some "val" ] |> ignore

    use sst = new SSTable(path)
    assertEqual 2 sst.Count "Should have 2 entries"
    assertEqual (Some None) (sst.Get("gone", System.Int64.MaxValue)) "Tombstone entry returns Some None"
    assertEqual (Some(Some "val")) (sst.Get("keep", System.Int64.MaxValue)) "Regular entry returns Some(Some value)"

[<Fact>]
let ``SSTable GetAll preserves tombstone entries`` () =
    let testDataDir = getTestDir "sst_getall_tomb"
    let path = System.IO.Path.Combine(testDataDir, "getall_tomb.sst")

    SSTableWriter.write path [ "k1", 1L, Some "v1"; "k2", 2L, None; "k3", 3L, Some "v3" ]
    |> ignore

    use sst = new SSTable(path)
    let entries = sst.GetAll()
    assertEqual 3 entries.Length "Should have 3 entries"
    assertEqual ("k1", 1L, Some "v1") entries.[0] "First entry is k1=v1"
    assertEqual ("k2", 2L, None) entries.[1] "Second entry is k2=tombstone"
    assertEqual ("k3", 3L, Some "v3") entries.[2] "Third entry is k3=v3"

[<Fact>]
let ``SSTable Get returns None for missing key`` () =
    let testDataDir = getTestDir "sst_get_missing"
    let path = writeSst testDataDir "has_data.sst" [ "present", 1L, Some "value" ]

    use sst = new SSTable(path)
    assertEqual None (sst.Get("missing", System.Int64.MaxValue)) "Get for missing key should return None"

    assertEqual
        (Some(Some "value"))
        (sst.Get("present", System.Int64.MaxValue))
        "Get for existing key should return Some(Some value)"

[<Fact>]
let ``SSTable double dispose does not throw`` () =
    let testDataDir = getTestDir "sst_double_dispose"
    let sstPath = System.IO.Path.Combine(testDataDir, "double_dispose.sst")
    SSTableWriter.write sstPath [] |> ignore

    let sst = new SSTable(sstPath)
    (sst :> System.IDisposable).Dispose()

    (sst :> System.IDisposable).Dispose()

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
