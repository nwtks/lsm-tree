module LsmTree.Tests.SSTableTests

open Xunit
open LsmTree

let writeSst dataDir name entries =
    let path = System.IO.Path.Combine(dataDir, name)
    SSTableWriter.write path entries |> ignore
    path

let writeRawSst dataDir name (action: System.IO.BinaryWriter -> int64 -> int64 -> unit) =
    let path = System.IO.Path.Combine(dataDir, name)

    do
        use fs =
            new System.IO.FileStream(path, System.IO.FileMode.Create, System.IO.FileAccess.Write)

        use bw = new System.IO.BinaryWriter(fs)
        bw.Write 1L // seq
        bw.Write 2 // key length
        bw.Write "k1"B // key
        bw.Write false // has value
        bw.Write 2 // value length
        bw.Write "v1"B // value
        let dataEnd = fs.Position
        bw.Write 1 // count
        bw.Write 0L // offset of the single entry
        let bloomPos = fs.Position
        bw.Write 1 // byte count
        bw.Write 0uy // bloom byte
        action bw dataEnd bloomPos
        fs.Flush true

    path

[<Fact>]
let ``SSTable short file below footer size handles gracefully`` () =
    let testDataDir = getTestDir "sst_short_file"
    let path = System.IO.Path.Combine(testDataDir, "short.sst")

    System.IO.File.WriteAllBytes(path, [| 0uy .. 9uy |])

    use sst = new SSTable(path)
    assertEqual 0 sst.Count "Short file should have 0 entries"
    assertEqual None (sst.Get("any", 0L)) "Short file Get should return None"
    assertEqual [||] (sst.GetAll()) "Short file GetAll should return empty array"

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

[<Fact>]
let ``SSTable index offset out of range throws`` () =
    let testDataDir = getTestDir "sst_idx_ofs"

    writeRawSst testDataDir "L0_bad_idx_ofs.sst" (fun bw _dataEnd bloomPos ->
        bw.Write(bloomPos + 9999L)
        bw.Write bloomPos
        bw.Write 0L // maxSeq
        bw.Write SSTable.MAGIC)
    |> ignore

    Assert.Throws<System.IO.InvalidDataException>(fun () ->
        new SSTable(System.IO.Path.Combine(testDataDir, "L0_bad_idx_ofs.sst")) |> ignore)

[<Fact>]
let ``SSTable bloom offset before index offset throws`` () =
    let testDataDir = getTestDir "sst_bloom_before_idx"

    writeRawSst testDataDir "L0_bloom_before_idx.sst" (fun bw dataEnd _bloomPos ->
        bw.Write dataEnd
        bw.Write 0L
        bw.Write 0L // maxSeq
        bw.Write SSTable.MAGIC)
    |> ignore

    Assert.Throws<System.IO.InvalidDataException>(fun () ->
        new SSTable(System.IO.Path.Combine(testDataDir, "L0_bloom_before_idx.sst"))
        |> ignore)

[<Fact>]
let ``SSTable negative index entry count throws`` () =
    let testDataDir = getTestDir "sst_neg_count"
    let path = System.IO.Path.Combine(testDataDir, "L0_neg_count.sst")

    do
        use fs =
            new System.IO.FileStream(path, System.IO.FileMode.Create, System.IO.FileAccess.Write)

        use bw = new System.IO.BinaryWriter(fs)
        bw.Write 1L // seq
        bw.Write 2 // key length
        bw.Write "k1"B // key
        bw.Write false // has value
        bw.Write 2 // value length
        bw.Write "v1"B // value

        let indexPos = fs.Position
        bw.Write -1 // negative count!
        bw.Write 0L // dummy offset

        let bloomPos = fs.Position
        bw.Write 1 // byte count
        bw.Write 0uy // bloom byte

        bw.Write indexPos // indexOffset
        bw.Write bloomPos // bloomOffset
        bw.Write 0L // maxSeq
        bw.Write SSTable.MAGIC

    Assert.Throws<System.IO.InvalidDataException>(fun () -> new SSTable(path) |> ignore)

[<Fact>]
let ``SSTable index entry count exceeds remaining space throws`` () =
    let testDataDir = getTestDir "sst_count_overflow"
    let path = System.IO.Path.Combine(testDataDir, "L0_count_overflow.sst")

    do
        use fs =
            new System.IO.FileStream(path, System.IO.FileMode.Create, System.IO.FileAccess.Write)

        use bw = new System.IO.BinaryWriter(fs)
        bw.Write 1L
        bw.Write 2
        bw.Write "k1"B
        bw.Write false
        bw.Write 2
        bw.Write "v1"B

        let indexPos = fs.Position
        bw.Write 100000

        for _ = 0 to 9999 do
            bw.Write 0L // write 10k offsets to make file large

        let bloomPos = fs.Position
        bw.Write 1
        bw.Write 0uy

        bw.Write indexPos
        bw.Write bloomPos
        bw.Write 0L // maxSeq
        bw.Write SSTable.MAGIC

    Assert.Throws<System.IO.InvalidDataException>(fun () -> new SSTable(path) |> ignore)

[<Fact>]
let ``SSTable entry offset out of range throws`` () =
    let testDataDir = getTestDir "sst_entry_ofs"
    let path = System.IO.Path.Combine(testDataDir, "L0_entry_ofs.sst")

    do
        use fs =
            new System.IO.FileStream(path, System.IO.FileMode.Create, System.IO.FileAccess.Write)

        use bw = new System.IO.BinaryWriter(fs)
        bw.Write 1L
        bw.Write 2
        bw.Write "k1"B
        bw.Write false
        bw.Write 2
        bw.Write "v1"B

        let indexPos = fs.Position
        bw.Write 1
        bw.Write -1L // entry offset points before file start!

        let bloomPos = fs.Position
        bw.Write 1
        bw.Write 0uy

        bw.Write indexPos
        bw.Write bloomPos
        bw.Write 0L // maxSeq
        bw.Write SSTable.MAGIC

    Assert.Throws<System.IO.InvalidDataException>(fun () -> new SSTable(path) |> ignore)

[<Fact>]
let ``SSTable entry offset at index position throws`` () =
    let testDataDir = getTestDir "sst_entry_at_idx"
    let path = System.IO.Path.Combine(testDataDir, "L0_entry_at_idx.sst")

    do
        use fs =
            new System.IO.FileStream(path, System.IO.FileMode.Create, System.IO.FileAccess.Write)

        use bw = new System.IO.BinaryWriter(fs)
        bw.Write 1L
        bw.Write 2
        bw.Write "k1"B
        bw.Write false
        bw.Write 2
        bw.Write "v1"B

        let indexPos = fs.Position
        bw.Write 1
        bw.Write indexPos // entry offset equals indexPos — should trigger >= offset check

        let bloomPos = fs.Position
        bw.Write 1
        bw.Write 0uy

        bw.Write indexPos
        bw.Write bloomPos
        bw.Write 0L // maxSeq
        bw.Write SSTable.MAGIC

    Assert.Throws<System.IO.InvalidDataException>(fun () -> new SSTable(path) |> ignore)

[<Fact>]
let ``SSTable negative bloom byte count throws`` () =
    let testDataDir = getTestDir "sst_neg_bloom"
    let path = System.IO.Path.Combine(testDataDir, "L0_neg_bloom.sst")

    do
        use fs =
            new System.IO.FileStream(path, System.IO.FileMode.Create, System.IO.FileAccess.Write)

        use bw = new System.IO.BinaryWriter(fs)
        bw.Write 1L
        bw.Write 2
        bw.Write "k1"B
        bw.Write false
        bw.Write 2
        bw.Write "v1"B

        let indexPos = fs.Position
        bw.Write 1
        bw.Write 0L

        let bloomPos = fs.Position
        bw.Write -1 // negative bloom byte count!
        bw.Write 0uy

        bw.Write indexPos
        bw.Write bloomPos
        bw.Write 0L // maxSeq
        bw.Write SSTable.MAGIC

    Assert.Throws<System.IO.InvalidDataException>(fun () -> new SSTable(path) |> ignore)

[<Fact>]
let ``SSTable bloom byte count exceeds remaining space throws`` () =
    let testDataDir = getTestDir "sst_bloom_overflow"
    let path = System.IO.Path.Combine(testDataDir, "L0_bloom_overflow.sst")

    do
        use fs =
            new System.IO.FileStream(path, System.IO.FileMode.Create, System.IO.FileAccess.Write)

        use bw = new System.IO.BinaryWriter(fs)
        bw.Write 1L
        bw.Write 2
        bw.Write "k1"B
        bw.Write false
        bw.Write 2
        bw.Write "v1"B

        let indexPos = fs.Position
        bw.Write 1
        bw.Write 0L

        let bloomPos = fs.Position
        bw.Write 10000

        bw.Write indexPos
        bw.Write bloomPos
        bw.Write 0L // maxSeq
        bw.Write SSTable.MAGIC

    Assert.Throws<System.IO.InvalidDataException>(fun () -> new SSTable(path) |> ignore)

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

    use sst = new SSTable(sstPath)
    (sst :> System.IDisposable).Dispose()

    (sst :> System.IDisposable).Dispose()
    Assert.True(true, "Should not throw")

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
