module LsmTree.Tests.SSTableTests

open Xunit
open LsmTree

let writeSst dataDir name entries =
    let path = System.IO.Path.Combine(dataDir, name)
    SSTableWriter.write path entries |> ignore
    path

[<Fact>]
let ``SSTable validateIndexOffset throws when offset is out of range`` () =
    Assert.Throws<System.IO.InvalidDataException>(fun () -> SSTable.validateIndexOffset 100L 100L SSTable.FOOTER_SIZE)

[<Fact>]
let ``SSTable validateBloomOffset throws when bloom offset precedes index offset`` () =
    Assert.Throws<System.IO.InvalidDataException>(fun () ->
        SSTable.validateBloomOffset 100L 50L 30L SSTable.FOOTER_SIZE)

[<Fact>]
let ``SSTable validateBloomOffset throws when offset exceeds file size`` () =
    Assert.Throws<System.IO.InvalidDataException>(fun () ->
        SSTable.validateBloomOffset 100L 10L 90L SSTable.FOOTER_SIZE)

[<Fact>]
let ``SSTable validateBloomOffset throws when offset is negative`` () =
    Assert.Throws<System.IO.InvalidDataException>(fun () ->
        SSTable.validateBloomOffset 100L 50L -1L SSTable.FOOTER_SIZE)

[<Fact>]
let ``SSTable validateBloomCount throws when byte count is negative`` () =
    Assert.Throws<System.IO.InvalidDataException>(fun () -> SSTable.validateBloomCount 100L 60L -1 SSTable.FOOTER_SIZE)

[<Fact>]
let ``SSTable validateBloomCount throws when count exceeds remaining space`` () =
    Assert.Throws<System.IO.InvalidDataException>(fun () ->
        SSTable.validateBloomCount 100L 60L 10000 SSTable.FOOTER_SIZE)

[<Fact>]
let ``SSTable validateSSTableMagic throws for invalid magic number`` () =
    Assert.Throws<System.IO.InvalidDataException>(fun () -> SSTable.validateSSTableMagic 0xFEEDFACEL)

[<Fact>]
let ``SSTable load returns empty for short file`` () =
    withTestDir "sst_short_file" (fun testDataDir ->
        let path = System.IO.Path.Combine(testDataDir, "short.sst")
        System.IO.File.WriteAllBytes(path, [| 0uy .. 9uy |])

        use fs =
            new System.IO.FileStream(
                path,
                System.IO.FileMode.Open,
                System.IO.FileAccess.Read,
                System.IO.FileShare.Read
            )

        use br = new System.IO.BinaryReader(fs)
        let _, maxSeq, _, index = SSTable.load fs br
        assertEqual [||] index "Short file should have empty index"
        assertEqual 0L maxSeq "Short file should have maxSeq 0")

[<Fact>]
let ``SSTable load and loadIndex handle empty SSTable`` () =
    withTestDir "sst_empty" (fun testDataDir ->
        let path = writeSst testDataDir "L0_empty.sst" []

        use fs =
            new System.IO.FileStream(
                path,
                System.IO.FileMode.Open,
                System.IO.FileAccess.Read,
                System.IO.FileShare.Read
            )

        use br = new System.IO.BinaryReader(fs)
        let _, maxSeq, _, index = SSTable.load fs br
        assertEqual [||] index "Empty SSTable should have empty index"
        assertEqual 0L maxSeq "Empty SSTable maxSeq = 0")

[<Fact>]
let ``SSTable readValue roundtrips correctly`` () =
    use ms = new System.IO.MemoryStream()
    use bw = new System.IO.BinaryWriter(ms)
    SSTableWriter.writeValue bw "roundtrip_val"
    bw.Flush()
    ms.Position <- 0L
    use br = new System.IO.BinaryReader(ms)
    assertEqual "roundtrip_val" (SSTable.readValue br) "readValue should roundtrip"

[<Fact>]
let ``SSTable readItem roundtrips Some value`` () =
    use ms = new System.IO.MemoryStream()
    use bw = new System.IO.BinaryWriter(ms)
    SSTableWriter.writeItem bw (Some "item_roundtrip")
    bw.Flush()
    ms.Position <- 0L
    use br = new System.IO.BinaryReader(ms)
    assertEqual (Some "item_roundtrip") (SSTable.readItem br) "readItem should roundtrip Some"

[<Fact>]
let ``SSTable readItem roundtrips None`` () =
    use ms = new System.IO.MemoryStream()
    use bw = new System.IO.BinaryWriter(ms)
    SSTableWriter.writeItem bw None
    bw.Flush()
    ms.Position <- 0L
    use br = new System.IO.BinaryReader(ms)
    assertEqual None (SSTable.readItem br) "readItem should roundtrip None"

[<Fact>]
let ``SSTable loadIndex handles tombstone and value entries`` () =
    withTestDir "sst_idx_tomb" (fun testDataDir ->
        let path = System.IO.Path.Combine(testDataDir, "idx_tomb.sst")
        SSTableWriter.write path [ "gone", 2L, None; "keep", 1L, Some "val" ] |> ignore

        use fs =
            new System.IO.FileStream(
                path,
                System.IO.FileMode.Open,
                System.IO.FileAccess.Read,
                System.IO.FileShare.Read
            )

        use br = new System.IO.BinaryReader(fs)
        let _, _, _, index = SSTable.load fs br
        assertEqual 2 index.Length "Should have 2 index entries"
        assertEqual "gone" index.[0].Key "First entry key"
        assertEqual 2L index.[0].Seq "First entry seq"
        assertEqual "keep" index.[1].Key "Second entry key"
        assertEqual 1L index.[1].Seq "Second entry seq")

[<Fact>]
let ``SSTable binSearchIndex finds existing key`` () =
    let index =
        [| { Key = "present"
             Seq = 1L
             Offset = 0L
             KeyByteLen = 7 } |]

    let result = SSTable.binSearchIndex index "present" System.Int64.MaxValue 0 0 None
    assertEqual (Some 0) result "Existing key returns correct index"

[<Fact>]
let ``SSTable binSearchIndex respects snapshot isolation`` () =
    let index =
        [| { Key = "k"
             Seq = 10L
             Offset = 0L
             KeyByteLen = 1 }
           { Key = "k"
             Seq = 5L
             Offset = 10L
             KeyByteLen = 1 } |]

    assertEqual None (SSTable.binSearchIndex index "k" 3L 0 1 None) "Snapshot below all seqs returns None"
    assertEqual (Some 1) (SSTable.binSearchIndex index "k" 7L 0 1 None) "Snapshot between seqs finds older version"
    assertEqual (Some 0) (SSTable.binSearchIndex index "k" 10L 0 1 None) "Snapshot at max seq finds newest"

[<Fact>]
let ``SSTable binSearchIndex returns None for missing key`` () =
    let index =
        [| { Key = "present"
             Seq = 1L
             Offset = 0L
             KeyByteLen = 7 } |]

    let result = SSTable.binSearchIndex index "missing" System.Int64.MaxValue 0 0 None
    assertEqual None result "Missing key returns None"

[<Fact>]
let ``SSTable readAllEntries preserves tombstone entries`` () =
    withTestDir "sst_getall_tomb" (fun testDataDir ->
        let path = System.IO.Path.Combine(testDataDir, "getall_tomb.sst")

        SSTableWriter.write path [ "k1", 1L, Some "v1"; "k2", 2L, None; "k3", 3L, Some "v3" ]
        |> ignore

        use fs =
            new System.IO.FileStream(
                path,
                System.IO.FileMode.Open,
                System.IO.FileAccess.Read,
                System.IO.FileShare.Read
            )

        use br = new System.IO.BinaryReader(fs)
        let _, _, _, index = SSTable.load fs br
        fs.Seek(index.[0].Offset, System.IO.SeekOrigin.Begin) |> ignore
        let entries = SSTable.readAllEntries br index.Length
        assertEqual 3 entries.Length "Should have 3 entries"
        assertEqual ("k1", 1L, Some "v1") entries.[0] "First entry is k1=v1"
        assertEqual ("k2", 2L, None) entries.[1] "Second entry is k2=tombstone"
        assertEqual ("k3", 3L, Some "v3") entries.[2] "Third entry is k3=v3")

[<Fact>]
let ``SSTable Get returns NotFound for empty SSTable`` () =
    withTestDir "sst_get_empty" (fun testDataDir ->
        let path = writeSst testDataDir "L0_empty.sst" []
        let sst = new SSTable(path)
        assertEqual NotFound (sst.Get("some_key", System.Int64.MaxValue)) "Get on empty SSTable returns NotFound"
        (sst :> System.IDisposable).Dispose())

[<Fact>]
let ``SSTable Get returns NotFound for missing key`` () =
    withTestDir "sst_get_missing" (fun testDataDir ->
        let path = writeSst testDataDir "L0_data.sst" [ "k", 1L, Some "v" ]
        let sst = new SSTable(path)
        let result = sst.Get("missing", System.Int64.MaxValue)
        assertEqual NotFound result "Get for missing key returns NotFound"
        (sst :> System.IDisposable).Dispose())

[<Fact>]
let ``SSTable Get returns NotFound for disposed SSTable`` () =
    withTestDir "sst_get_disposed" (fun testDataDir ->
        let path = writeSst testDataDir "L0_disposed.sst" [ "k", 1L, Some "v" ]
        let sst = new SSTable(path)
        assertEqual (Found "v") (sst.Get("k", System.Int64.MaxValue)) "Get before dispose returns value"
        (sst :> System.IDisposable).Dispose()

        assertEqual
            NotFound
            (sst.Get("k", System.Int64.MaxValue))
            "Get after dispose returns NotFound without throwing")

[<Fact>]
let ``SSTable GetAll reads data region in one batch and repeats identically`` () =
    withTestDir "sst_getall_all" (fun testDataDir ->
        let path =
            writeSst testDataDir "L0_getall.sst" [ "a", 1L, Some "va"; "b", 2L, None; "c", 3L, Some "vc" ]

        use sst = new SSTable(path)
        let first = sst.GetAll()
        let second = sst.GetAll()

        assertEqual 3 first.Length "GetAll returns 3 entries"
        assertEqual ("a", 1L, Some "va") first.[0] "First entry is a"
        assertEqual ("b", 2L, None) first.[1] "Second entry is tombstone"
        assertEqual ("c", 3L, Some "vc") first.[2] "Third entry is c"
        assertEqual first second "Repeated GetAll returns identical results")

[<Fact>]
let ``SSTable GetRange returns entries within range`` () =
    withTestDir "sst_range_basic" (fun testDataDir ->
        let path =
            writeSst
                testDataDir
                "L0_range.sst"
                [ "a", 1L, Some "va"
                  "b", 2L, Some "vb"
                  "c", 3L, Some "vc"
                  "d", 4L, Some "vd" ]

        use sst = new SSTable(path)

        match sst.GetRange("b", "c") with
        | RangeOk result ->
            assertEqual 2 result.Length "Two entries in [b,c]"
            assertEqual ("b", 2L, Some "vb") result.[0] "First entry is b"
            assertEqual ("c", 3L, Some "vc") result.[1] "Second entry is c"
        | RangeDisposed -> failwith "unexpected RangeDisposed")

[<Fact>]
let ``SSTable GetRange reads many entries sequentially`` () =
    withTestDir "sst_range_many" (fun testDataDir ->
        let path =
            writeSst
                testDataDir
                "L0_range_many.sst"
                [ "a", 1L, Some "va"
                  "b", 2L, None
                  "c", 3L, Some "vc"
                  "d", 4L, Some "vd"
                  "e", 5L, Some "ve" ]

        use sst = new SSTable(path)

        match sst.GetRange("a", "e") with
        | RangeOk result ->
            assertEqual 5 result.Length "Five entries in [a,e]"
            assertEqual ("a", 1L, Some "va") result.[0] "First entry is a"
            assertEqual ("b", 2L, None) result.[1] "Tombstone entry is b"
            assertEqual ("c", 3L, Some "vc") result.[2] "Third entry is c"
            assertEqual ("d", 4L, Some "vd") result.[3] "Fourth entry is d"
            assertEqual ("e", 5L, Some "ve") result.[4] "Fifth entry is e"
        | RangeDisposed -> failwith "unexpected RangeDisposed")

[<Fact>]
let ``SSTable GetRange includes tombstones`` () =
    withTestDir "sst_range_tomb" (fun testDataDir ->
        let path =
            writeSst testDataDir "L0_range_tomb.sst" [ "a", 1L, Some "va"; "b", 2L, None; "c", 3L, Some "vc" ]

        use sst = new SSTable(path)

        match sst.GetRange("b", "c") with
        | RangeOk result ->
            assertEqual 2 result.Length "Two entries in [b,c]"
            assertEqual ("b", 2L, None) result.[0] "Tombstone entry"
            assertEqual ("c", 3L, Some "vc") result.[1] "Live value entry"
        | RangeDisposed -> failwith "unexpected RangeDisposed")

[<Fact>]
let ``SSTable GetRange returns empty when range outside data`` () =
    withTestDir "sst_range_outside" (fun testDataDir ->
        let path =
            writeSst testDataDir "L0_outside.sst" [ "c", 1L, Some "vc"; "d", 2L, Some "vd" ]

        use sst = new SSTable(path)
        assertEqual (RangeOk [||]) (sst.GetRange("a", "b")) "Range before data returns empty"
        assertEqual (RangeOk [||]) (sst.GetRange("e", "z")) "Range after data returns empty")

[<Fact>]
let ``SSTable GetRange returns empty for SSTable with no entries`` () =
    withTestDir "sst_range_empty" (fun testDataDir ->
        let path = writeSst testDataDir "L0_empty_range.sst" []
        use sst = new SSTable(path)
        assertEqual (RangeOk [||]) (sst.GetRange("a", "z")) "Empty SSTable GetRange returns empty")

[<Fact>]
let ``SSTable GetRange returns RangeDisposed for disposed SSTable`` () =
    withTestDir "sst_range_disposed" (fun testDataDir ->
        let path =
            writeSst testDataDir "L0_range_disposed.sst" [ "a", 1L, Some "va"; "b", 2L, Some "vb" ]

        let sst = new SSTable(path)

        match sst.GetRange("a", "b") with
        | RangeOk entries -> assertEqual 2 entries.Length "GetRange before dispose returns entries"
        | RangeDisposed -> failwith "unexpected RangeDisposed"

        (sst :> System.IDisposable).Dispose()
        assertEqual RangeDisposed (sst.GetRange("a", "b")) "GetRange after dispose returns RangeDisposed")

[<Fact>]
let ``SSTable double dispose does not throw`` () =
    withTestDir "sst_double_dispose" (fun testDataDir ->
        let sstPath = System.IO.Path.Combine(testDataDir, "double_dispose.sst")
        SSTableWriter.write sstPath [] |> ignore

        let sst = new SSTable(sstPath)
        (sst :> System.IDisposable).Dispose()
        (sst :> System.IDisposable).Dispose())

[<Fact>]
let ``SSTableWriter writeBytes writes length-prefixed bytes`` () =
    use ms = new System.IO.MemoryStream()
    use bw = new System.IO.BinaryWriter(ms)
    let bytes = "hello"B
    SSTableWriter.writeBytes bw bytes
    bw.Flush()
    ms.Position <- 0L
    use br = new System.IO.BinaryReader(ms)
    let len = br.ReadInt32()
    assertEqual bytes.Length len "Length prefix should match"

    let data = br.ReadBytes len
    assertEqual "hello" (System.Text.Encoding.UTF8.GetString data) "Data should roundtrip"

[<Fact>]
let ``SSTableWriter writeValue writes length-prefixed string`` () =
    use ms = new System.IO.MemoryStream()
    use bw = new System.IO.BinaryWriter(ms)
    SSTableWriter.writeValue bw "test_value"
    bw.Flush()
    ms.Position <- 0L
    use br = new System.IO.BinaryReader(ms)
    let len = br.ReadInt32()
    let data = br.ReadBytes len
    assertEqual "test_value" (System.Text.Encoding.UTF8.GetString data) "Value should roundtrip"

[<Fact>]
let ``SSTableWriter writeItem writes Some value`` () =
    use ms = new System.IO.MemoryStream()
    use bw = new System.IO.BinaryWriter(ms)
    SSTableWriter.writeItem bw (Some "item_val")
    bw.Flush()
    ms.Position <- 0L
    use br = new System.IO.BinaryReader(ms)
    assertEqual false (br.ReadBoolean()) "Some value should write false (has value)"

    let len = br.ReadInt32()
    let data = br.ReadBytes len
    assertEqual "item_val" (System.Text.Encoding.UTF8.GetString data) "Item value should roundtrip"

[<Fact>]
let ``SSTableWriter writeItem writes None`` () =
    use ms = new System.IO.MemoryStream()
    use bw = new System.IO.BinaryWriter(ms)
    SSTableWriter.writeItem bw None
    bw.Flush()
    ms.Position <- 0L
    use br = new System.IO.BinaryReader(ms)
    assertEqual true (br.ReadBoolean()) "None should write true (no value)"

[<Fact>]
let ``SSTableWriter writes inline index and roundtrips`` () =
    withTestDir "sst_inline_idx" (fun testDataDir ->
        let path = System.IO.Path.Combine(testDataDir, "inline_idx.sst")

        SSTableWriter.write path [ "k1", 1L, Some "v1"; "k2", 2L, None; "k3", 3L, Some "v3" ]
        |> ignore

        use fs =
            new System.IO.FileStream(
                path,
                System.IO.FileMode.Open,
                System.IO.FileAccess.Read,
                System.IO.FileShare.Read
            )

        use br = new System.IO.BinaryReader(fs)
        let _, _, _, index = SSTable.load fs br
        assertEqual 3 index.Length "Should have 3 index entries"
        assertEqual "k1" index.[0].Key "First entry key"
        assertEqual 1L index.[0].Seq "First entry seq"
        assertEqual "k2" index.[1].Key "Second entry key"
        assertEqual 2L index.[1].Seq "Second entry seq"
        assertEqual "k3" index.[2].Key "Third entry key"
        assertEqual 3L index.[2].Seq "Third entry seq")

[<Fact>]
let ``SSTableWriter writeStream throws on cancellation`` () =
    withTestDir "sst_cancel" (fun testDataDir ->
        let path = System.IO.Path.Combine(testDataDir, "cancel.sst")
        let ct = System.Threading.CancellationToken true

        Assert.Throws<System.OperationCanceledException>(fun () ->
            SSTableWriter.writeStream path ct 64 [ "k", 1L, Some "v" ] |> ignore))
