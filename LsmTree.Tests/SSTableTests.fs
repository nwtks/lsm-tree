module LsmTree.Tests.SSTableTests

open Xunit
open LsmTree

let writeSst dataDir name entries =
    let path = System.IO.Path.Combine(dataDir, name)
    SSTableWriter.write path entries |> ignore
    path

let readFooterInt64 (path: string) (fieldOffset: int) =
    use fs =
        new System.IO.FileStream(path, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read)

    let fileLen = fs.Length
    let footer = Array.zeroCreate<byte> (int SSTable.FOOTER_SIZE)
    fs.Seek(fileLen - SSTable.FOOTER_SIZE, System.IO.SeekOrigin.Begin) |> ignore
    fs.Read(footer, 0, footer.Length) |> ignore
    System.BitConverter.ToInt64(footer, fieldOffset)

let patchFileBytes (path: string) (offset: int64) (bytes: byte[]) =
    use fs =
        new System.IO.FileStream(
            path,
            System.IO.FileMode.Open,
            System.IO.FileAccess.ReadWrite,
            System.IO.FileShare.Read
        )

    fs.Seek(offset, System.IO.SeekOrigin.Begin) |> ignore
    fs.Write(bytes, 0, bytes.Length) |> ignore
    fs.Flush()

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
let ``SSTable validateMaxSeq throws when max_seq is negative`` () =
    Assert.Throws<System.IO.InvalidDataException>(fun () -> SSTable.validateMaxSeq -1L 0L)

[<Fact>]
let ``SSTable validateMaxSeq throws when max_seq does not match entries`` () =
    Assert.Throws<System.IO.InvalidDataException>(fun () -> SSTable.validateMaxSeq 100L 5L)

[<Fact>]
let ``SSTable readIndexEntry throws when entry is truncated`` () =
    let buf = Array.zeroCreate<byte> 10
    let mutable pos = 0
    Assert.Throws<System.IO.InvalidDataException>(fun () -> SSTable.readIndexEntry buf &pos |> ignore)

[<Fact>]
let ``SSTable readIndexEntry throws when key length is negative`` () =
    let buf = Array.zeroCreate<byte> 24
    System.Array.Copy(System.BitConverter.GetBytes 1L, 0, buf, 0, 8)
    System.Array.Copy(System.BitConverter.GetBytes 0L, 0, buf, 8, 8)
    System.Array.Copy(System.BitConverter.GetBytes -1, 0, buf, 16, 4)
    System.Array.Copy(System.BitConverter.GetBytes 0, 0, buf, 20, 4)
    let mutable pos = 0
    Assert.Throws<System.IO.InvalidDataException>(fun () -> SSTable.readIndexEntry buf &pos |> ignore)

[<Fact>]
let ``SSTable readIndexEntry throws when key length exceeds buffer`` () =
    let buf = Array.zeroCreate<byte> 24
    System.Array.Copy(System.BitConverter.GetBytes 1L, 0, buf, 0, 8)
    System.Array.Copy(System.BitConverter.GetBytes 0L, 0, buf, 8, 8)
    System.Array.Copy(System.BitConverter.GetBytes 5, 0, buf, 16, 4)
    System.Array.Copy(System.BitConverter.GetBytes 0, 0, buf, 20, 4)
    let mutable pos = 0
    Assert.Throws<System.IO.InvalidDataException>(fun () -> SSTable.readIndexEntry buf &pos |> ignore)

[<Fact>]
let ``SSTable readIndexEntry throws when value length is invalid`` () =
    let buf = Array.zeroCreate<byte> 25
    System.Array.Copy(System.BitConverter.GetBytes 1L, 0, buf, 0, 8)
    System.Array.Copy(System.BitConverter.GetBytes 0L, 0, buf, 8, 8)
    System.Array.Copy(System.BitConverter.GetBytes 1, 0, buf, 16, 4)
    System.Array.Copy(System.BitConverter.GetBytes -2, 0, buf, 20, 4)
    let mutable pos = 0
    Assert.Throws<System.IO.InvalidDataException>(fun () -> SSTable.readIndexEntry buf &pos |> ignore)

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

        let _, _, _, index = SSTable.load fs
        assertEqual 2 index.Length "Should have 2 index entries"
        assertEqual "gone" index.[0].Key "First entry key"
        assertEqual 2L index.[0].Seq "First entry seq"
        assertEqual -1 index.[0].ValueByteLen "Tombstone entry value length is -1"
        assertEqual "keep" index.[1].Key "Second entry key"
        assertEqual 1L index.[1].Seq "Second entry seq"
        assertEqual 3 index.[1].ValueByteLen "Value entry length is 3")

[<Fact>]
let ``SSTable loadIndex throws when entry count exceeds region size`` () =
    withTestDir "sst_idx_count_overflow" (fun testDataDir ->
        let path = writeSst testDataDir "L0_count_overflow.sst" [ "k", 1L, Some "v" ]
        let indexOffset = readFooterInt64 path 0
        let bloomOffset = readFooterInt64 path 8
        patchFileBytes path indexOffset (System.BitConverter.GetBytes 100)

        use fs =
            new System.IO.FileStream(
                path,
                System.IO.FileMode.Open,
                System.IO.FileAccess.Read,
                System.IO.FileShare.Read
            )

        Assert.Throws<System.IO.InvalidDataException>(fun () ->
            SSTable.loadIndex fs.SafeFileHandle fs.Length indexOffset bloomOffset SSTable.FOOTER_SIZE
            |> ignore))

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

        let _, maxSeq, _, index = SSTable.load fs
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

        let _, maxSeq, _, index = SSTable.load fs
        assertEqual [||] index "Empty SSTable should have empty index"
        assertEqual 0L maxSeq "Empty SSTable maxSeq = 0")

[<Fact>]
let ``SSTable load throws when footer max_seq does not match entries`` () =
    withTestDir "sst_maxseq_mismatch" (fun testDataDir ->
        let path = writeSst testDataDir "L0_maxseq_bad.sst" [ "k", 1L, Some "v" ]
        let maxSeqOffset = System.IO.FileInfo(path).Length - SSTable.FOOTER_SIZE + 16L
        patchFileBytes path maxSeqOffset (System.BitConverter.GetBytes 100L)

        use fs =
            new System.IO.FileStream(
                path,
                System.IO.FileMode.Open,
                System.IO.FileAccess.Read,
                System.IO.FileShare.Read
            )

        Assert.Throws<System.IO.InvalidDataException>(fun () -> SSTable.load fs |> ignore))

[<Fact>]
let ``SSTable binSearchIndex finds existing key`` () =
    let index =
        [| { Key = "present"
             Seq = 1L
             Offset = 0L
             KeyByteLen = 7
             ValueByteLen = 2 } |]

    let result = SSTable.binSearchIndex index "present" System.Int64.MaxValue 0 0 None
    assertEqual (Some 0) result "Existing key returns correct index"

[<Fact>]
let ``SSTable binSearchIndex respects snapshot isolation`` () =
    let index =
        [| { Key = "k"
             Seq = 10L
             Offset = 0L
             KeyByteLen = 1
             ValueByteLen = 2 }
           { Key = "k"
             Seq = 5L
             Offset = 10L
             KeyByteLen = 1
             ValueByteLen = 2 } |]

    assertEqual None (SSTable.binSearchIndex index "k" 3L 0 1 None) "Snapshot below all seqs returns None"
    assertEqual (Some 1) (SSTable.binSearchIndex index "k" 7L 0 1 None) "Snapshot between seqs finds older version"
    assertEqual (Some 0) (SSTable.binSearchIndex index "k" 10L 0 1 None) "Snapshot at max seq finds newest"

[<Fact>]
let ``SSTable binSearchIndex returns None for missing key`` () =
    let index =
        [| { Key = "present"
             Seq = 1L
             Offset = 0L
             KeyByteLen = 7
             ValueByteLen = 2 } |]

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

        let _, _, indexOffset, index = SSTable.load fs
        let dataLen = int (indexOffset - index.[0].Offset)
        let buf = Array.zeroCreate<byte> dataLen
        SSTable.readExactly fs.SafeFileHandle index.[0].Offset buf
        let entries = SSTable.readAllEntries buf index.Length
        assertEqual 3 entries.Length "Should have 3 entries"
        assertEqual ("k1", 1L, Some "v1") entries.[0] "First entry is k1=v1"
        assertEqual ("k2", 2L, None) entries.[1] "Second entry is k2=tombstone"
        assertEqual ("k3", 3L, Some "v3") entries.[2] "Third entry is k3=v3")

[<Fact>]
let ``SSTable readItemAt reads value in one call and returns None for tombstone`` () =
    withTestDir "sst_readitem" (fun testDataDir ->
        let path =
            writeSst testDataDir "L0_readitem.sst" [ "gone", 2L, None; "keep", 1L, Some "hello" ]

        use fs =
            new System.IO.FileStream(
                path,
                System.IO.FileMode.Open,
                System.IO.FileAccess.Read,
                System.IO.FileShare.Read
            )

        let _, _, _, index = SSTable.load fs

        let valueOffset (e: IndexEntry) =
            e.Offset
            + SSTable.SEQ_BYTE_SIZE
            + SSTable.KEY_LEN_BYTE_SIZE
            + int64 e.KeyByteLen

        assertEqual
            (Some "hello")
            (SSTable.readItemAt fs.SafeFileHandle (valueOffset index.[1]) index.[1].ValueByteLen)
            "Live value read in one call"

        assertEqual
            None
            (SSTable.readItemAt fs.SafeFileHandle (valueOffset index.[0]) index.[0].ValueByteLen)
            "Tombstone returns None without reading")

[<Fact>]
let ``SSTable constructor throws for invalid file`` () =
    withTestDir "sst_invalid_file" (fun testDataDir ->
        let path = System.IO.Path.Combine(testDataDir, "corrupt.sst")
        let data = Array.create 32 0uy
        System.IO.File.WriteAllBytes(path, data)

        let exceptionThrown =
            try
                use _ = new SSTable(path)
                false
            with _ ->
                true

        Assert.True(exceptionThrown, "SSTable constructor should throw for invalid file"))

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
let ``SSTable GetAll on disposed SSTable returns empty array`` () =
    withTestDir "sst_getall_disposed" (fun testDataDir ->
        let path =
            writeSst testDataDir "L0_getall.sst" [ "a", 1L, Some "va"; "b", 2L, Some "vb" ]

        let sst = new SSTable(path)
        (sst :> System.IDisposable).Dispose()

        let result = sst.GetAll()
        assertEqual 0 result.Length "GetAll on disposed SSTable returns empty array")

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

        let _, _, _, index = SSTable.load fs
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
