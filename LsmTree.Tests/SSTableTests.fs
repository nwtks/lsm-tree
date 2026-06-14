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
let ``SSTable load returns empty for short file`` () =
    let testDataDir = getTestDir "sst_short_file"
    let path = System.IO.Path.Combine(testDataDir, "short.sst")
    System.IO.File.WriteAllBytes(path, [| 0uy .. 9uy |])

    use fs =
        new System.IO.FileStream(path, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read)

    use br = new System.IO.BinaryReader(fs)
    let offsets, _, maxSeq = SSTable.load fs br
    assertEqual [||] offsets "Short file should have no offsets"
    assertEqual 0L maxSeq "Short file should have maxSeq 0"

[<Fact>]
let ``SSTable load and loadIndex handle empty SSTable`` () =
    let testDataDir = getTestDir "sst_empty"
    let path = writeSst testDataDir "L0_empty.sst" []

    use fs =
        new System.IO.FileStream(path, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read)

    use br = new System.IO.BinaryReader(fs)
    let offsets, _, maxSeq = SSTable.load fs br
    let index = SSTable.loadIndex fs br offsets
    assertEqual [||] offsets "Empty SSTable should have no offsets"
    assertEqual [||] index "Empty SSTable should have empty index"
    assertEqual 0L maxSeq "Empty SSTable maxSeq = 0"

[<Fact>]
let ``SSTable loadIndex handles tombstone and value entries`` () =
    let testDataDir = getTestDir "sst_idx_tomb"
    let path = System.IO.Path.Combine(testDataDir, "idx_tomb.sst")
    SSTableWriter.write path [ "gone", 2L, None; "keep", 1L, Some "val" ] |> ignore

    use fs =
        new System.IO.FileStream(path, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read)

    use br = new System.IO.BinaryReader(fs)
    let offsets, _, _ = SSTable.load fs br
    let index = SSTable.loadIndex fs br offsets
    assertEqual 2 index.Length "Should have 2 index entries"
    assertEqual "gone" index.[0].Key "First entry key"
    assertEqual 2L index.[0].Seq "First entry seq"
    assertEqual "keep" index.[1].Key "Second entry key"
    assertEqual 1L index.[1].Seq "Second entry seq"

[<Fact>]
let ``SSTable readAllEntries preserves tombstone entries`` () =
    let testDataDir = getTestDir "sst_getall_tomb"
    let path = System.IO.Path.Combine(testDataDir, "getall_tomb.sst")

    SSTableWriter.write path [ "k1", 1L, Some "v1"; "k2", 2L, None; "k3", 3L, Some "v3" ]
    |> ignore

    use fs =
        new System.IO.FileStream(path, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read)

    use br = new System.IO.BinaryReader(fs)
    let offsets, _, _ = SSTable.load fs br
    fs.Seek(offsets.[0], System.IO.SeekOrigin.Begin) |> ignore
    let entries = SSTable.readAllEntries br offsets
    assertEqual 3 entries.Length "Should have 3 entries"
    assertEqual ("k1", 1L, Some "v1") entries.[0] "First entry is k1=v1"
    assertEqual ("k2", 2L, None) entries.[1] "Second entry is k2=tombstone"
    assertEqual ("k3", 3L, Some "v3") entries.[2] "Third entry is k3=v3"

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
let ``SSTable double dispose does not throw`` () =
    let testDataDir = getTestDir "sst_double_dispose"
    let sstPath = System.IO.Path.Combine(testDataDir, "double_dispose.sst")
    SSTableWriter.write sstPath [] |> ignore

    let sst = new SSTable(sstPath)
    (sst :> System.IDisposable).Dispose()
    (sst :> System.IDisposable).Dispose()

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
let ``SSTableWriter writeOffsets writes count and offsets`` () =
    use ms = new System.IO.MemoryStream()
    use bw = new System.IO.BinaryWriter(ms)
    SSTableWriter.writeOffsets bw [ 100L; 200L; 300L ]
    bw.Flush()
    ms.Position <- 0L
    use br = new System.IO.BinaryReader(ms)
    let count = br.ReadInt32()
    assertEqual 3 count "Should write 3 offsets"
    assertEqual 100L (br.ReadInt64()) "First offset"
    assertEqual 200L (br.ReadInt64()) "Second offset"
    assertEqual 300L (br.ReadInt64()) "Third offset"

[<Fact>]
let ``SSTableWriter writeOffsets writes empty list`` () =
    use ms = new System.IO.MemoryStream()
    use bw = new System.IO.BinaryWriter(ms)
    SSTableWriter.writeOffsets bw []
    bw.Flush()
    ms.Position <- 0L
    use br = new System.IO.BinaryReader(ms)
    let count = br.ReadInt32()
    assertEqual 0 count "Empty list should write count 0"
