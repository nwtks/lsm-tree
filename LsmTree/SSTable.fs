namespace LsmTree

[<Struct>]
type IndexEntry =
    { Key: string
      Seq: int64
      Offset: int64
      KeyByteLen: int32 }

module SSTable =
    [<Literal>]
    let MAGIC = 0x4C534D54L

    [<Literal>]
    let FOOTER_SIZE = 32L

    [<Literal>]
    let SEQ_BYTE_SIZE = 8L

    [<Literal>]
    let KEY_LEN_BYTE_SIZE = 4L

    [<Literal>]
    let INDEX_COUNT_BYTE_SIZE = 4L

    [<Literal>]
    let BLOOM_COUNT_BYTE_SIZE = 4L

    let validateIndexOffset fileLen offset footerSize =
        if offset < 0L || offset > fileLen - footerSize then
            System.IO.InvalidDataException $"SSTable index offset {offset} is out of range (file size: {fileLen})"
            |> raise

    let validateIndexCount fileLen offset count footerSize =
        let remaining = fileLen - footerSize - offset - INDEX_COUNT_BYTE_SIZE

        if count < 0 then
            System.IO.InvalidDataException $"SSTable index entry count is negative: {count}"
            |> raise

        if int64 count * SEQ_BYTE_SIZE > remaining then
            System.IO.InvalidDataException $"SSTable index of {count} entries would exceed remaining space"
            |> raise

    let validateEntryOffsets offset (offsets: int64[]) =
        match offsets |> Array.tryFindIndex (fun o -> o < 0L || o >= offset) with
        | Some i ->
            System.IO.InvalidDataException $"SSTable entry offset at index {i} is out of range: {offsets.[i]}"
            |> raise
        | None -> ()

    let validateBloomOffset fileLen indexOffset offset footerSize =
        if offset < indexOffset then
            System.IO.InvalidDataException
                $"SSTable bloom offset {offset} is out of range (index offset: {indexOffset})"
            |> raise

        if offset < 0L || offset > fileLen - footerSize then
            System.IO.InvalidDataException $"SSTable bloom offset {offset} is out of range (file size: {fileLen})"
            |> raise

    let validateBloomCount fileLen offset count footerSize =
        let remaining = fileLen - footerSize - offset - BLOOM_COUNT_BYTE_SIZE

        if count < 0 then
            System.IO.InvalidDataException $"SSTable bloom filter byte count is negative: {count}"
            |> raise

        if int64 count > remaining then
            System.IO.InvalidDataException $"SSTable bloom filter of {count} bytes would exceed remaining space"
            |> raise

    let loadBloomFilter (fs: System.IO.FileStream) (br: System.IO.BinaryReader) fileLen indexOffset offset footerSize =
        validateBloomOffset fileLen indexOffset offset footerSize
        fs.Seek(offset, System.IO.SeekOrigin.Begin) |> ignore
        let byteCount = br.ReadInt32()
        validateBloomCount fileLen offset byteCount footerSize
        let bfBytes = br.ReadBytes byteCount
        BloomFilter(bfBytes, BloomFilter.numHashFunctions)

    let validateSSTableMagic magic =
        if magic <> MAGIC then
            System.IO.InvalidDataException $"Invalid SSTable magic number: expected 0x{MAGIC:x}, got 0x{magic:x}"
            |> raise

    let readFooter (br: System.IO.BinaryReader) =
        let indexOffset = br.ReadInt64()
        let bloomOffset = br.ReadInt64()
        let maxSeq = br.ReadInt64()
        let magic = br.ReadInt64()
        struct (indexOffset, bloomOffset, maxSeq, magic)

    let validateFooter fileLen indexOffset bloomOffset magic =
        validateSSTableMagic magic
        validateIndexOffset fileLen indexOffset FOOTER_SIZE
        validateBloomOffset fileLen indexOffset bloomOffset FOOTER_SIZE

    let readValue (br: System.IO.BinaryReader) =
        br.ReadInt32() |> br.ReadBytes |> System.Text.Encoding.UTF8.GetString

    let readItem (br: System.IO.BinaryReader) =
        if br.ReadBoolean() then None else readValue br |> Some

    let readIndexEntry buf (pos: byref<int>) =
        let seq =
            System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(System.Span<byte>(buf, pos, 8))

        pos <- pos + 8

        let offset =
            System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(System.Span<byte>(buf, pos, 8))

        pos <- pos + 8

        let keyByteLen =
            System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(System.Span<byte>(buf, pos, 4))

        pos <- pos + 4
        let key = System.Text.Encoding.UTF8.GetString(buf, pos, keyByteLen)
        pos <- pos + keyByteLen

        { Key = key
          Seq = seq
          Offset = offset
          KeyByteLen = keyByteLen }

    let loadIndex (fs: System.IO.FileStream) fileLen indexOffset bloomOffset footerSize =
        validateIndexOffset fileLen indexOffset footerSize
        let regionSize = int (bloomOffset - indexOffset)

        if regionSize <= 4 then
            [||]
        else
            let buf = Array.zeroCreate<byte> regionSize
            fs.Seek(indexOffset, System.IO.SeekOrigin.Begin) |> ignore
            fs.ReadExactly(buf, 0, regionSize)
            let mutable pos = 0

            let count =
                System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(System.Span<byte>(buf, pos, 4))

            pos <- pos + 4

            if count < 0 then
                System.IO.InvalidDataException $"SSTable index entry count is negative: {count}"
                |> raise

            Array.init count (fun _ -> readIndexEntry buf &pos)

    let load (fs: System.IO.FileStream) (br: System.IO.BinaryReader) =
        let fileLen = fs.Length

        if fileLen >= FOOTER_SIZE then
            fs.Seek(-FOOTER_SIZE, System.IO.SeekOrigin.End) |> ignore
            let struct (indexOffset, bloomOffset, maxSeq, magic) = readFooter br
            validateFooter fileLen indexOffset bloomOffset magic

            let bloom = loadBloomFilter fs br fileLen indexOffset bloomOffset FOOTER_SIZE
            let index = loadIndex fs fileLen indexOffset bloomOffset FOOTER_SIZE
            bloom, maxSeq, index
        else
            BloomFilter([||], 0), 0L, [||]

    [<TailCall>]
    let rec binSearchIndex (index: IndexEntry[]) key snap left right bestIdx =
        if left > right then
            bestIdx
        else
            let mid = left + (right - left) / 2
            let entry = index.[mid]
            let comp = System.String.CompareOrdinal(key, entry.Key)

            if comp = 0 then
                if entry.Seq <= snap then
                    binSearchIndex index key snap left (mid - 1) (Some mid)
                else
                    binSearchIndex index key snap (mid + 1) right bestIdx
            elif comp < 0 then
                binSearchIndex index key snap left (mid - 1) bestIdx
            else
                binSearchIndex index key snap (mid + 1) right bestIdx

    let readAllEntries (br: System.IO.BinaryReader) count =
        Array.init count (fun _ ->
            let seq = br.ReadInt64()
            let key = readValue br
            let value = readItem br
            key, seq, value)

    [<TailCall>]
    let rec lowerBound (index: IndexEntry[]) key left right =
        if left > right then
            left
        else
            let mid = left + (right - left) / 2

            if System.String.CompareOrdinal(key, index.[mid].Key) <= 0 then
                lowerBound index key left (mid - 1)
            else
                lowerBound index key (mid + 1) right

    [<TailCall>]
    let rec upperBound (index: IndexEntry[]) key left right =
        if left > right then
            left
        else
            let mid = left + (right - left) / 2

            if System.String.CompareOrdinal(key, index.[mid].Key) < 0 then
                upperBound index key left (mid - 1)
            else
                upperBound index key (mid + 1) right

    let readIndexedEntryAt (fs: System.IO.FileStream) (br: System.IO.BinaryReader) entry =
        fs.Seek(entry.Offset + SEQ_BYTE_SIZE + KEY_LEN_BYTE_SIZE + int64 entry.KeyByteLen, System.IO.SeekOrigin.Begin)
        |> ignore

        let value = readItem br
        entry.Key, entry.Seq, value

type SSTable(path) =
    let fs =
        new System.IO.FileStream(path, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read)

    let br = new System.IO.BinaryReader(fs)
    let bloomFilter, maxSeq, index = SSTable.load fs br
    let rwLock = new System.Threading.ReaderWriterLockSlim()
    let mutable disposed = false

    member _.Path = path

    member _.Count = index.Length

    member _.MaxSeq = maxSeq

    member _.Get(key, snapshot) =
        if index.Length > 0 && bloomFilter.MightContain key then
            match SSTable.binSearchIndex index key snapshot 0 (index.Length - 1) None with
            | Some idx ->
                let entry = index.[idx]

                LockExtensions.withReadLock rwLock (fun () ->
                    fs.Seek(
                        entry.Offset
                        + SSTable.SEQ_BYTE_SIZE
                        + SSTable.KEY_LEN_BYTE_SIZE
                        + int64 entry.KeyByteLen,
                        System.IO.SeekOrigin.Begin
                    )
                    |> ignore

                    match SSTable.readItem br with
                    | Some v -> Found v
                    | None -> Tombstone)
            | None -> NotFound
        else
            NotFound

    member _.GetAll() =
        LockExtensions.withWriteLock rwLock (fun () ->
            if index.Length > 0 then
                fs.Seek(index.[0].Offset, System.IO.SeekOrigin.Begin) |> ignore
                SSTable.readAllEntries br index.Length
            else
                [||])

    member _.GetRange(fromKey, toKey) =
        if index.Length = 0 then
            [||]
        else
            let lo = SSTable.lowerBound index fromKey 0 (index.Length - 1)
            let hi = SSTable.upperBound index toKey 0 (index.Length - 1)

            if lo >= hi then
                [||]
            else
                LockExtensions.withReadLock rwLock (fun () ->
                    Array.init (hi - lo) (fun i -> SSTable.readIndexedEntryAt fs br index.[lo + i]))

    interface System.IDisposable with
        member _.Dispose() =
            if not disposed then
                let shouldDispose =
                    LockExtensions.withWriteLock rwLock (fun () ->
                        if not disposed then
                            disposed <- true
                            br.Dispose()
                            fs.Dispose()
                            true
                        else
                            false)

                if shouldDispose then
                    rwLock.Dispose()

module SSTableWriter =
    let writeBytes (bw: System.IO.BinaryWriter) (bytes: byte[]) =
        bw.Write bytes.Length
        bw.Write bytes

    let writeValue (bw: System.IO.BinaryWriter) (value: string) =
        System.Text.Encoding.UTF8.GetBytes value |> writeBytes bw

    let writeItem (bw: System.IO.BinaryWriter) item =
        match item with
        | None -> bw.Write true
        | Some v ->
            bw.Write false
            writeValue bw v

    let writeEntry
        (bw: System.IO.BinaryWriter)
        (fs: System.IO.FileStream)
        (bf: BloomFilter)
        (indexData: ResizeArray<int64 * int64 * int32 * byte[]>)
        maxSeq
        key
        (seq: int64)
        (value: string option)
        =
        bf.Add key
        let entryOffset = fs.Position
        let keyBytes = System.Text.Encoding.UTF8.GetBytes key
        bw.Write seq
        bw.Write keyBytes.Length
        bw.Write keyBytes

        match value with
        | None -> bw.Write true
        | Some v ->
            bw.Write false
            let valBytes = System.Text.Encoding.UTF8.GetBytes v
            bw.Write valBytes.Length
            bw.Write valBytes

        indexData.Add(seq, entryOffset, keyBytes.Length, keyBytes)
        if seq > maxSeq then seq else maxSeq

    let writeIndexData (bw: System.IO.BinaryWriter) (indexData: ResizeArray<int64 * int64 * int32 * byte[]>) =
        bw.Write indexData.Count

        for seq, offset, keyByteLen, keyBytes in indexData do
            bw.Write seq
            bw.Write offset
            bw.Write keyByteLen
            bw.Write keyBytes

    let writeSSTableContent
        (bw: System.IO.BinaryWriter)
        (fs: System.IO.FileStream)
        (bf: BloomFilter)
        (ct: System.Threading.CancellationToken)
        (entries: seq<string * int64 * string option>)
        =
        let indexData = ResizeArray<int64 * int64 * int32 * byte[]>()
        let mutable maxSeq = 0L

        for key, seq, value in entries do
            ct.ThrowIfCancellationRequested()
            maxSeq <- writeEntry bw fs bf indexData maxSeq key seq value

        let indexOffset = fs.Position
        writeIndexData bw indexData
        let bloomOffset = fs.Position
        writeBytes bw bf.Bytes
        bw.Write indexOffset
        bw.Write bloomOffset
        bw.Write maxSeq
        bw.Write SSTable.MAGIC
        fs.Flush true

    let writeCore
        outPath
        (bf: BloomFilter)
        (ct: System.Threading.CancellationToken)
        (entries: seq<string * int64 * string option>)
        =
        let tempPath = outPath + ".tmp"

        try
            do
                use fs =
                    new System.IO.FileStream(
                        tempPath,
                        System.IO.FileMode.Create,
                        System.IO.FileAccess.Write,
                        System.IO.FileShare.None
                    )

                use bw = new System.IO.BinaryWriter(fs)
                writeSSTableContent bw fs bf ct entries

            System.IO.File.Move(tempPath, outPath, overwrite = true)
        finally
            if System.IO.File.Exists tempPath then
                try
                    System.IO.File.Delete tempPath
                with _ ->
                    ()

        new SSTable(outPath)

    let write outPath (memTableEntries: (string * int64 * string option) list) =
        let bf = BloomFilter.create memTableEntries.Length
        writeCore outPath bf System.Threading.CancellationToken.None memTableEntries

    let writeStream outPath (ct: System.Threading.CancellationToken) estimatedEntries entries =
        let bf = BloomFilter.create (max 64 estimatedEntries)
        writeCore outPath bf ct entries
