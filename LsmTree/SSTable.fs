namespace LsmTree

[<Struct>]
type internal IndexEntry =
    { Key: string
      Seq: int64
      Offset: int64
      KeyByteLen: int32 }

module internal SSTable =
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

    let validateSSTableMagic magic =
        if magic <> MAGIC then
            System.IO.InvalidDataException $"Invalid SSTable magic number: expected 0x{MAGIC:x}, got 0x{magic:x}"
            |> raise

    let validateFooter fileLen indexOffset bloomOffset magic =
        validateSSTableMagic magic
        validateIndexOffset fileLen indexOffset FOOTER_SIZE
        validateBloomOffset fileLen indexOffset bloomOffset FOOTER_SIZE

    let readExactly handle offset (buf: byte[]) =
        let mutable total = 0

        while total < buf.Length do
            let n =
                System.IO.RandomAccess.Read(
                    handle,
                    System.Span<byte>(buf, total, buf.Length - total),
                    offset + int64 total
                )

            if n <= 0 then
                raise (System.IO.EndOfStreamException())

            total <- total + n

    let readInt32 buf (pos: byref<int>) =
        let v =
            System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(System.ReadOnlySpan<byte>(buf, pos, 4))

        pos <- pos + 4
        v

    let readInt64 buf (pos: byref<int>) =
        let v =
            System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(System.ReadOnlySpan<byte>(buf, pos, 8))

        pos <- pos + 8
        v

    let readValue buf (pos: byref<int>) =
        let len = readInt32 buf &pos
        let s = System.Text.Encoding.UTF8.GetString(buf, pos, len)
        pos <- pos + len
        s

    let readItem (buf: byte[]) (pos: byref<int>) =
        if buf.[pos] <> 0uy then
            pos <- pos + 1
            None
        else
            pos <- pos + 1
            readValue buf &pos |> Some

    let readIndexEntry buf (pos: byref<int>) =
        let seq = readInt64 buf &pos
        let offset = readInt64 buf &pos
        let keyByteLen = readInt32 buf &pos
        let key = System.Text.Encoding.UTF8.GetString(buf, pos, keyByteLen)
        pos <- pos + keyByteLen

        { Key = key
          Seq = seq
          Offset = offset
          KeyByteLen = keyByteLen }

    let loadBloomFilter handle fileLen indexOffset offset footerSize =
        validateBloomOffset fileLen indexOffset offset footerSize
        let countBuf = Array.zeroCreate<byte> 4
        readExactly handle offset countBuf

        let byteCount =
            System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(System.ReadOnlySpan<byte> countBuf)

        validateBloomCount fileLen offset byteCount footerSize
        let bfBytes = Array.zeroCreate<byte> byteCount
        readExactly handle (offset + BLOOM_COUNT_BYTE_SIZE) bfBytes
        BloomFilter(bfBytes, BloomFilter.numHashFunctions)

    let loadIndex handle fileLen indexOffset bloomOffset footerSize =
        validateIndexOffset fileLen indexOffset footerSize
        let regionSize = int (bloomOffset - indexOffset)

        if regionSize <= 4 then
            [||]
        else
            let buf = Array.zeroCreate<byte> regionSize
            readExactly handle indexOffset buf
            let mutable pos = 0
            let count = readInt32 buf &pos

            if count < 0 then
                System.IO.InvalidDataException $"SSTable index entry count is negative: {count}"
                |> raise

            Array.init count (fun _ -> readIndexEntry buf &pos)

    let load (fs: System.IO.FileStream) =
        let fileLen = fs.Length

        if fileLen >= FOOTER_SIZE then
            let footerBuf = Array.zeroCreate<byte> (int FOOTER_SIZE)
            readExactly fs.SafeFileHandle (fileLen - FOOTER_SIZE) footerBuf
            let mutable pos = 0
            let indexOffset = readInt64 footerBuf &pos
            let bloomOffset = readInt64 footerBuf &pos
            let maxSeq = readInt64 footerBuf &pos
            let magic = readInt64 footerBuf &pos
            validateFooter fileLen indexOffset bloomOffset magic

            let bloom =
                loadBloomFilter fs.SafeFileHandle fileLen indexOffset bloomOffset FOOTER_SIZE

            let index = loadIndex fs.SafeFileHandle fileLen indexOffset bloomOffset FOOTER_SIZE
            bloom, maxSeq, indexOffset, index
        else
            BloomFilter([||], 0), 0L, 0L, [||]

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

    let readAllEntries buf count =
        let mutable pos = 0

        Array.init count (fun _ ->
            let seq = readInt64 buf &pos
            let key = readValue buf &pos
            let value = readItem buf &pos
            key, seq, value)

    let readItemAt handle offset =
        let header = Array.zeroCreate<byte> 5
        readExactly handle offset header

        if header.[0] <> 0uy then
            None
        else
            let len =
                System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(System.ReadOnlySpan<byte>(header, 1, 4))

            let valueBuf = Array.zeroCreate<byte> len
            readExactly handle (offset + 5L) valueBuf
            Some(System.Text.Encoding.UTF8.GetString valueBuf)

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

    let readRangeEntries handle (index: IndexEntry[]) lo hi indexOffset =
        let fromOffset = index.[lo].Offset
        let endOffset = if hi < index.Length then index.[hi].Offset else indexOffset
        let length = int (endOffset - fromOffset)
        let buf = Array.zeroCreate<byte> length
        readExactly handle fromOffset buf
        let mutable pos = 0

        Array.init (hi - lo) (fun i ->
            let entry = index.[lo + i]
            pos <- pos + 8 + 4 + int entry.KeyByteLen
            let value = readItem buf &pos
            entry.Key, entry.Seq, value)

type internal RangeReadResult =
    | RangeOk of entries: (string * int64 * string option)[]
    | RangeDisposed

type internal SSTable(path) =
    let fs =
        new System.IO.FileStream(path, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read)

    let bloomFilter, maxSeq, indexOffset, index =
        try
            SSTable.load fs
        with ex ->
            fs.Dispose()
            raise ex

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

                let valueOffset =
                    entry.Offset
                    + SSTable.SEQ_BYTE_SIZE
                    + SSTable.KEY_LEN_BYTE_SIZE
                    + int64 entry.KeyByteLen

                try
                    match
                        LockExtensions.withReadLock rwLock (fun () -> SSTable.readItemAt fs.SafeFileHandle valueOffset)
                    with
                    | Some v -> Found v
                    | None -> Tombstone
                with :? System.ObjectDisposedException ->
                    NotFound
            | None -> NotFound
        else
            NotFound

    member _.GetAll() =
        try
            LockExtensions.withReadLock rwLock (fun () ->
                if index.Length > 0 then
                    let dataLen = int (indexOffset - index.[0].Offset)
                    let buf = Array.zeroCreate<byte> dataLen
                    SSTable.readExactly fs.SafeFileHandle index.[0].Offset buf
                    SSTable.readAllEntries buf index.Length
                else
                    [||])
        with :? System.ObjectDisposedException ->
            [||]

    member _.GetRange(fromKey, toKey) =
        if index.Length = 0 then
            RangeOk [||]
        else
            let lo = SSTable.lowerBound index fromKey 0 (index.Length - 1)
            let hi = SSTable.upperBound index toKey 0 (index.Length - 1)

            if lo >= hi then
                RangeOk [||]
            else
                try
                    LockExtensions.withReadLock rwLock (fun () ->
                        SSTable.readRangeEntries fs.SafeFileHandle index lo hi indexOffset)
                    |> RangeOk
                with :? System.ObjectDisposedException ->
                    RangeDisposed

    interface System.IDisposable with
        member _.Dispose() =
            if not disposed then
                let shouldDispose =
                    LockExtensions.withWriteLock rwLock (fun () ->
                        if not disposed then
                            disposed <- true
                            fs.Dispose()
                            true
                        else
                            false)

                if shouldDispose then
                    rwLock.Dispose()

module internal SSTableWriter =
    let writeBytes (bw: System.IO.BinaryWriter) (bytes: byte[]) =
        bw.Write bytes.Length
        bw.Write bytes

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
