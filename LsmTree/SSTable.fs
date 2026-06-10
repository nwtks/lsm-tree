namespace LsmTree

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

    let load (fs: System.IO.FileStream) (br: System.IO.BinaryReader) =
        let fileLen = fs.Length

        let loadOffsets offset footerSize =
            if offset < 0L || offset > fileLen - footerSize then
                System.IO.InvalidDataException $"SSTable index offset {offset} is out of range (file size: {fileLen})"
                |> raise

            fs.Seek(offset, System.IO.SeekOrigin.Begin) |> ignore
            let count = br.ReadInt32()

            if count < 0 then
                System.IO.InvalidDataException $"SSTable index entry count is negative: {count}"
                |> raise

            let remaining = fileLen - footerSize - offset - INDEX_COUNT_BYTE_SIZE

            if int64 count * SEQ_BYTE_SIZE > remaining then
                System.IO.InvalidDataException $"SSTable index of {count} entries would exceed remaining space"
                |> raise

            let offsets = Array.init count (fun _ -> br.ReadInt64())

            for i = 0 to offsets.Length - 1 do
                if offsets.[i] < 0L || offsets.[i] >= offset then
                    System.IO.InvalidDataException $"SSTable entry offset at index {i} is out of range: {offsets.[i]}"
                    |> raise

            offsets

        let loadBloomFilter offset footerSize =
            if offset < 0L || offset > fileLen - footerSize then
                System.IO.InvalidDataException $"SSTable bloom offset {offset} is out of range (file size: {fileLen})"
                |> raise

            fs.Seek(offset, System.IO.SeekOrigin.Begin) |> ignore
            let byteCount = br.ReadInt32()

            if byteCount < 0 then
                System.IO.InvalidDataException $"SSTable bloom filter byte count is negative: {byteCount}"
                |> raise

            let remaining = fileLen - footerSize - offset - BLOOM_COUNT_BYTE_SIZE

            if int64 byteCount > remaining then
                System.IO.InvalidDataException $"SSTable bloom filter of {byteCount} bytes would exceed remaining space"
                |> raise

            let bfBytes = br.ReadBytes byteCount
            BloomFilter(bfBytes, BloomFilter.numHashFunctions)

        if fileLen >= FOOTER_SIZE then
            fs.Seek(-FOOTER_SIZE, System.IO.SeekOrigin.End) |> ignore
            let indexOffset = br.ReadInt64()
            let bloomOffset = br.ReadInt64()
            let maxSeq = br.ReadInt64()
            let magic = br.ReadInt64()

            if magic <> MAGIC then
                System.IO.InvalidDataException $"Invalid SSTable magic number: expected 0x{MAGIC:x}, got 0x{magic:x}"
                |> raise

            if indexOffset < 0L || indexOffset > fileLen - FOOTER_SIZE then
                System.IO.InvalidDataException
                    $"SSTable index offset {indexOffset} is out of range (file size: {fileLen})"
                |> raise

            if bloomOffset < indexOffset || bloomOffset > fileLen - FOOTER_SIZE then
                System.IO.InvalidDataException
                    $"SSTable bloom offset {bloomOffset} is out of range (index offset: {indexOffset})"
                |> raise

            loadOffsets indexOffset FOOTER_SIZE, loadBloomFilter bloomOffset FOOTER_SIZE, maxSeq
        else
            [||], BloomFilter([||], 0), 0L

    let readValue (br: System.IO.BinaryReader) =
        br.ReadInt32() |> br.ReadBytes |> System.Text.Encoding.UTF8.GetString

    let readItem (br: System.IO.BinaryReader) =
        if br.ReadBoolean() then None else readValue br |> Some

    let readAllEntries (br: System.IO.BinaryReader) (offsets: int64[]) =
        offsets
        |> Array.map (fun _ ->
            let seq = br.ReadInt64()
            let key = readValue br
            let value = readItem br
            key, seq, value)

    let loadIndex (fs: System.IO.FileStream) (br: System.IO.BinaryReader) (offsets: int64[]) =
        if offsets.Length > 0 then
            fs.Seek(offsets.[0], System.IO.SeekOrigin.Begin) |> ignore

            offsets
            |> Array.map (fun offset ->
                let seq = br.ReadInt64()
                let key = readValue br
                let keyByteLen = System.Text.Encoding.UTF8.GetByteCount key

                if br.ReadBoolean() then
                    ()
                else
                    let valueLen = br.ReadInt32()
                    fs.Seek(int64 valueLen, System.IO.SeekOrigin.Current) |> ignore

                { Key = key
                  Seq = seq
                  Offset = offset
                  KeyByteLen = keyByteLen })
        else
            [||]

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

type SSTable(path: string) =
    let fs =
        new System.IO.FileStream(path, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read)

    let br = new System.IO.BinaryReader(fs)
    let offsets, bloomFilter, maxSeq = SSTable.load fs br
    let index = SSTable.loadIndex fs br offsets
    let rwLock = new System.Threading.ReaderWriterLockSlim()
    let mutable disposed = false

    member _.Path = path

    member _.Count = offsets.Length

    member _.MaxSeq = maxSeq

    member _.GetAll() =
        LockExtensions.withWriteLock rwLock (fun () ->
            if offsets.Length > 0 then
                fs.Seek(offsets.[0], System.IO.SeekOrigin.Begin) |> ignore
                SSTable.readAllEntries br offsets
            else
                [||])

    member _.Get(key: string, snapshot: int64) =
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

                    Some(SSTable.readItem br))
            | None -> None
        else
            None

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

    let writeOffsets (bw: System.IO.BinaryWriter) (offsets: int64 list) =
        bw.Write offsets.Length
        offsets |> List.iter bw.Write

    let writeCore
        outPath
        (bf: BloomFilter)
        (ct: System.Threading.CancellationToken)
        (entries: seq<string * int64 * string option>)
        =
        let offsets = ResizeArray<int64>()
        let mutable maxSeq = 0L
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

                for key, seq, value in entries do
                    ct.ThrowIfCancellationRequested()
                    bf.Add key
                    offsets.Add fs.Position
                    bw.Write seq

                    if seq > maxSeq then
                        maxSeq <- seq

                    writeValue bw key
                    writeItem bw value

                let indexOffset = fs.Position
                writeOffsets bw (offsets |> Seq.toList)

                let bloomOffset = fs.Position
                writeBytes bw bf.Bytes

                bw.Write indexOffset
                bw.Write bloomOffset
                bw.Write maxSeq
                bw.Write SSTable.MAGIC
                fs.Flush true

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
