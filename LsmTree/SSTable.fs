namespace LsmTree

module SSTable =
    [<Literal>]
    let MAGIC = 0x534D434CL

    let footerSize = 24L

    let load (fs: System.IO.FileStream) (br: System.IO.BinaryReader) =
        let fileLen = fs.Length

        let loadOffsets offset =
            if offset < 0L || offset > fileLen - footerSize then
                System.IO.InvalidDataException $"SSTable index offset {offset} is out of range (file size: {fileLen})"
                |> raise

            fs.Seek(offset, System.IO.SeekOrigin.Begin) |> ignore
            let count = br.ReadInt32()

            if count < 0 then
                System.IO.InvalidDataException $"SSTable index entry count is negative: {count}"
                |> raise

            let remaining = fileLen - footerSize - offset - 4L

            if int64 count * 8L > remaining then
                System.IO.InvalidDataException $"SSTable index of {count} entries would exceed remaining space"
                |> raise

            let offsets = Array.init count (fun _ -> br.ReadInt64())

            for i = 0 to offsets.Length - 1 do
                if offsets.[i] < 0L || offsets.[i] >= offset then
                    System.IO.InvalidDataException $"SSTable entry offset at index {i} is out of range: {offsets.[i]}"
                    |> raise

            offsets

        let loadBloomFilter offset =
            if offset < 0L || offset > fileLen - footerSize then
                System.IO.InvalidDataException $"SSTable bloom offset {offset} is out of range (file size: {fileLen})"
                |> raise

            fs.Seek(offset, System.IO.SeekOrigin.Begin) |> ignore
            let byteCount = br.ReadInt32()

            if byteCount < 0 then
                System.IO.InvalidDataException $"SSTable bloom filter byte count is negative: {byteCount}"
                |> raise

            let remaining = fileLen - footerSize - offset - 4L

            if int64 byteCount > remaining then
                System.IO.InvalidDataException $"SSTable bloom filter of {byteCount} bytes would exceed remaining space"
                |> raise

            let bfBytes = br.ReadBytes byteCount
            BloomFilter(bfBytes, BloomFilter.numHashFunctions)

        if fileLen >= footerSize then
            fs.Seek(-footerSize, System.IO.SeekOrigin.End) |> ignore
            let indexOffset = br.ReadInt64()
            let bloomOffset = br.ReadInt64()
            let magic = br.ReadInt64()

            if magic <> MAGIC then
                System.IO.InvalidDataException $"Invalid SSTable magic number: expected 0x{MAGIC:x}, got 0x{magic:x}"
                |> raise

            if indexOffset < 0L || indexOffset > fileLen - footerSize then
                System.IO.InvalidDataException
                    $"SSTable index offset {indexOffset} is out of range (file size: {fileLen})"
                |> raise

            if bloomOffset < indexOffset || bloomOffset > fileLen - footerSize then
                System.IO.InvalidDataException
                    $"SSTable bloom offset {bloomOffset} is out of range (index offset: {indexOffset})"
                |> raise

            loadOffsets indexOffset, loadBloomFilter bloomOffset
        else
            [||], BloomFilter([||], 0)

    let readValue (br: System.IO.BinaryReader) =
        br.ReadInt32() |> br.ReadBytes |> System.Text.Encoding.UTF8.GetString

    let readItem (br: System.IO.BinaryReader) =
        if br.ReadBoolean() then None else readValue br |> Some

    let readEntry (fs: System.IO.FileStream) (br: System.IO.BinaryReader) (offset: int64) =
        fs.Seek(offset, System.IO.SeekOrigin.Begin) |> ignore
        let seq = br.ReadInt64()
        let key = readValue br
        let value = readItem br
        key, seq, value

    [<TailCall>]
    let rec binSearch
        (fs: System.IO.FileStream)
        (br: System.IO.BinaryReader)
        (offsets: int64[])
        key
        snap
        left
        right
        bestMatch
        =
        if left > right then
            bestMatch
        else
            let mid = left + (right - left) / 2
            fs.Seek(offsets.[mid], System.IO.SeekOrigin.Begin) |> ignore
            let currentSeq = br.ReadInt64()
            let currentKey = readValue br
            let comp = System.String.CompareOrdinal(key, currentKey)

            if comp = 0 then
                if currentSeq <= snap then
                    binSearch fs br offsets key snap left (mid - 1) (readItem br |> Some)
                else
                    binSearch fs br offsets key snap (mid + 1) right bestMatch
            elif comp < 0 then
                binSearch fs br offsets key snap left (mid - 1) bestMatch
            else
                binSearch fs br offsets key snap (mid + 1) right bestMatch

type SSTable(path: string) =
    let fs =
        new System.IO.FileStream(path, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read)

    let br = new System.IO.BinaryReader(fs)
    let offsets, bloomFilter = SSTable.load fs br
    let mutable disposed = false

    member _.Path = path

    member _.Count = offsets.Length

    member _.GetAll() =
        lock fs (fun () -> offsets |> Array.map (fun offset -> SSTable.readEntry fs br offset))

    member _.Get(key: string, snapshot: int64) =
        if offsets.Length > 0 && bloomFilter.MightContain key then
            lock fs (fun () -> SSTable.binSearch fs br offsets key snapshot 0 (offsets.Length - 1) None)
        else
            None

    interface System.IDisposable with
        member _.Dispose() =
            if not disposed then
                lock fs (fun () ->
                    br.Dispose()
                    fs.Dispose())

                disposed <- true

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

    let writeCore outPath (entries: seq<string * int64 * string option>) (bf: BloomFilter) =
        let offsets = ResizeArray<int64>()

        do
            use fs =
                new System.IO.FileStream(
                    outPath,
                    System.IO.FileMode.Create,
                    System.IO.FileAccess.Write,
                    System.IO.FileShare.None
                )

            use bw = new System.IO.BinaryWriter(fs)

            for key, seq, value in entries do
                bf.Add key
                offsets.Add fs.Position
                bw.Write seq
                writeValue bw key
                writeItem bw value

            let indexOffset = fs.Position
            writeOffsets bw (offsets |> Seq.toList)

            let bloomOffset = fs.Position
            writeBytes bw bf.Bytes

            bw.Write indexOffset
            bw.Write bloomOffset
            bw.Write SSTable.MAGIC
            fs.Flush true

        new SSTable(outPath)

    let write outPath (memTableEntries: (string * int64 * string option) list) =
        let bf = BloomFilter.create memTableEntries.Length
        writeCore outPath memTableEntries bf

    let writeStream outPath estimatedEntries entries =
        let bf = BloomFilter.create (max 64 estimatedEntries)
        writeCore outPath entries bf
