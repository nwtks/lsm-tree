namespace LsmTree

module SSTable =
    [<Literal>]
    let MAGIC = 0x534D434CL

    let load (fs: System.IO.FileStream) (br: System.IO.BinaryReader) =
        let loadOffsets offset =
            fs.Seek(offset, System.IO.SeekOrigin.Begin) |> ignore
            Array.init (br.ReadInt32()) (fun _ -> br.ReadInt64())

        let loadBloomFilter offset =
            fs.Seek(offset, System.IO.SeekOrigin.Begin) |> ignore
            let bfBytes = br.ReadInt32() |> br.ReadBytes
            BloomFilter(bfBytes, BloomFilter.numHashFunctions)

        if fs.Length >= 24L then
            fs.Seek(-24L, System.IO.SeekOrigin.End) |> ignore
            let indexOffset = br.ReadInt64()
            let bloomOffset = br.ReadInt64()
            let magic = br.ReadInt64()

            if magic <> MAGIC then
                raise (System.IO.InvalidDataException "Invalid SSTable magic number")

            loadOffsets indexOffset, loadBloomFilter bloomOffset
        else
            [||], BloomFilter([||], 0)

    let readValue (br: System.IO.BinaryReader) =
        br.ReadInt32() |> br.ReadBytes |> System.Text.Encoding.UTF8.GetString

    let readItem (br: System.IO.BinaryReader) =
        if br.ReadBoolean() then None else Some(readValue br)

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

    member _.GetAll() =
        seq {
            for offset in offsets do
                yield lock fs (fun () -> SSTable.readEntry fs br offset)
        }

    member _.Get(key: string, snapshot: int64) =
        if offsets.Length = 0 then
            None
        elif not (bloomFilter.MightContain key) then
            None
        else
            lock fs (fun () -> SSTable.binSearch fs br offsets key snapshot 0 (offsets.Length - 1) None)

    interface System.IDisposable with
        member _.Dispose() =
            if not disposed then
                br.Dispose()
                fs.Dispose()
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

    let write outPath (memTableEntries: (string * int64 * string option) list) =
        use fs =
            new System.IO.FileStream(
                outPath,
                System.IO.FileMode.Create,
                System.IO.FileAccess.Write,
                System.IO.FileShare.None
            )

        use bw = new System.IO.BinaryWriter(fs)
        let bf = BloomFilter.create memTableEntries.Length

        let offsets =
            memTableEntries
            |> List.map (fun (key, seq, value) ->
                bf.Add key
                let offset = fs.Position
                bw.Write seq
                writeValue bw key
                writeItem bw value
                offset)

        let indexOffset = fs.Position
        writeOffsets bw offsets

        let bloomOffset = fs.Position
        writeBytes bw bf.Bytes

        bw.Write indexOffset
        bw.Write bloomOffset
        bw.Write SSTable.MAGIC
        fs.Flush true

    let flush outPath memTableEntries =
        write outPath memTableEntries
        new SSTable(outPath)
