namespace LsmTree

open System
open System.IO
open System.Text

type SSTable(path: string) =
    let mutable offsets: int64[] = [||]
    let mutable bloomFilter = BloomFilter([||], 0)

    let load path =
        use fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read)
        use br = new BinaryReader(fs)

        let loadOffsets offset =
            fs.Seek(offset, SeekOrigin.Begin) |> ignore
            Array.init (br.ReadInt32()) (fun _ -> br.ReadInt64())

        let loadBloomFilter offset =
            fs.Seek(offset, SeekOrigin.Begin) |> ignore
            let bfBytes = br.ReadInt32() |> br.ReadBytes
            BloomFilter(bfBytes, 7)

        if fs.Length >= 16L then
            fs.Seek(-16L, SeekOrigin.End) |> ignore
            let indexOffset = br.ReadInt64()
            let bloomOffset = br.ReadInt64()
            offsets <- loadOffsets indexOffset
            bloomFilter <- loadBloomFilter bloomOffset

    do
        if File.Exists path then
            load path

    let readValue (br: BinaryReader) =
        br.ReadInt32() |> br.ReadBytes |> Encoding.UTF8.GetString

    let readItem (br: BinaryReader) =
        if br.ReadBoolean() then None else Some(readValue br)

    let search key snapshot =
        use fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read)
        use br = new BinaryReader(fs)

        let rec binSearch left right bestMatch =
            if left > right then
                bestMatch
            else
                let mid = left + (right - left) / 2
                fs.Seek(offsets.[mid], SeekOrigin.Begin) |> ignore

                let currentSeq = br.ReadInt64()
                let currentKey = readValue br
                let comp = String.CompareOrdinal(key, currentKey)

                if comp = 0 then
                    if currentSeq <= snapshot then
                        binSearch left (mid - 1) (readItem br |> Some)
                    else
                        binSearch (mid + 1) right bestMatch
                elif comp < 0 then
                    binSearch left (mid - 1) bestMatch
                else
                    binSearch (mid + 1) right bestMatch

        binSearch 0 (offsets.Length - 1) None

    member _.Path = path

    member _.GetAll() =
        seq {
            use fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read)
            use br = new BinaryReader(fs)

            for offset in offsets do
                fs.Seek(offset, SeekOrigin.Begin) |> ignore
                let currentSeq = br.ReadInt64()
                let key = readValue br
                yield key, currentSeq, readItem br
        }

    member _.Get(key: string, snapshot: int64) =
        if offsets.Length = 0 then None
        elif not (bloomFilter.MightContain key) then None
        else search key snapshot

module SSTableWriter =
    let writeBytes (bw: BinaryWriter) (bytes: byte[]) =
        bw.Write bytes.Length
        bw.Write bytes

    let writeValue (bw: BinaryWriter) (value: string) =
        Encoding.UTF8.GetBytes value |> writeBytes bw

    let writeItem (bw: BinaryWriter) item =
        match item with
        | None -> bw.Write true
        | Some v ->
            bw.Write false
            writeValue bw v

    let writeOffsets (bw: BinaryWriter) (offsets: int64 list) =
        bw.Write offsets.Length
        offsets |> List.iter bw.Write

    let write outPath (memTableEntries: (string * int64 * string option) list) =
        use fs = new FileStream(outPath, FileMode.Create, FileAccess.Write, FileShare.None)
        use bw = new BinaryWriter(fs)
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

    let flush memTableEntries outPath =
        write outPath memTableEntries
        SSTable outPath
