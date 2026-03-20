namespace LsmTree

open System
open System.IO
open System.Text
open System.Collections.Generic

type BloomFilter(bits: byte[], numHashFunctions: int) =
    let bitSize = bits.Length * 8

    let hash (key: string) (seed: int) =
        let mutable h = uint32 seed

        for i = 0 to key.Length - 1 do
            h <- h * 31u + uint32 key.[i]

        int (h % uint32 bitSize)

    member this.Add(key: string) =
        if bitSize > 0 then
            for i = 0 to numHashFunctions - 1 do
                let idx = hash key i
                let byteIdx = idx / 8
                let bitIdx = idx % 8
                bits.[byteIdx] <- bits.[byteIdx] ||| (1uy <<< bitIdx)

    member this.MightContain(key: string) =
        if bitSize = 0 then
            true
        else
            let rec check i =
                if i >= numHashFunctions then
                    true
                else
                    let idx = hash key i
                    let byteIdx = idx / 8
                    let bitIdx = idx % 8

                    if (bits.[byteIdx] &&& (1uy <<< bitIdx)) = 0uy then
                        false
                    else
                        check (i + 1)

            check 0

    member this.Bytes = bits

    static member Create(numEntries: int) =
        let bitsPerItem = 10
        let bitSize = max 64 (numEntries * bitsPerItem)
        let byteSize = (bitSize + 7) / 8
        BloomFilter(Array.zeroCreate<byte> byteSize, 7)

type SSTable(path: string) =
    let mutable numEntries = 0
    let mutable indexOffset = 0L
    let mutable offsets: int64[] = [||]
    let mutable bloomFilter = BloomFilter([||], 0)

    let loadOffsets () =
        use fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read)
        use br = new BinaryReader(fs)

        if fs.Length >= 16L then
            fs.Seek(-16L, SeekOrigin.End) |> ignore
            indexOffset <- br.ReadInt64()
            let bloomOffset = br.ReadInt64()

            fs.Seek(indexOffset, SeekOrigin.Begin) |> ignore
            numEntries <- br.ReadInt32()

            offsets <- Array.zeroCreate numEntries

            for i = 0 to numEntries - 1 do
                offsets.[i] <- br.ReadInt64()

            fs.Seek(bloomOffset, SeekOrigin.Begin) |> ignore
            let bfLength = br.ReadInt32()
            let bfBytes = br.ReadBytes(bfLength)
            bloomFilter <- BloomFilter(bfBytes, 7)

    do
        if File.Exists(path) then
            loadOffsets ()

    member this.Path = path

    member this.GetAll() =
        seq {
            use fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read)
            use br = new BinaryReader(fs)

            for i = 0 to numEntries - 1 do
                fs.Seek(offsets.[i], SeekOrigin.Begin) |> ignore
                let currentSeq = br.ReadInt64()
                let keyLen = br.ReadInt32()
                let keyBytes = br.ReadBytes(keyLen)
                let key = Encoding.UTF8.GetString(keyBytes)

                let isTombstone = br.ReadBoolean()

                if isTombstone then
                    yield (key, currentSeq, None)
                else
                    let valLen = br.ReadInt32()
                    let valBytes = br.ReadBytes(valLen)
                    yield (key, currentSeq, Some(Encoding.UTF8.GetString(valBytes)))
        }

    member this.Get(key: string, snapshot: int64) : string option option =
        if numEntries = 0 then
            None
        elif not (bloomFilter.MightContain(key)) then
            None
        else
            use fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read)
            use br = new BinaryReader(fs)

            let rec binSearch left right bestMatch =
                if left > right then
                    bestMatch
                else
                    let mid = left + (right - left) / 2
                    fs.Seek(offsets.[mid], SeekOrigin.Begin) |> ignore

                    let currentSeq = br.ReadInt64()
                    let keyLen = br.ReadInt32()
                    let keyBytes = br.ReadBytes(keyLen)
                    let currentKey = Encoding.UTF8.GetString(keyBytes)

                    let comp = String.CompareOrdinal(key, currentKey)

                    if comp = 0 then
                        if currentSeq <= snapshot then
                            let isTombstone = br.ReadBoolean()

                            let rv =
                                if isTombstone then
                                    Some None
                                else
                                    let valLen = br.ReadInt32()
                                    let valBytes = br.ReadBytes(valLen)
                                    Some(Some(Encoding.UTF8.GetString(valBytes)))
                            // Even if we matched, check if there's a newer valid one to the "left" (since seq is desc)
                            binSearch left (mid - 1) rv
                        else
                            // Seq is too new (> snapshot). The older sequences wait to the "right"
                            binSearch (mid + 1) right bestMatch
                    elif comp < 0 then
                        binSearch left (mid - 1) bestMatch
                    else
                        binSearch (mid + 1) right bestMatch

            binSearch 0 (numEntries - 1) None

module SSTableBuilder =
    let flush (memTableEntries: (string * int64 * string option) list) (outPath: string) =
        let write () =
            use fs = new FileStream(outPath, FileMode.Create, FileAccess.Write, FileShare.None)
            use bw = new BinaryWriter(fs)

            let offsets = List<int64>()
            let bf = BloomFilter.Create(memTableEntries.Length)

            for (key, seq, valueOpt) in memTableEntries do
                bf.Add(key)
                offsets.Add(fs.Position)
                bw.Write(seq)
                let keyBytes = Encoding.UTF8.GetBytes(key)
                bw.Write(keyBytes.Length)
                bw.Write(keyBytes)

                match valueOpt with
                | None -> bw.Write(true) // isTombstone
                | Some v ->
                    bw.Write(false)
                    let valBytes = Encoding.UTF8.GetBytes(v)
                    bw.Write(valBytes.Length)
                    bw.Write(valBytes)

            let indexOffset = fs.Position
            bw.Write(offsets.Count)

            for offset in offsets do
                bw.Write(offset)

            let bloomOffset = fs.Position
            let bfBytes = bf.Bytes
            bw.Write(bfBytes.Length)
            bw.Write(bfBytes)

            bw.Write(indexOffset)
            bw.Write(bloomOffset)

        write ()
        SSTable(outPath)
