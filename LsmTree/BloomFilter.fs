namespace LsmTree

type BloomFilter(bits: byte[], numHashFunctions: int) =
    let bitSize = bits.Length * 8

    let hash (key: string) =
        let bytes = System.Text.Encoding.UTF8.GetBytes key
        let mutable h = 14695981039346656037uL

        for i = 0 to bytes.Length - 1 do
            h <- (h ^^^ uint64 bytes.[i]) * 1099511628211uL

        uint32 (h >>> 32), uint32 (h &&& 0xFFFFFFFFuL)

    let keyIndex h1 h2 seed =
        let idx = (h1 + uint32 seed * h2) % uint32 bitSize
        let byteIdx = int (idx / 8u)
        let bitIdx = int (idx % 8u)
        byteIdx, bitIdx

    [<TailCall>]
    let rec check h1 h2 seed =
        if seed >= numHashFunctions then
            true
        else
            let byteIdx, bitIdx = keyIndex h1 h2 seed

            if bits.[byteIdx] &&& (1uy <<< bitIdx) = 0uy then
                false
            else
                check h1 h2 (seed + 1)

    member _.Add(key: string) =
        if bitSize > 0 then
            let h1, h2 = hash key

            for i = 0 to numHashFunctions - 1 do
                let byteIdx, bitIdx = keyIndex h1 h2 i
                bits.[byteIdx] <- bits.[byteIdx] ||| (1uy <<< bitIdx)

    member _.MightContain(key: string) =
        if bitSize > 0 then
            let h1, h2 = hash key
            check h1 h2 0
        else
            true

    member _.Bytes = bits

module BloomFilter =
    let numHashFunctions = 7
    let bitsPerItem = 10

    let create numEntries =
        let bitSize =
            if numEntries > 0 then
                max 64 (numEntries * bitsPerItem)
            else
                0

        let byteSize = (bitSize + 7) / 8
        BloomFilter(Array.zeroCreate<byte> byteSize, numHashFunctions)
