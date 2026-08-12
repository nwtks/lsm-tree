namespace LsmTree

type internal BloomFilter(bits: byte[], numHashFunctions: int) =
    let bitSize = bits.Length * 8

    let hash (key: string) =
        let h =
            System.Text.Encoding.UTF8.GetBytes key
            |> Array.fold (fun acc b -> (acc ^^^ uint64 b) * 1099511628211uL) 14695981039346656037uL

        struct (uint32 (h >>> 32), uint32 (h &&& 0xFFFFFFFFuL))

    let keyIndex h1 h2 seed =
        let idx = (h1 + uint32 seed * (h2 ||| 1u)) % uint32 bitSize
        let byteIdx = int (idx / 8u)
        let bitIdx = int (idx % 8u)
        struct (byteIdx, bitIdx)

    [<TailCall>]
    let rec check h1 h2 seed =
        if seed >= numHashFunctions then
            true
        else
            let struct (byteIdx, bitIdx) = keyIndex h1 h2 seed

            if bits.[byteIdx] &&& (1uy <<< bitIdx) = 0uy then
                false
            else
                check h1 h2 (seed + 1)

    member _.Add(key: string) =
        if bitSize > 0 then
            let struct (h1, h2) = hash key

            for i = 0 to numHashFunctions - 1 do
                let struct (byteIdx, bitIdx) = keyIndex h1 h2 i
                bits.[byteIdx] <- bits.[byteIdx] ||| (1uy <<< bitIdx)

    member _.MightContain(key: string) =
        if bitSize > 0 then
            let struct (h1, h2) = hash key
            check h1 h2 0
        else
            true

    member _.Bytes = bits

module internal BloomFilter =
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
