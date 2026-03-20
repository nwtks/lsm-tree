namespace LsmTree

type BloomFilter(bits: byte[], numHashFunctions: int) =
    let bitSize = bits.Length * 8

    let hash (key: string) seed =
        let h = key |> Seq.fold (fun acc c -> acc * 31u + uint32 c) (uint32 seed)
        int (h % uint32 bitSize)

    let keyIndex key seed =
        let idx = hash key seed
        let byteIdx = idx / 8
        let bitIdx = idx % 8
        byteIdx, bitIdx

    let rec check key i =
        if i >= numHashFunctions then
            true
        else
            let byteIdx, bitIdx = keyIndex key i

            if bits.[byteIdx] &&& (1uy <<< bitIdx) = 0uy then
                false
            else
                check key (i + 1)

    member _.Add(key: string) =
        if bitSize > 0 then
            for i = 0 to numHashFunctions - 1 do
                let byteIdx, bitIdx = keyIndex key i
                bits.[byteIdx] <- bits.[byteIdx] ||| (1uy <<< bitIdx)

    member _.MightContain(key: string) =
        if bitSize = 0 then true else check key 0

    member _.Bytes = bits

module BloomFilter =
    let bitsPerItem = 10

    let create numEntries =
        let bitSize = max 64 (numEntries * bitsPerItem)
        let byteSize = (bitSize + 7) / 8
        BloomFilter(Array.zeroCreate<byte> byteSize, 7)
