namespace LsmTree

type MemTable() =
    [<Literal>]
    let NODE_OVERHEAD = 32

    [<Literal>]
    let SEQ_SIZE = 8

    let data = SkipList()
    let mutable sizeBytes = 0

    member _.Put(key: string, seq, value: string) =
        let sizeDelta =
            NODE_OVERHEAD
            + System.Text.Encoding.UTF8.GetByteCount key
            + SEQ_SIZE
            + System.Text.Encoding.UTF8.GetByteCount value

        System.Threading.Interlocked.Add(&sizeBytes, sizeDelta) |> ignore
        data.Put(key, seq, value)

    member _.Delete(key: string, seq) =
        let sizeDelta =
            NODE_OVERHEAD + System.Text.Encoding.UTF8.GetByteCount key + SEQ_SIZE

        System.Threading.Interlocked.Add(&sizeBytes, sizeDelta) |> ignore
        data.Put(key, seq)

    member _.Get(key, snapshot) = data.Find(key, snapshot)

    member _.SizeBytes = System.Threading.Volatile.Read(&sizeBytes)

    member _.Entries = data.Entries()

    member _.EntriesRange(fromKey, toKey) =
        data.EntriesRange(fromKey, toKey) |> List.toArray
