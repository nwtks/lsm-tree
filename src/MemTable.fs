namespace LsmTree

open System.Text
open System.Threading

type MemTable() =
    [<Literal>]
    let NODE_OVERHEAD = 32

    [<Literal>]
    let SEQ_SIZE = 8

    let data = SkipList()
    let mutable sizeBytes = 0

    member _.Put(key: string, seq: int64, value: string) =
        let sizeDelta =
            NODE_OVERHEAD
            + Encoding.UTF8.GetByteCount key
            + SEQ_SIZE
            + Encoding.UTF8.GetByteCount value

        Interlocked.Add(&sizeBytes, sizeDelta) |> ignore
        data.Put(key, seq, value)

    member _.Delete(key: string, seq: int64) =
        let sizeDelta = NODE_OVERHEAD + Encoding.UTF8.GetByteCount key + SEQ_SIZE
        Interlocked.Add(&sizeBytes, sizeDelta) |> ignore
        data.Put(key, seq)

    member _.Get(key: string, snapshot: int64) = data.Find(key, snapshot)

    member _.SizeBytes = Volatile.Read(&sizeBytes)

    member _.Entries = data.Entries()
