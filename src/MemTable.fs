namespace LsmTree

open System.Text

type MemTable() =
    [<Literal>]
    let SEQ_LENGTH = 8

    let data = SkipList()
    let mutable sizeBytes = 0

    member _.Put(key: string, seq: int64, value: string) =
        sizeBytes <-
            sizeBytes
            + Encoding.UTF8.GetByteCount key
            + SEQ_LENGTH
            + Encoding.UTF8.GetByteCount value

        data.Put(key, seq, value)

    member _.Delete(key: string, seq: int64) =
        sizeBytes <- sizeBytes + Encoding.UTF8.GetByteCount key + SEQ_LENGTH
        data.Put(key, seq)

    member _.Get(key: string, snapshot: int64) = data.Find(key, snapshot)

    member _.SizeBytes = sizeBytes

    member _.Clear() =
        data.Clear()
        sizeBytes <- 0

    member _.Entries = data.Entries()
