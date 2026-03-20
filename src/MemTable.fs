namespace LsmTree

type MemTable() =
    let mutable data = SkipList()
    let mutable sizeBytes = 0

    member this.Put(key: string, seq: int64, value: string) =
        sizeBytes <- sizeBytes + key.Length + value.Length + 8 // 8 for int64 seq
        data.Put(key, seq, Some value)

    member this.Delete(key: string, seq: int64) =
        sizeBytes <- sizeBytes + key.Length + 8
        data.Put(key, seq, None)

    member this.Get(key: string, snapshot: int64) = data.Find(key, snapshot)

    member this.SizeBytes = sizeBytes

    member this.Clear() =
        data.Clear()
        sizeBytes <- 0

    member this.Entries = data.Entries()
