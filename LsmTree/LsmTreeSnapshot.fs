namespace LsmTree

type LsmTreeSnapshot() =
    let mutable globalSeq = 0L
    let mutable activeSnapshots = Set.empty<int64>
    let activeSnapshotsLock = obj ()

    member _.CurrentSequence() =
        System.Threading.Interlocked.Read(&globalSeq)

    member _.NextSequence() =
        System.Threading.Interlocked.Increment(&globalSeq)

    member _.AdvanceSequence seq =
        let mutable current = System.Threading.Interlocked.Read(&globalSeq)

        while current < seq do
            let original =
                System.Threading.Interlocked.CompareExchange(&globalSeq, seq, current)

            if original = current then
                current <- seq
            else
                current <- original

    member _.RegisterSnapshot snapshot =
        lock activeSnapshotsLock (fun () -> activeSnapshots <- Set.add snapshot activeSnapshots)

    member _.ReleaseSnapshot snapshot =
        lock activeSnapshotsLock (fun () -> activeSnapshots <- Set.remove snapshot activeSnapshots)

    member _.GetMinActiveSnapshot() =
        lock activeSnapshotsLock (fun () ->
            if Set.isEmpty activeSnapshots then
                System.Threading.Interlocked.Read(&globalSeq)
            else
                Set.minElement activeSnapshots)
