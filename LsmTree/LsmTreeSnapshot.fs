namespace LsmTree

type LsmTreeSnapshot() =
    let mutable globalSeq = 0L
    let mutable activeSnapshots = Map.empty<int64, int>
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

    member this.AcquireSnapshot() =
        let seq = System.Threading.Interlocked.Read(&globalSeq)
        this.RegisterSnapshot seq
        new SnapshotHandle(this, seq)

    member _.RegisterSnapshot snapshot =
        lock activeSnapshotsLock (fun () ->
            activeSnapshots <-
                match Map.tryFind snapshot activeSnapshots with
                | Some count -> Map.add snapshot (count + 1) activeSnapshots
                | None -> Map.add snapshot 1 activeSnapshots)

    member _.ReleaseSnapshot snapshot =
        lock activeSnapshotsLock (fun () ->
            activeSnapshots <-
                match Map.tryFind snapshot activeSnapshots with
                | Some 1 -> Map.remove snapshot activeSnapshots
                | Some count -> Map.add snapshot (count - 1) activeSnapshots
                | None -> activeSnapshots)

    member _.GetMinActiveSnapshot() =
        lock activeSnapshotsLock (fun () ->
            if Map.isEmpty activeSnapshots then
                System.Threading.Interlocked.Read(&globalSeq)
            else
                Map.minKeyValue activeSnapshots |> fst)

and SnapshotHandle(snapshotManager: LsmTreeSnapshot, seq: int64) =
    member _.Seq = seq

    member _.Dispose() = snapshotManager.ReleaseSnapshot seq

    interface System.IDisposable with
        member this.Dispose() = this.Dispose()
