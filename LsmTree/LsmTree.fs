namespace LsmTree

type LsmTree(dataDir: string, ?memTableSizeLimit: int, ?compactLevelLimits: int[]) =
    let memTableLimit = defaultArg memTableSizeLimit (1024 * 1024)

    let compactLevelLimits =
        defaultArg compactLevelLimits [| 4; 10; 100; 1000 |]
        |> LsmTreeLoader.validateCompactLevelLimits

    let walPath = System.IO.Path.Combine(dataDir, "wal.log")
    let mutable memTable = MemTable()
    let mutable immutableMemTable: MemTable option = None
    let mainLock = new System.Threading.ReaderWriterLockSlim()
    let snapshotManager = LsmTreeSnapshot()

    let ssTables =
        Array.init (compactLevelLimits.Length + 1) (fun _ -> list<SSTable>.Empty)

    let ssTablesLock = obj ()
    let compaction = new CompactionCoordinator()
    let flushCoordinator = new FlushCoordinator()
    let mutable disposed = false

    do
        if not (System.IO.Directory.Exists dataDir) then
            System.IO.Directory.CreateDirectory dataDir |> ignore

        LsmTreeLoader.loadSSTables dataDir ssTables snapshotManager
        LsmTreeLoader.loadWal dataDir memTable snapshotManager

    let mutable wal = new WAL(walPath)

    let flushMemTable () =
        if not (flushCoordinator.AcquireAndReset()) then
            ()
        else
            let swapResult =
                LockExtensions.withWriteLock mainLock (fun () ->
                    match LsmTreeFlush.swapMemTableAndWal dataDir memTable wal walPath with
                    | Some(newMt, newWal, oldMt, oldWalPath) ->
                        memTable <- newMt
                        wal <- newWal
                        immutableMemTable <- Some oldMt
                        Some(oldMt, oldWalPath)
                    | None -> None)

            match swapResult with
            | Some(oldMemTable, oldWalPath) ->
                LsmTreeFlush.asyncFlushToSSTable
                    mainLock
                    dataDir
                    snapshotManager
                    ssTablesLock
                    ssTables
                    compactLevelLimits
                    compaction
                    flushCoordinator
                    oldMemTable
                    oldWalPath
                    (fun () ->
                        match immutableMemTable with
                        | Some mt when obj.ReferenceEquals(mt, oldMemTable) -> immutableMemTable <- None
                        | _ -> ())
            | None -> flushCoordinator.SignalCompleted()

    let writeWithFlushCheck writeFn =
        let shouldFlush =
            LockExtensions.withReadLock mainLock (fun () ->
                writeFn ()
                memTable.SizeBytes >= memTableLimit)

        if shouldFlush then
            flushMemTable ()

    let putDirect key value =
        writeWithFlushCheck (fun () ->
            let seq = snapshotManager.NextSequence()
            wal.PutSingle(seq, key, value, false)
            memTable.Put(key, seq, value))

    let deleteDirect key =
        writeWithFlushCheck (fun () ->
            let seq = snapshotManager.NextSequence()
            wal.DeleteSingle(seq, key, false)
            memTable.Delete(key, seq))

    let commitTransaction (ops: (string * string option) list) =
        let shouldFlush =
            LockExtensions.withReadLock mainLock (fun () ->
                if not ops.IsEmpty then
                    let commitSeq = snapshotManager.NextSequence()
                    wal.Begin commitSeq

                    ops
                    |> List.iter (fun (k, vOpt) ->
                        match vOpt with
                        | Some v ->
                            wal.Put(commitSeq, k, v)
                            memTable.Put(k, commitSeq, v)
                        | None ->
                            wal.Delete(commitSeq, k)
                            memTable.Delete(k, commitSeq))

                    wal.Commit(commitSeq, sync = true)

                memTable.SizeBytes >= memTableLimit)

        if shouldFlush then
            flushMemTable ()

    let rollbackTransaction () =
        let seq = snapshotManager.NextSequence()
        wal.Abort(seq, false)

    member _.Snapshot() = snapshotManager.CurrentSequence()

    member this.BeginTransaction() =
        let snap = this.Snapshot()
        snapshotManager.RegisterSnapshot snap
        new LsmTransaction(this :> ILsmTree, snap) :> ITransaction

    member _.Put(key: string, value: string) = putDirect key value

    member _.Delete(key: string) = deleteDirect key

    member this.Get(key: string, ?snapshot: int64) =
        defaultArg snapshot (this.Snapshot())
        |> LsmTreeSearch.findValue mainLock memTable immutableMemTable ssTablesLock ssTables key

    member _.Flush() =
        flushMemTable ()
        flushCoordinator.WaitForCompletion()
        LockExtensions.checkCoordError flushCoordinator ssTablesLock "Flush failed"

    member _.FlushAsync() =
        async {
            flushMemTable ()
            do! flushCoordinator.AwaitCompletion()
        }

    member _.WaitForCompaction() =
        LsmTreeFlush.waitForCompaction compaction
        LockExtensions.checkCoordError compaction ssTablesLock "Compaction failed"

    member _.WaitForCompactionAsync() =
        async { do! LsmTreeFlush.waitForCompactionAsync compaction }

    member _.ReleaseSnapshot(snapshot: int64) =
        snapshotManager.ReleaseSnapshot snapshot

    member this.Close() = (this :> System.IDisposable).Dispose()

    interface System.IDisposable with
        member _.Dispose() =
            if not disposed then
                disposed <- true
                compaction.Cancel()

                try
                    LsmTreeFlush.waitForCompaction compaction
                    LockExtensions.logCoordError compaction ssTablesLock "compaction"
                with _ ->
                    ()

                try
                    flushCoordinator.WaitForCompletion()
                    LockExtensions.logCoordError flushCoordinator ssTablesLock "flush"
                with _ ->
                    ()

                LockExtensions.disposeOf wal
                LockExtensions.disposeOf mainLock

                ssTables
                |> Array.iter (fun level -> level |> Seq.iter (fun sst -> LockExtensions.disposeOf sst))

                LockExtensions.disposeOf compaction
                LockExtensions.disposeOf flushCoordinator

    interface ILsmTree with
        member this.Get(key, snapshot) = this.Get(key, ?snapshot = snapshot)
        member _.CommitTransaction ops = commitTransaction ops
        member _.RollbackTransaction() = rollbackTransaction ()

        member this.ReleaseSnapshot snapshot = this.ReleaseSnapshot snapshot
