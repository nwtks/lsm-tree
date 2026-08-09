namespace LsmTree

type LsmTree(dataDir: string, ?memTableSizeLimit: int, ?compactLevelLimits: int[], ?rangeScanMaxRetries: int) =
    let memTableLimit = defaultArg memTableSizeLimit (1024 * 1024)

    let compactLevelLimits =
        defaultArg compactLevelLimits [| 4; 10; 100; 1000 |]
        |> LsmTreeLoader.validateCompactLevelLimits

    let rangeScanRetries = defaultArg rangeScanMaxRetries 8

    let walPath = System.IO.Path.Combine(dataDir, "wal.log")
    let mutable immutableMemTable: MemTable option = None
    let mainLock = new System.Threading.ReaderWriterLockSlim()
    let snapshotManager = LsmTreeSnapshot()

    let ssTablesLock = obj ()
    let compaction = new CompactionCoordinator()
    let flushCoordinator = new FlushCoordinator()
    let mutable disposed = false

    do
        if not (System.IO.Directory.Exists dataDir) then
            System.IO.Directory.CreateDirectory dataDir |> ignore

    let ssTables =
        LsmTreeLoader.loadSSTables dataDir (compactLevelLimits.Length + 1) snapshotManager

    let mutable memTable = LsmTreeLoader.loadWal dataDir snapshotManager
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
                |> Async.Start
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

    let collectMemSources fromKey toKey =
        let mutable acc = []
        let m = memTable.EntriesRange(fromKey, toKey)

        if m.Length > 0 then
            acc <- m :: acc

        match immutableMemTable with
        | Some imm ->
            let i = imm.EntriesRange(fromKey, toKey)

            if i.Length > 0 then
                acc <- i :: acc
        | None -> ()

        acc

    let collectSstSources fromKey toKey =
        let mutable acc = []

        for level in ssTables do
            for sst in level do
                match sst.GetRange(fromKey, toKey) with
                | RangeOk entries ->
                    if entries.Length > 0 then
                        acc <- entries :: acc
                | RangeDisposed -> ()

        acc

    let collectSstSourcesFromSnapshot fromKey toKey (snapshot: SSTable list[]) =
        let readTable (disposed, acc) (sst: SSTable) =
            if disposed then
                disposed, acc
            else
                match sst.GetRange(fromKey, toKey) with
                | RangeOk entries -> false, (if entries.Length > 0 then entries :: acc else acc)
                | RangeDisposed -> true, acc

        snapshot
        |> Array.fold (fun (disposed, acc) level -> List.fold readTable (disposed, acc) level) (false, [])

    let snapshotStable (snapshot: SSTable list[]) =
        lock ssTablesLock (fun () ->
            Array.forall2
                (fun snapLevel curLevel -> System.Object.ReferenceEquals(snapLevel, curLevel))
                snapshot
                ssTables)

    [<TailCall>]
    let rec tryCollectRangeSources fromKey toKey maxRetries attempt =
        let memSources =
            LockExtensions.withReadLock mainLock (fun () -> collectMemSources fromKey toKey)

        let snapshot = lock ssTablesLock (fun () -> Array.copy ssTables)
        let disposed, sstSources = collectSstSourcesFromSnapshot fromKey toKey snapshot

        if disposed then
            tryCollectRangeSources fromKey toKey maxRetries (attempt + 1)
        elif snapshotStable snapshot then
            memSources @ sstSources |> List.rev |> List.toArray
        elif attempt < maxRetries then
            tryCollectRangeSources fromKey toKey maxRetries (attempt + 1)
        else
            let sstSources = lock ssTablesLock (fun () -> collectSstSources fromKey toKey)
            memSources @ sstSources |> List.rev |> List.toArray

    let collectRangeSources fromKey toKey =
        tryCollectRangeSources fromKey toKey rangeScanRetries 0

    let applyTransactionOps commitSeq ops =
        ops
        |> List.iter (fun (k, vOpt) ->
            match vOpt with
            | Some v ->
                wal.Put(commitSeq, k, v)
                memTable.Put(k, commitSeq, v)
            | None ->
                wal.Delete(commitSeq, k)
                memTable.Delete(k, commitSeq))

    let commitTransaction (ops: (string * string option) list) =
        let shouldFlush =
            LockExtensions.withReadLock mainLock (fun () ->
                if not ops.IsEmpty then
                    let commitSeq = snapshotManager.NextSequence()
                    wal.Begin commitSeq
                    applyTransactionOps commitSeq ops
                    wal.Commit(commitSeq, sync = true)

                memTable.SizeBytes >= memTableLimit)

        if shouldFlush then
            flushMemTable ()

    let rollbackTransaction () =
        let seq = snapshotManager.NextSequence()
        wal.Abort(seq, false)

    member _.Snapshot() = snapshotManager.AcquireSnapshot()

    member this.BeginTransaction() =
        let snap = snapshotManager.AcquireSnapshot()
        new LsmTransaction(this :> ILsmTree, snap) :> ITransaction

    member _.Put(key, value) = putDirect key value

    member _.Delete key = deleteDirect key

    member _.Get key =
        LsmTreeSearch.findValue
            mainLock
            memTable
            immutableMemTable
            ssTablesLock
            ssTables
            key
            (snapshotManager.CurrentSequence())

    member _.Get(key, snapshot: SnapshotHandle) =
        LsmTreeSearch.findValue mainLock memTable immutableMemTable ssTablesLock ssTables key snapshot.Seq

    member _.Flush() =
        flushMemTable ()
        flushCoordinator.WaitForCompletion()
        LockExtensions.checkCoordinatorError flushCoordinator ssTablesLock "Flush failed"

    member _.FlushAsync() =
        async {
            flushMemTable ()
            do! flushCoordinator.AwaitCompletion()
            LockExtensions.checkCoordinatorError flushCoordinator ssTablesLock "Flush failed"
        }

    member _.WaitForCompaction() =
        compaction.WaitForCompletion()
        LockExtensions.checkCoordinatorError compaction ssTablesLock "Compaction failed"

    member _.WaitForCompactionAsync() =
        async {
            do! compaction.AwaitCompletion()
            LockExtensions.checkCoordinatorError compaction ssTablesLock "Compaction failed"
        }

    member _.ReleaseSnapshot snapshot =
        snapshotManager.ReleaseSnapshot snapshot

    member _.NewIterator(fromKey, toKey, ?snapshot: SnapshotHandle) =
        if isNull fromKey then
            nullArg "fromKey"

        if isNull toKey then
            nullArg "toKey"

        let snap =
            match snapshot with
            | Some handle -> handle.Seq
            | None -> snapshotManager.CurrentSequence()

        snapshotManager.RegisterSnapshot snap
        let sources = collectRangeSources fromKey toKey
        new RangeIterator(snapshotManager, sources, snap) :> IIterator

    member this.RangeScan(fromKey, toKey, ?snapshot) =
        seq {
            use it = this.NewIterator(fromKey, toKey, ?snapshot = snapshot)

            while it.MoveNext() do
                yield it.Current
        }

    member this.Close() = (this :> System.IDisposable).Dispose()

    interface System.IDisposable with
        member _.Dispose() =
            if not disposed then
                disposed <- true
                compaction.Cancel()

                try
                    compaction.WaitForCompletion()
                    LockExtensions.logCoordinatorError compaction ssTablesLock "compaction"
                with _ ->
                    ()

                try
                    flushCoordinator.WaitForCompletion()
                    LockExtensions.logCoordinatorError flushCoordinator ssTablesLock "flush"
                with _ ->
                    ()

                LockExtensions.disposeOf wal
                LockExtensions.disposeOf mainLock

                ssTables
                |> Array.iter (fun level -> level |> Seq.iter (fun sst -> LockExtensions.disposeOf sst))

                LockExtensions.disposeOf compaction
                LockExtensions.disposeOf flushCoordinator

    interface ILsmTree with
        member this.Get(key, snapshot) = this.Get(key, snapshot)
        member _.CommitTransaction ops = commitTransaction ops
        member _.RollbackTransaction() = rollbackTransaction ()
