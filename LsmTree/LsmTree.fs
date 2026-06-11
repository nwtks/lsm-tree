namespace LsmTree

type LsmTree(dataDir: string, ?memTableSizeLimit: int, ?syncOnCommit: bool, ?compactLevelLimits: int[]) =
    let memTableLimit = defaultArg memTableSizeLimit (1024 * 1024)
    let syncOnCommit = defaultArg syncOnCommit true

    let compactLevelLimits =
        let limits = defaultArg compactLevelLimits [| 4; 10; 100; 1000 |]

        if limits.Length = 0 then
            invalidArg "compactLevelLimits" "compactLevelLimits must not be empty"

        if limits |> Array.exists (fun x -> x < 0) then
            invalidArg "compactLevelLimits" "compactLevelLimits must not contain negative values"

        limits

    let walPath = System.IO.Path.Combine(dataDir, "wal.log")
    let mutable memTable = MemTable()
    let mutable immutableMemTable: MemTable option = None
    let mainLock = new System.Threading.ReaderWriterLockSlim()
    let snapshotManager = LsmTreeSnapshot()

    let ssTables =
        Array.init (compactLevelLimits.Length + 1) (fun _ -> list<SSTable>.Empty)

    let ssTablesLock = obj ()
    let compaction = new CompactionCoordinator()
    let flushCoordinator = FlushCoordinator()
    let mutable disposed = false

    let parseSstLevel (path: string) =
        let name = System.IO.Path.GetFileName path

        if name.StartsWith "L" then
            System.Int32.Parse(name.Substring(1, name.IndexOf '_' - 1))
        else
            0

    let loadSSTables () =
        System.IO.Directory.GetFiles(dataDir, "*.sst.tmp")
        |> Array.iter System.IO.File.Delete

        let mutable maxSeq = 0L

        System.IO.Directory.GetFiles(dataDir, "*.sst")
        |> Array.iter (fun path ->
            let level = parseSstLevel path

            if level < ssTables.Length then
                let sst = new SSTable(path)
                ssTables.[level] <- sst :: ssTables.[level]

                if sst.MaxSeq > maxSeq then
                    maxSeq <- sst.MaxSeq)

        for i = 0 to ssTables.Length - 1 do
            ssTables.[i] <-
                ssTables.[i]
                |> List.sortWith (fun a b ->
                    let cmp = compare b.MaxSeq a.MaxSeq
                    if cmp <> 0 then cmp else compare a.Path b.Path)

        if maxSeq > 0L then
            snapshotManager.AdvanceSequence maxSeq

    let loadWal () =
        let logs = System.IO.Directory.GetFiles(dataDir, "wal*.log")
        let olds = System.IO.Directory.GetFiles(dataDir, "wal*.old")

        Array.append logs olds
        |> Seq.collect WALRecovery.recover
        |> Seq.sortBy (fun (seq, _, _) -> seq)
        |> Seq.iter (function
            | seq, k, Some v ->
                memTable.Put(k, seq, v)
                snapshotManager.AdvanceSequence seq
            | seq, k, None ->
                memTable.Delete(k, seq)
                snapshotManager.AdvanceSequence seq)

    do
        if not (System.IO.Directory.Exists dataDir) then
            System.IO.Directory.CreateDirectory dataDir |> ignore

    let mutable wal = new WAL(walPath)

    do
        loadSSTables ()
        loadWal ()

    let flushMemTable () =
        flushCoordinator.AcquireAndReset()

        match
            LsmTreeFlush.swapMemTableAndWal mainLock dataDir memTable wal walPath (fun newMt newWal oldMt ->
                memTable <- newMt
                wal <- newWal
                immutableMemTable <- Some oldMt)
        with
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

    let putDirect key value =
        let shouldFlush =
            LockExtensions.withReadLock mainLock (fun () ->
                let seq = snapshotManager.NextSequence()
                wal.PutSingle(seq, key, value, syncOnCommit)
                memTable.Put(key, seq, value)
                memTable.SizeBytes >= memTableLimit)

        if shouldFlush then
            flushMemTable ()

    let deleteDirect key =
        let shouldFlush =
            LockExtensions.withReadLock mainLock (fun () ->
                let seq = snapshotManager.NextSequence()
                wal.DeleteSingle(seq, key, syncOnCommit)
                memTable.Delete(key, seq)
                memTable.SizeBytes >= memTableLimit)

        if shouldFlush then
            flushMemTable ()

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

                    wal.Commit(commitSeq, sync = syncOnCommit)

                memTable.SizeBytes >= memTableLimit)

        if shouldFlush then
            flushMemTable ()

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

        lock ssTablesLock (fun () ->
            match flushCoordinator.Error with
            | Some ex ->
                flushCoordinator.Error <- None
                raise (System.AggregateException("Flush failed", ex))
            | None -> ())

    member _.WaitForCompaction() =
        LsmTreeFlush.waitForCompaction compaction

        lock ssTablesLock (fun () ->
            match compaction.Error with
            | Some ex ->
                compaction.Error <- None
                raise (System.AggregateException("Compaction failed", ex))
            | None -> ())

    member _.SyncOnCommit = syncOnCommit

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

                    lock ssTablesLock (fun () ->
                        match compaction.Error with
                        | Some ex ->
                            compaction.Error <- None
                            eprintfn $"[WARN] LsmTree: compaction error during dispose: {ex.Message}"
                        | None -> ())
                with _ ->
                    ()

                try
                    flushCoordinator.WaitForCompletion()

                    lock ssTablesLock (fun () ->
                        match flushCoordinator.Error with
                        | Some ex ->
                            flushCoordinator.Error <- None
                            eprintfn $"[WARN] LsmTree: flush error during dispose: {ex.Message}"
                        | None -> ())
                with _ ->
                    ()

                LockExtensions.disposeOf wal
                LockExtensions.disposeOf mainLock

                ssTables
                |> Array.iter (fun level -> level |> Seq.iter (fun sst -> LockExtensions.disposeOf sst))

                LockExtensions.disposeOf compaction

    interface ILsmTree with
        member this.Get(key, snapshot) = this.Get(key, ?snapshot = snapshot)
        member _.CommitTransaction ops = commitTransaction ops

        member _.ReleaseSnapshot snapshot =
            snapshotManager.ReleaseSnapshot snapshot
