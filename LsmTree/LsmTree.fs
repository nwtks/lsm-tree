namespace LsmTree

type LsmTree(dataDir: string, ?memTableSizeLimit: int, ?syncOnCommit: bool, ?compactLevelLimits: int[]) =
    let memTableLimit = defaultArg memTableSizeLimit (1024 * 1024)
    let syncOnCommit = defaultArg syncOnCommit true
    let compactLevelLimits = defaultArg compactLevelLimits [| 4; 10; 100; 1000 |]
    let walPath = System.IO.Path.Combine(dataDir, "wal.log")
    let mutable memTable = MemTable()
    let mutable immutableMemTable: MemTable option = None
    let mainLock = new System.Threading.ReaderWriterLockSlim()
    let snapshotManager = LsmTreeSnapshot()

    let ssTables =
        Array.init (compactLevelLimits.Length + 1) (fun _ -> list<SSTable>.Empty)

    let compaction = CompactionCoordinator()
    let ssTablesLock = obj ()

    let parseSstLevel (path: string) =
        let name = System.IO.Path.GetFileName path

        if name.StartsWith "L" then
            System.Int32.Parse(name.Substring(1, name.IndexOf '_' - 1))
        else
            0

    let loadSSTables () =
        let mutable maxSeq = 0L

        System.IO.Directory.GetFiles(dataDir, "*.sst")
        |> Array.iter (fun path ->
            let level = parseSstLevel path

            if level < ssTables.Length then
                let sst = new SSTable(path)
                ssTables.[level] <- sst :: ssTables.[level]

                for _, seq, _ in sst.GetAll() do
                    if seq > maxSeq then
                        maxSeq <- seq)

        for i = 0 to ssTables.Length - 1 do
            ssTables.[i] <- ssTables.[i] |> List.sortByDescending (fun t -> t.Path)

        if maxSeq > 0L then
            snapshotManager.AdvanceSequence maxSeq

    let loadWal () =
        let logs = System.IO.Directory.GetFiles(dataDir, "wal*.log")
        let olds = System.IO.Directory.GetFiles(dataDir, "wal*.old")

        Array.append logs olds
        |> Seq.sort
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
        match
            LsmTreeFlush.swapMemTableAndWal mainLock dataDir memTable wal walPath (fun newMt newWal oldMt ->
                memTable <- newMt
                wal <- newWal
                immutableMemTable <- Some oldMt)
        with
        | Some(oldMemTable, oldWalPath) ->
            LsmTreeFlush.flushToSSTable dataDir oldMemTable
            |> LsmTreeFlush.addSSTable mainLock ssTablesLock ssTables (fun () -> immutableMemTable <- None)

            if System.IO.File.Exists oldWalPath then
                System.IO.File.Delete oldWalPath

            LsmTreeFlush.triggerCompaction dataDir snapshotManager ssTablesLock ssTables compactLevelLimits compaction
        | None -> ()

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

        shouldFlush

    member _.Snapshot() = snapshotManager.CurrentSequence()

    member this.BeginTransaction() =
        let snap = this.Snapshot()
        snapshotManager.RegisterSnapshot snap
        new LsmTransaction(this :> ILsmTree, snap) :> ITransaction

    member this.Put(key: string, value: string) =
        let tx = this.BeginTransaction()
        tx.Put(key, value)
        tx.Commit()

    member this.Delete(key: string) =
        let tx = this.BeginTransaction()
        tx.Delete key
        tx.Commit()

    member this.Get(key: string, ?snapshot: int64) =
        defaultArg snapshot (this.Snapshot())
        |> LsmTreeSearch.findValue mainLock memTable immutableMemTable ssTablesLock ssTables key

    member _.Flush() = flushMemTable ()

    member _.WaitForCompaction() =
        LsmTreeFlush.waitForCompaction ssTablesLock compaction

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
            LsmTreeFlush.waitForCompaction ssTablesLock compaction

            lock ssTablesLock (fun () ->
                match compaction.Error with
                | Some ex -> raise (System.AggregateException("Compaction failed", ex))
                | None -> ())

            wal.Close()
            mainLock.Dispose()

            ssTables
            |> Array.iter (fun level -> level |> Seq.iter (fun sst -> (sst :> System.IDisposable).Dispose()))

    interface ILsmTree with
        member this.Get(key, snapshot) = this.Get(key, ?snapshot = snapshot)
        member _.CommitTransaction ops = commitTransaction ops

        member _.ReleaseSnapshot snapshot =
            snapshotManager.ReleaseSnapshot snapshot
