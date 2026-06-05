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

    let isCompacting = ref false
    let ssTablesLock = obj ()

    let parseSstLevel (path: string) =
        let name = System.IO.Path.GetFileName path

        if name.StartsWith "L" then
            System.Int32.Parse(name.Substring(1, name.IndexOf '_' - 1))
        else
            0

    let loadSSTables () =
        System.IO.Directory.GetFiles(dataDir, "*.sst")
        |> Array.iter (fun path ->
            let level = parseSstLevel path

            if level < ssTables.Length then
                ssTables.[level] <- new SSTable(path) :: ssTables.[level])

        for i = 0 to ssTables.Length - 1 do
            ssTables.[i] <- ssTables.[i] |> List.sortByDescending (fun t -> t.Path)

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

    let startup dataDir =
        if not (System.IO.Directory.Exists dataDir) then
            System.IO.Directory.CreateDirectory dataDir |> ignore

        loadSSTables ()
        loadWal ()

    do startup dataDir
    let mutable wal = new WAL(walPath)

    let flushMemTable () =
        match
            LsmTreeFlush.swapMemTableAndWal mainLock memTable wal walPath dataDir (fun newMt newWal oldMt ->
                memTable <- newMt
                wal <- newWal
                immutableMemTable <- Some oldMt)
        with
        | Some(oldMemTable, oldWalPath) ->
            LsmTreeFlush.flushToSSTable dataDir oldMemTable
            |> LsmTreeFlush.addSSTable mainLock ssTablesLock ssTables (fun () -> immutableMemTable <- None)

            if System.IO.File.Exists oldWalPath then
                System.IO.File.Delete oldWalPath

            LsmTreeFlush.triggerCompaction ssTables ssTablesLock isCompacting compactLevelLimits dataDir snapshotManager
        | None -> ()

    let commitTransaction (ops: (string * string option) list) =
        let shouldFlush =
            mainLock.EnterReadLock()

            try
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

                memTable.SizeBytes >= memTableLimit
            finally
                mainLock.ExitReadLock()

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
        |> LsmTreeSearch.findValue mainLock ssTablesLock memTable immutableMemTable ssTables key

    member _.Flush() = flushMemTable ()

    member _.WaitForCompaction() =
        LsmTreeFlush.waitForCompaction ssTablesLock isCompacting

    member _.SyncOnCommit = syncOnCommit

    member _.ReleaseSnapshot(snapshot: int64) =
        snapshotManager.ReleaseSnapshot snapshot

    member this.Close() = (this :> System.IDisposable).Dispose()

    interface System.IDisposable with
        member _.Dispose() =
            LsmTreeFlush.waitForCompaction ssTablesLock isCompacting
            wal.Close()
            mainLock.Dispose()

            ssTables
            |> Array.iter (fun level -> level |> Seq.iter (fun sst -> (sst :> System.IDisposable).Dispose()))

    interface ILsmTree with
        member this.Get(key, snapshot) = this.Get(key, ?snapshot = snapshot)
        member _.CommitTransaction ops = commitTransaction ops

        member _.ReleaseSnapshot snapshot =
            snapshotManager.ReleaseSnapshot snapshot
