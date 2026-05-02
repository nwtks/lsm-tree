namespace LsmTree

open System.IO
open System.Threading
open System.Threading.Tasks

type ITransaction =
    inherit System.IDisposable
    abstract member Put: key: string * value: string -> unit
    abstract member Delete: key: string -> unit
    abstract member Get: key: string -> string option
    abstract member Commit: unit -> unit
    abstract member Rollback: unit -> unit

type LsmTree(dataDir: string, ?memTableSizeLimit: int, ?syncOnCommit: bool, ?compactLevelLimits: int[]) =
    let memTableLimit = defaultArg memTableSizeLimit (1024 * 1024)
    let syncOnCommit = defaultArg syncOnCommit true
    let compactLevelLimits = defaultArg compactLevelLimits [| 4; 10; 100; 1000 |]
    let walPath = Path.Combine(dataDir, "wal.log")
    let mutable memTable = MemTable()
    let mutable immutableMemTable: MemTable option = None
    let mainLock = new ReaderWriterLockSlim()

    let ssTables =
        Array.init (compactLevelLimits.Length + 1) (fun _ -> list<SSTable>.Empty)

    let mutable isCompacting = false
    let ssTablesLock = obj ()

    let mutable globalSeq = 0L
    let mutable activeSnapshots = Set.empty<int64>
    let activeSnapshotsLock = obj ()

    let releaseSnapshot (snapshot: int64) =
        lock activeSnapshotsLock (fun () -> activeSnapshots <- Set.remove snapshot activeSnapshots)

    let getMinActiveSnapshot () =
        lock activeSnapshotsLock (fun () ->
            if Set.isEmpty activeSnapshots then
                Interlocked.Read(&globalSeq)
            else
                Set.minElement activeSnapshots)

    let parseSstLevel (path: string) =
        let name = Path.GetFileName path

        if name.StartsWith "L" then
            System.Int32.Parse(name.Substring(1, name.IndexOf '_' - 1))
        else
            0

    let loadSSTables () =
        Directory.GetFiles(dataDir, "*.sst")
        |> Array.iter (fun path ->
            let level = parseSstLevel path

            if level < ssTables.Length then
                ssTables.[level] <- new SSTable(path) :: ssTables.[level])

        for i = 0 to ssTables.Length - 1 do
            ssTables.[i] <- ssTables.[i] |> List.sortByDescending (fun t -> t.Path)

    let loadWal () =
        let logs = Directory.GetFiles(dataDir, "wal*.log")
        let olds = Directory.GetFiles(dataDir, "wal*.old")

        Array.append logs olds
        |> Seq.sort
        |> Seq.collect WALRecovery.recover
        |> Seq.sortBy (fun (seq, _, _) -> seq)
        |> Seq.iter (function
            | seq, k, Some v ->
                memTable.Put(k, seq, v)
                globalSeq <- max globalSeq seq
            | seq, k, None ->
                memTable.Delete(k, seq)
                globalSeq <- max globalSeq seq)

    let startup dataDir =
        if not (Directory.Exists dataDir) then
            Directory.CreateDirectory dataDir |> ignore

        loadSSTables ()
        loadWal ()

    do startup dataDir
    let mutable wal = new WAL(walPath)

    let timestamp () =
        System.DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()

    let newGuid () = System.Guid.NewGuid().ToString "N"

    let ssTablePath level =
        Path.Combine(dataDir, sprintf "L%d_%d_%s.sst" level (timestamp ()) (newGuid ()))

    let collectKeyVersions isLastLevel minSnap (key, versions: seq<string * int64 * string option>) =
        let sorted =
            versions
            |> Seq.map (fun (_, seq, value) -> seq, value)
            |> Seq.sortByDescending fst
            |> Seq.toList

        let newer = sorted |> List.filter (fun (s, _) -> s >= minSnap)
        let older = sorted |> List.filter (fun (s, _) -> s < minSnap) |> List.tryHead

        let kept =
            match older with
            | Some o -> List.append newer [ o ]
            | None -> newer

        if isLastLevel then
            kept |> List.filter (fun (_, v) -> v.IsSome)
        else
            kept
        |> Seq.map (fun (s, v) -> key, s, v)

    let mergeSSTables level (tablesToCompact: SSTable list) minSnap =
        tablesToCompact
        |> List.rev
        |> Seq.collect (fun t -> t.GetAll())
        |> Seq.groupBy (fun (key, _, _) -> key)
        |> Seq.collect (collectKeyVersions (compactLevelLimits.Length = level + 1) minSnap)
        |> Seq.sortWith (fun (k1, s1, _) (k2, s2, _) ->
            let c = System.String.CompareOrdinal(k1, k2)
            if c <> 0 then c else s2.CompareTo s1)
        |> Seq.toList
        |> SSTableWriter.flush (ssTablePath (level + 1))

    let performMerge level tablesToCompact =
        let minSnap = getMinActiveSnapshot ()
        let newSSTable = mergeSSTables level tablesToCompact minSnap

        lock ssTablesLock (fun () ->
            ssTables.[level + 1] <- newSSTable :: ssTables.[level + 1]

            let remaining =
                ssTables.[level] |> List.filter (fun t -> not (List.contains t tablesToCompact))

            ssTables.[level] <- remaining)

        tablesToCompact
        |> List.iter (fun t ->
            try
                (t :> System.IDisposable).Dispose()

                if File.Exists t.Path then
                    File.Delete t.Path
            with e ->
                printfn "Compaction: Failed to cleanup old SSTable %s: %s" t.Path e.Message)

    [<TailCall>]
    let rec compact level =
        let tablesToCompact =
            lock ssTablesLock (fun () ->
                if
                    level < ssTables.Length - 1
                    && ssTables.[level].Length > compactLevelLimits.[level]
                then
                    ssTables.[level]
                else
                    [])

        if tablesToCompact.Length > 0 then
            performMerge level tablesToCompact
            compact (level + 1)

    let triggerCompaction () =
        let shouldStart =
            lock ssTablesLock (fun () ->
                if not isCompacting then
                    isCompacting <- true
                    true
                else
                    false)

        if shouldStart then
            Task.Run(fun () ->
                try
                    compact 0
                finally
                    lock ssTablesLock (fun () -> isCompacting <- false))
            |> ignore

    let swapMemTableAndWal () =
        mainLock.EnterWriteLock()

        try
            if memTable.SizeBytes > 0 then
                let oldMemTable = memTable
                wal.Close()
                let oldWalPath = Path.Combine(dataDir, sprintf "wal_%s.old" (newGuid ()))
                File.Move(walPath, oldWalPath)
                memTable <- MemTable()
                wal <- new WAL(walPath)
                immutableMemTable <- Some oldMemTable
                Some(oldMemTable, oldWalPath)
            else
                None
        finally
            mainLock.ExitWriteLock()

    let addSSTable (oldMemTable: MemTable) =
        let sst = SSTableWriter.flush (ssTablePath 0) oldMemTable.Entries
        lock ssTablesLock (fun () -> ssTables.[0] <- sst :: ssTables.[0])
        mainLock.EnterWriteLock()

        try
            immutableMemTable <- None
        finally
            mainLock.ExitWriteLock()

    let flushMemTable () =
        match swapMemTableAndWal () with
        | Some(oldMemTable, oldWalPath) ->
            addSSTable oldMemTable

            if File.Exists oldWalPath then
                File.Delete oldWalPath

            triggerCompaction ()
        | None -> ()

    let searchInTables key snap level =
        lock ssTablesLock (fun () -> ssTables.[level])
        |> List.tryPick (fun t -> t.Get(key, snap))

    [<TailCall>]
    let rec searchLevel key snap level =
        if level >= ssTables.Length then
            None
        else
            match searchInTables key snap level with
            | Some res -> Some res
            | None -> searchLevel key snap (level + 1)

    let findValue key snap =
        let memRes, immRes =
            mainLock.EnterReadLock()

            try
                memTable.Get(key, snap),
                match immutableMemTable with
                | Some m -> m.Get(key, snap)
                | None -> None
            finally
                mainLock.ExitReadLock()

        match memRes with
        | Some(Some v) -> Some v
        | Some None -> None
        | None ->
            match immRes with
            | Some(Some v) -> Some v
            | Some None -> None
            | None ->
                match searchLevel key snap 0 with
                | Some(Some v) -> Some v
                | _ -> None

    [<TailCall>]
    let rec wait () =
        let active = lock ssTablesLock (fun () -> isCompacting)

        if active then
            Thread.Sleep 50
            wait ()

    member _.Snapshot() = Interlocked.Read(&globalSeq)

    member this.BeginTransaction() =
        let snap = this.Snapshot()
        lock activeSnapshotsLock (fun () -> activeSnapshots <- Set.add snap activeSnapshots)
        new LsmTransaction(this, snap) :> ITransaction

    member this.Put(key: string, value: string) =
        let tx = this.BeginTransaction()
        tx.Put(key, value)
        tx.Commit()

    member this.Delete(key: string) =
        let tx = this.BeginTransaction()
        tx.Delete key
        tx.Commit()

    member this.Get(key: string, ?snapshot: int64) =
        defaultArg snapshot (this.Snapshot()) |> findValue key

    member _.Flush() = flushMemTable ()

    member _.WaitForCompaction() = wait ()

    member _.SyncOnCommit = syncOnCommit

    member _.ReleaseSnapshot(snapshot: int64) = releaseSnapshot snapshot

    member this.Close() = (this :> System.IDisposable).Dispose()

    interface System.IDisposable with
        member _.Dispose() =
            wait ()
            wal.Close()
            mainLock.Dispose()

            ssTables
            |> Array.iter (fun level -> level |> Seq.iter (fun sst -> (sst :> System.IDisposable).Dispose()))

    member internal this.CommitTransaction(ops: (string * string option) list) =
        let shouldFlush =
            mainLock.EnterReadLock()

            try
                if not ops.IsEmpty then
                    let commitSeq = Interlocked.Increment(&globalSeq)
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
            this.Flush()

and LsmTransaction(lsm: LsmTree, snapshot: int64) =
    let mutable ops = []
    let mutable finished = false

    let checkFinished () =
        if finished then
            failwith "Transaction already finished."

    interface ITransaction with
        member _.Put(key, value) =
            checkFinished ()
            ops <- (key, Some value) :: ops

        member _.Delete key =
            checkFinished ()
            ops <- (key, None) :: ops

        member _.Get key =
            checkFinished ()
            let local = ops |> Seq.tryFind (fun (k, _) -> k = key)

            match local with
            | Some(_, Some v) -> Some v
            | Some(_, None) -> None
            | None -> lsm.Get(key, snapshot)

        member this.Commit() =
            checkFinished ()
            lsm.CommitTransaction(ops |> Seq.rev |> Seq.toList)
            (this :> ITransaction).Dispose()

        member this.Rollback() =
            checkFinished ()
            ops <- []
            (this :> ITransaction).Dispose()

        member _.Dispose() =
            if not finished then
                lsm.ReleaseSnapshot snapshot
                finished <- true
