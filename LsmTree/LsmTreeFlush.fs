namespace LsmTree

module LsmTreeFlush =
    let timestamp () =
        System.DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()

    let newGuid () = System.Guid.NewGuid().ToString "N"

    let ssTablePath dataDir level =
        System.IO.Path.Combine(dataDir, sprintf "L%d_%d_%s.sst" level (timestamp ()) (newGuid ()))

    let swapMemTableAndWal
        (mainLock: System.Threading.ReaderWriterLockSlim)
        (memTable: MemTable)
        (wal: WAL)
        walPath
        dataDir
        (swapState: MemTable -> WAL -> MemTable -> unit)
        =
        mainLock.EnterWriteLock()

        try
            if memTable.SizeBytes > 0 then
                let oldMemTable = memTable
                wal.Close()

                let oldWalPath = System.IO.Path.Combine(dataDir, sprintf "wal_%s.old" (newGuid ()))

                System.IO.File.Move(walPath, oldWalPath)
                swapState (MemTable()) (new WAL(walPath)) oldMemTable
                Some(oldMemTable, oldWalPath)
            else
                None
        finally
            mainLock.ExitWriteLock()

    let flushToSSTable dataDir (oldMemTable: MemTable) =
        SSTableWriter.flush (ssTablePath dataDir 0) oldMemTable.Entries

    let addSSTable
        (mainLock: System.Threading.ReaderWriterLockSlim)
        ssTablesLock
        (ssTables: list<SSTable>[])
        (clearState: unit -> unit)
        sst
        =
        lock ssTablesLock (fun () -> ssTables.[0] <- sst :: ssTables.[0])
        mainLock.EnterWriteLock()

        try
            clearState ()
        finally
            mainLock.ExitWriteLock()

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

    let mergeSSTables (compactLevelLimits: int[]) dataDir level (tablesToCompact: SSTable list) minSnap =
        tablesToCompact
        |> List.rev
        |> Seq.collect (fun t -> t.GetAll())
        |> Seq.groupBy (fun (key, _, _) -> key)
        |> Seq.collect (collectKeyVersions (compactLevelLimits.Length = level + 1) minSnap)
        |> Seq.sortWith (fun (k1, s1, _) (k2, s2, _) ->
            let c = System.String.CompareOrdinal(k1, k2)

            if c <> 0 then c else s2.CompareTo s1)
        |> Seq.toList
        |> SSTableWriter.flush (ssTablePath dataDir (level + 1))

    let performMerge
        (ssTables: SSTable list[])
        ssTablesLock
        compactLevelLimits
        dataDir
        (snapshotManager: LsmTreeSnapshot)
        level
        tablesToCompact
        =
        let minSnap = snapshotManager.GetMinActiveSnapshot()

        let newSSTable =
            mergeSSTables compactLevelLimits dataDir level tablesToCompact minSnap

        lock ssTablesLock (fun () ->
            ssTables.[level + 1] <- newSSTable :: ssTables.[level + 1]

            let remaining =
                ssTables.[level] |> List.filter (fun t -> not (List.contains t tablesToCompact))

            ssTables.[level] <- remaining)

        tablesToCompact
        |> List.iter (fun t ->
            try
                (t :> System.IDisposable).Dispose()

                if System.IO.File.Exists t.Path then
                    System.IO.File.Delete t.Path
            with e ->
                printfn "Compaction: Failed to cleanup old SSTable %s: %s" t.Path e.Message)

    [<TailCall>]
    let rec compact (ssTables: SSTable list[]) ssTablesLock (compactLevelLimits: int[]) dataDir snapshotManager level =
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
            performMerge ssTables ssTablesLock compactLevelLimits dataDir snapshotManager level tablesToCompact
            compact ssTables ssTablesLock compactLevelLimits dataDir snapshotManager (level + 1)

    let triggerCompaction ssTables ssTablesLock (isCompacting: bool ref) compactLevelLimits dataDir snapshotManager =
        let shouldStart =
            lock ssTablesLock (fun () ->
                if not isCompacting.Value then
                    isCompacting.Value <- true
                    true
                else
                    false)

        if shouldStart then
            System.Threading.Tasks.Task.Run(fun () ->
                try
                    compact ssTables ssTablesLock compactLevelLimits dataDir snapshotManager 0
                finally
                    lock ssTablesLock (fun () -> isCompacting.Value <- false))
            |> ignore

    [<TailCall>]
    let rec waitForCompaction ssTablesLock (isCompacting: bool ref) =
        let active = lock ssTablesLock (fun () -> isCompacting.Value)

        if active then
            System.Threading.Thread.Sleep 50
            waitForCompaction ssTablesLock isCompacting
