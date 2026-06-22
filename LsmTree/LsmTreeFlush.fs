namespace LsmTree

type CompactionCoordinator() =
    let cts = new System.Threading.CancellationTokenSource()
    let mutable completedEvent = new System.Threading.ManualResetEvent true
    let mutable error: exn option = None
    let mutable disposed = false
    member val IsCompacting = false with get, set
    member val Token = cts.Token with get

    member _.Error
        with get () = error
        and set v = error <- v

    member _.Cancel() = cts.Cancel()
    member _.ResetCompletion() = completedEvent.Reset() |> ignore

    member _.SetCompleted() =
        if not disposed then
            completedEvent.Set() |> ignore

    member _.WaitForCompletion() = completedEvent.WaitOne() |> ignore

    member _.AwaitCompletion() =
        Async.AwaitWaitHandle completedEvent |> Async.Ignore

    interface ICoordinatorError with
        member _.Error
            with get () = error
            and set v = error <- v

    interface System.IDisposable with
        member _.Dispose() =
            if not disposed then
                disposed <- true
                cts.Dispose()
                (completedEvent :> System.IDisposable).Dispose()

type FlushCoordinator() =
    let flushLock = obj ()
    let mutable completedEvent = new System.Threading.ManualResetEvent true
    let mutable error: exn option = None
    let mutable disposed = false

    member _.Error
        with get () = error
        and set v = error <- v

    interface ICoordinatorError with
        member _.Error
            with get () = error
            and set v = error <- v

    member _.AcquireAndReset() =
        completedEvent.WaitOne() |> ignore

        lock flushLock (fun () ->
            if not disposed then
                completedEvent.Reset() |> ignore
                true
            else
                false)

    member _.SignalCompleted() =
        lock flushLock (fun () ->
            if not disposed then
                completedEvent.Set() |> ignore)

    member _.WaitForCompletion() =
        try
            completedEvent.WaitOne() |> ignore
        with :? System.ObjectDisposedException ->
            ()

    member _.AwaitCompletion() =
        Async.AwaitWaitHandle completedEvent |> Async.Ignore

    interface System.IDisposable with
        member _.Dispose() =
            lock flushLock (fun () ->
                if not disposed then
                    disposed <- true
                    (completedEvent :> System.IDisposable).Dispose())

module LsmTreeFlush =
    let timestamp () =
        System.DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()

    let newGuid () = System.Guid.NewGuid().ToString "N"

    let ssTablePath dataDir level =
        System.IO.Path.Combine(dataDir, $"L{level}_{timestamp ()}_{newGuid ()}.sst")

    let swapMemTableAndWal dataDir (memTable: MemTable) (wal: WAL) walPath =
        if memTable.SizeBytes > 0 then
            let oldMemTable = memTable
            let oldWalPath = System.IO.Path.Combine(dataDir, $"wal_{newGuid ()}.old")
            System.IO.File.Move(walPath, oldWalPath)
            wal.Close()
            Some(MemTable(), new WAL(walPath), oldMemTable, oldWalPath)
        else
            None

    let flushToSSTable dataDir (oldMemTable: MemTable) =
        SSTableWriter.write (ssTablePath dataDir 0) oldMemTable.Entries

    let addSSTable
        (mainLock: System.Threading.ReaderWriterLockSlim)
        ssTablesLock
        (ssTables: SSTable list[])
        (clearState: unit -> unit)
        ssTable
        =
        lock ssTablesLock (fun () -> ssTables.[0] <- ssTable :: ssTables.[0])
        LockExtensions.withWriteLock mainLock clearState

    let findMinKey (current: (string * int64 * string option) option[]) =
        current
        |> Array.fold
            (fun acc entry ->
                match entry with
                | Some(k, _, _) ->
                    match acc with
                    | Some mk -> Some(if System.String.CompareOrdinal(k, mk) < 0 then k else mk)
                    | None -> Some k
                | None -> acc)
            None

    let collectVersions advance (current: (string * int64 * string option) option[]) key =
        let versions = ResizeArray()

        for i in 0 .. current.Length - 1 do
            while current.[i] |> Option.exists (fun (k, _, _) -> k = key) do
                let _, seq, value = current.[i].Value
                versions.Add(seq, value)
                advance i

        versions

    let pruneVersions isLastLevel minSnap (versions: ResizeArray<int64 * string option>) =
        let sorted = versions |> Seq.sortByDescending fst |> Seq.toList
        let newer = sorted |> List.filter (fun (s, _) -> s >= minSnap)
        let older = sorted |> List.filter (fun (s, _) -> s < minSnap) |> List.tryHead

        let olderPruned =
            if isLastLevel then
                older |> Option.filter (fun (_, v) -> v.IsSome)
            else
                older

        match olderPruned with
        | Some o -> List.append newer [ o ]
        | None -> newer

    let mergeSortedEntriesData (tableData: (string * int64 * string option)[][]) isLastLevel minSnap =
        seq {
            let pos = Array.zeroCreate tableData.Length

            let entryAt i =
                if pos.[i] < tableData.[i].Length then
                    Some tableData.[i].[pos.[i]]
                else
                    None

            let current = Array.init tableData.Length entryAt

            let advance i =
                pos.[i] <- pos.[i] + 1
                current.[i] <- entryAt i

            let mutable running = true

            while running do
                match findMinKey current with
                | Some key ->
                    for s, v in collectVersions advance current key |> pruneVersions isLastLevel minSnap do
                        yield key, s, v
                | None -> running <- false
        }

    let mergeSortedEntries (tables: SSTable list) isLastLevel minSnap =
        let tableData = tables |> List.map (fun t -> t.GetAll()) |> List.toArray
        mergeSortedEntriesData tableData isLastLevel minSnap

    let mergeSSTables
        dataDir
        (compactLevelLimits: int[])
        (ct: System.Threading.CancellationToken)
        (tablesToCompact: SSTable list)
        level
        minSnap
        =
        let estimatedEntries = tablesToCompact |> List.sumBy (fun t -> t.Count)
        let isLastLevel = compactLevelLimits.Length = level + 1

        mergeSortedEntries (List.rev tablesToCompact) isLastLevel minSnap
        |> SSTableWriter.writeStream (ssTablePath dataDir (level + 1)) ct estimatedEntries

    let mergeAndCreateSSTable
        dataDir
        (snapshotManager: LsmTreeSnapshot)
        compactLevelLimits
        (ct: System.Threading.CancellationToken)
        tablesToCompact
        level
        =
        ct.ThrowIfCancellationRequested()
        let minSnap = snapshotManager.GetMinActiveSnapshot()
        mergeSSTables dataDir compactLevelLimits ct tablesToCompact level minSnap

    let replaceLevelTables ssTablesLock (ssTables: SSTable list[]) level tablesToCompact newSSTable =
        lock ssTablesLock (fun () ->
            ssTables.[level + 1] <- newSSTable :: ssTables.[level + 1]

            let remaining =
                ssTables.[level] |> List.filter (fun t -> not (List.contains t tablesToCompact))

            ssTables.[level] <- remaining)

    let cleanupSSTables (tablesToCompact: SSTable list) =
        let cleanupErrors =
            tablesToCompact
            |> List.collect (fun t ->
                try
                    (t :> System.IDisposable).Dispose()

                    if System.IO.File.Exists t.Path then
                        System.IO.File.Delete t.Path

                    []
                with e ->
                    [ e ])

        if not (List.isEmpty cleanupErrors) then
            raise (System.AggregateException("Compaction cleanup failed", cleanupErrors))

    let performMerge
        dataDir
        (snapshotManager: LsmTreeSnapshot)
        ssTablesLock
        (ssTables: SSTable list[])
        compactLevelLimits
        (ct: System.Threading.CancellationToken)
        tablesToCompact
        level
        =
        mergeAndCreateSSTable dataDir snapshotManager compactLevelLimits ct tablesToCompact level
        |> replaceLevelTables ssTablesLock ssTables level tablesToCompact

        cleanupSSTables tablesToCompact

    [<TailCall>]
    let rec compact
        dataDir
        snapshotManager
        ssTablesLock
        (ssTables: SSTable list[])
        (compactLevelLimits: int[])
        (ct: System.Threading.CancellationToken)
        level
        =
        ct.ThrowIfCancellationRequested()

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
            performMerge dataDir snapshotManager ssTablesLock ssTables compactLevelLimits ct tablesToCompact level
            compact dataDir snapshotManager ssTablesLock ssTables compactLevelLimits ct (level + 1)

    let triggerCompaction
        dataDir
        snapshotManager
        ssTablesLock
        ssTables
        compactLevelLimits
        (compaction: CompactionCoordinator)
        =
        let shouldStart =
            lock ssTablesLock (fun () ->
                if not compaction.IsCompacting && not compaction.Token.IsCancellationRequested then
                    compaction.IsCompacting <- true
                    compaction.ResetCompletion()
                    true
                else
                    false)

        if shouldStart then
            try
                try
                    compact dataDir snapshotManager ssTablesLock ssTables compactLevelLimits compaction.Token 0
                with
                | :? System.OperationCanceledException -> ()
                | ex -> lock ssTablesLock (fun () -> compaction.Error <- Some ex)
            finally
                lock ssTablesLock (fun () ->
                    compaction.IsCompacting <- false
                    compaction.SetCompleted())

    let flushAndRegisterSSTable dataDir mainLock ssTablesLock ssTables clearState oldMemTable oldWalPath =
        flushToSSTable dataDir oldMemTable
        |> addSSTable mainLock ssTablesLock ssTables clearState

        if System.IO.File.Exists oldWalPath then
            System.IO.File.Delete oldWalPath

    let asyncFlushToSSTable
        (mainLock: System.Threading.ReaderWriterLockSlim)
        dataDir
        (snapshotManager: LsmTreeSnapshot)
        ssTablesLock
        (ssTables: SSTable list[])
        (compactLevelLimits: int[])
        (compaction: CompactionCoordinator)
        (flushCoordinator: FlushCoordinator)
        (oldMemTable: MemTable)
        (oldWalPath: string)
        (clearState: unit -> unit)
        =
        async {
            try
                try
                    flushAndRegisterSSTable dataDir mainLock ssTablesLock ssTables clearState oldMemTable oldWalPath
                    triggerCompaction dataDir snapshotManager ssTablesLock ssTables compactLevelLimits compaction
                with ex ->
                    lock ssTablesLock (fun () -> flushCoordinator.Error <- Some ex)
            finally
                flushCoordinator.SignalCompleted()
        }
