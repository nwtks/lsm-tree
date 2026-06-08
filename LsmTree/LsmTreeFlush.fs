namespace LsmTree

type CompactionCoordinator() =
    let completedEvent = new System.Threading.ManualResetEvent(true)
    member val IsCompacting = false with get, set
    member val Error: exn option = None with get, set
    member _.CompletedEvent = completedEvent

    interface System.IDisposable with
        member _.Dispose() =
            (completedEvent :> System.IDisposable).Dispose()

module LsmTreeFlush =
    let timestamp () =
        System.DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()

    let newGuid () = System.Guid.NewGuid().ToString "N"

    let ssTablePath dataDir level =
        System.IO.Path.Combine(dataDir, $"L{level}_{timestamp ()}_{newGuid ()}.sst")

    let swapMemTableAndWal
        (mainLock: System.Threading.ReaderWriterLockSlim)
        dataDir
        (memTable: MemTable)
        (wal: WAL)
        walPath
        (swapState: MemTable -> WAL -> MemTable -> unit)
        =
        LockExtensions.withWriteLock mainLock (fun () ->
            if memTable.SizeBytes > 0 then
                let oldMemTable = memTable
                wal.Close()
                let oldWalPath = System.IO.Path.Combine(dataDir, $"wal_{newGuid ()}.old")
                System.IO.File.Move(walPath, oldWalPath)
                swapState (MemTable()) (new WAL(walPath)) oldMemTable
                Some(oldMemTable, oldWalPath)
            else
                None)

    let flushToSSTable dataDir (oldMemTable: MemTable) =
        SSTableWriter.write (ssTablePath dataDir 0) oldMemTable.Entries

    let addSSTable
        (mainLock: System.Threading.ReaderWriterLockSlim)
        ssTablesLock
        (ssTables: SSTable list[])
        (clearState: unit -> unit)
        sst
        =
        lock ssTablesLock (fun () -> ssTables.[0] <- sst :: ssTables.[0])
        LockExtensions.withWriteLock mainLock clearState

    let findMinKey (current: (string * int64 * string option) option[]) =
        (None, current)
        ||> Array.fold (fun acc entry ->
            match entry with
            | Some(k, _, _) ->
                match acc with
                | Some mk -> Some(if System.String.CompareOrdinal(k, mk) < 0 then k else mk)
                | None -> Some k
            | None -> acc)

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

    let mergeSortedEntries (tables: SSTable list) isLastLevel minSnap =
        seq {
            let tableData =
                tables |> List.map (fun t -> t.GetAll() |> Seq.toArray) |> List.toArray

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

    let mergeSSTables dataDir (tablesToCompact: SSTable list) (compactLevelLimits: int[]) level minSnap =
        let estimatedEntries = tablesToCompact |> List.sumBy (fun t -> t.Count)
        let isLastLevel = compactLevelLimits.Length = level + 1

        mergeSortedEntries (List.rev tablesToCompact) isLastLevel minSnap
        |> SSTableWriter.writeStream (ssTablePath dataDir (level + 1)) estimatedEntries

    let performMerge
        dataDir
        (snapshotManager: LsmTreeSnapshot)
        ssTablesLock
        (ssTables: SSTable list[])
        tablesToCompact
        compactLevelLimits
        level
        =
        let minSnap = snapshotManager.GetMinActiveSnapshot()

        let newSSTable =
            mergeSSTables dataDir tablesToCompact compactLevelLimits level minSnap

        lock ssTablesLock (fun () ->
            ssTables.[level + 1] <- newSSTable :: ssTables.[level + 1]

            let remaining =
                ssTables.[level] |> List.filter (fun t -> not (List.contains t tablesToCompact))

            ssTables.[level] <- remaining)

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

    [<TailCall>]
    let rec compact dataDir snapshotManager ssTablesLock (ssTables: SSTable list[]) (compactLevelLimits: int[]) level =
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
            performMerge dataDir snapshotManager ssTablesLock ssTables tablesToCompact compactLevelLimits level
            compact dataDir snapshotManager ssTablesLock ssTables compactLevelLimits (level + 1)

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
                if not compaction.IsCompacting then
                    compaction.IsCompacting <- true
                    compaction.CompletedEvent.Reset() |> ignore
                    true
                else
                    false)

        if shouldStart then
            System.Threading.Tasks.Task.Run(fun () ->
                try
                    try
                        compact dataDir snapshotManager ssTablesLock ssTables compactLevelLimits 0
                    with ex ->
                        lock ssTablesLock (fun () -> compaction.Error <- Some ex)
                finally
                    lock ssTablesLock (fun () ->
                        compaction.IsCompacting <- false
                        compaction.CompletedEvent.Set() |> ignore))
            |> ignore

    let waitForCompaction (compaction: CompactionCoordinator) =
        compaction.CompletedEvent.WaitOne() |> ignore
