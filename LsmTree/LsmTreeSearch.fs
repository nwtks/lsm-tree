namespace LsmTree

module internal LsmTreeSearch =
    [<TailCall>]
    let rec searchInTable key snap =
        function
        | [] -> NotFound
        | t: SSTable :: rest ->
            match t.Get(key, snap) with
            | NotFound -> searchInTable key snap rest
            | r -> r

    let searchInTables ssTablesLock (ssTables: SSTable list[]) key snap level =
        lock ssTablesLock (fun () -> ssTables.[level]) |> searchInTable key snap

    [<TailCall>]
    let rec searchLevel ssTablesLock (ssTables: SSTable list[]) key snap level =
        if level >= ssTables.Length then
            NotFound
        else
            match searchInTables ssTablesLock ssTables key snap level with
            | NotFound -> searchLevel ssTablesLock ssTables key snap (level + 1)
            | r -> r

    let findValue
        (mainLock: System.Threading.ReaderWriterLockSlim)
        (memTable: MemTable)
        (immutableMemTable: MemTable option)
        ssTablesLock
        ssTables
        key
        snap
        =
        let memRes, immRes =
            LockExtensions.withReadLock mainLock (fun () ->
                memTable.Get(key, snap),
                match immutableMemTable with
                | Some m -> m.Get(key, snap)
                | None -> NotFound)

        match memRes with
        | Found v -> Some v
        | Tombstone -> None
        | NotFound ->
            match immRes with
            | Found v -> Some v
            | Tombstone -> None
            | NotFound ->
                match searchLevel ssTablesLock ssTables key snap 0 with
                | Found v -> Some v
                | _ -> None

    let collectMemSources (memTable: MemTable) (immutableMemTable: MemTable option) fromKey toKey =
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

    let collectSstSources (ssTables: SSTable list[]) fromKey toKey =
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

    let snapshotStable ssTablesLock ssTables (snapshot: SSTable list[]) =
        lock ssTablesLock (fun () ->
            Array.forall2
                (fun snapLevel curLevel -> System.Object.ReferenceEquals(snapLevel, curLevel))
                snapshot
                ssTables)

    [<TailCall>]
    let rec tryCollectRangeSources
        mainLock
        memTable
        immutableMemTable
        ssTablesLock
        ssTables
        fromKey
        toKey
        maxRetries
        attempt
        =
        let memSources =
            LockExtensions.withReadLock mainLock (fun () -> collectMemSources memTable immutableMemTable fromKey toKey)

        let snapshot = lock ssTablesLock (fun () -> Array.copy ssTables)
        let disposed, sstSources = collectSstSourcesFromSnapshot fromKey toKey snapshot

        if disposed then
            if attempt < maxRetries then
                tryCollectRangeSources
                    mainLock
                    memTable
                    immutableMemTable
                    ssTablesLock
                    ssTables
                    fromKey
                    toKey
                    maxRetries
                    (attempt + 1)
            else
                let sstSources =
                    lock ssTablesLock (fun () -> collectSstSources ssTables fromKey toKey)

                memSources @ sstSources |> List.rev |> List.toArray
        elif snapshotStable ssTablesLock ssTables snapshot then
            memSources @ sstSources |> List.rev |> List.toArray
        elif attempt < maxRetries then
            tryCollectRangeSources
                mainLock
                memTable
                immutableMemTable
                ssTablesLock
                ssTables
                fromKey
                toKey
                maxRetries
                (attempt + 1)
        else
            let sstSources =
                lock ssTablesLock (fun () -> collectSstSources ssTables fromKey toKey)

            memSources @ sstSources |> List.rev |> List.toArray

    let collectRangeSources mainLock memTable immutableMemTable ssTablesLock ssTables fromKey toKey rangeScanRetries =
        tryCollectRangeSources
            mainLock
            memTable
            immutableMemTable
            ssTablesLock
            ssTables
            fromKey
            toKey
            rangeScanRetries
            0
