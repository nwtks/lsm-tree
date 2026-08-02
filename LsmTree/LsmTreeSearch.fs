namespace LsmTree

module LsmTreeSearch =
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
