namespace LsmTree

module LsmTreeSearch =
    let searchInTables ssTablesLock (ssTables: SSTable list[]) key snap level =
        lock ssTablesLock (fun () -> ssTables.[level] |> List.tryPick (fun t -> t.Get(key, snap)))

    [<TailCall>]
    let rec searchLevel ssTablesLock (ssTables: SSTable list[]) key snap level =
        if level >= ssTables.Length then
            None
        else
            match searchInTables ssTablesLock ssTables key snap level with
            | Some res -> Some res
            | None -> searchLevel ssTablesLock ssTables key snap (level + 1)

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
                | None -> None)

        match memRes with
        | Some(Some v) -> Some v
        | Some None -> None
        | None ->
            match immRes with
            | Some(Some v) -> Some v
            | Some None -> None
            | None -> searchLevel ssTablesLock ssTables key snap 0
