namespace LsmTree

module LsmTreeSearch =
    let searchInTables ssTablesLock (ssTables: list<SSTable>[]) key snap level =
        lock ssTablesLock (fun () -> ssTables.[level])
        |> List.tryPick (fun t -> t.Get(key, snap))

    [<TailCall>]
    let rec searchLevel ssTablesLock (ssTables: list<SSTable>[]) key snap level =
        if level >= ssTables.Length then
            None
        else
            match searchInTables ssTablesLock ssTables key snap level with
            | Some res -> Some res
            | None -> searchLevel ssTablesLock ssTables key snap (level + 1)

    let findValue
        (mainLock: System.Threading.ReaderWriterLockSlim)
        ssTablesLock
        (memTable: MemTable)
        (immutableMemTable: MemTable option)
        ssTables
        key
        snap
        =
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
                match searchLevel ssTablesLock ssTables key snap 0 with
                | Some(Some v) -> Some v
                | _ -> None
