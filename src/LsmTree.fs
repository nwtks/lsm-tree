namespace LsmTree

open System.IO
open System.Collections.Generic
open System.Threading.Tasks
open System.Threading

type ITransaction =
    abstract member Put: key: string * value: string -> unit
    abstract member Delete: key: string -> unit
    abstract member Get: key: string -> string option
    abstract member Commit: unit -> unit
    abstract member Rollback: unit -> unit

type LsmTree(dataDir: string, ?memTableSizeLimit: int) =
    let memTableLimit = defaultArg memTableSizeLimit (1024 * 1024)
    let walPath = Path.Combine(dataDir, "wal.log")
    let mutable memTable = MemTable()
    let mutable immutableMemTable: MemTable option = None
    let mainLock = new ReaderWriterLockSlim()

    let compactLevelLimits = [| 4; 10; 100; 1000 |]
    let ssTables = Array.init (compactLevelLimits.Length + 1) (fun _ -> List<SSTable>())
    let ssTablesLock = obj ()

    let mutable globalSeq = 0L

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
                ssTables.[level].Add(SSTable path))

        ssTables
        |> Array.iter (fun ssts ->
            let sorted = ssts |> Seq.sortByDescending (fun t -> t.Path)
            ssts.Clear()
            ssts.AddRange sorted)

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
    let mutable wal = WAL walPath

    let timestamp () =
        System.DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()

    let newGuid () = System.Guid.NewGuid().ToString "N"

    let ssTablePath level =
        Path.Combine(dataDir, sprintf "L%d_%d_%s.sst" level (timestamp ()) (newGuid ()))

    let mergeSSTables level (tablesToCompact: SSTable list) =
        // To support MVCC perfectly, we don't drop shadow versions unless we do GC against lowest active snapshot.
        // For now, we write out ALL versions, grouping by key.
        let mergedData = Dictionary<string, List<int64 * string option>>()

        tablesToCompact
        |> List.rev
        |> Seq.collect (fun t -> t.GetAll())
        |> Seq.iter (fun (key, seq, value) ->
            if not (mergedData.ContainsKey key) then
                mergedData.[key] <- List<int64 * string option>()

            mergedData.[key].Add((seq, value)))

        let finalEntries =
            mergedData
            |> Seq.collect (fun kv -> kv.Value |> Seq.map (fun (s, v) -> kv.Key, s, v))
            |> Seq.sortWith (fun (k1, s1, _) (k2, s2, _) ->
                let c = System.String.CompareOrdinal(k1, k2)
                if c <> 0 then c else s2.CompareTo s1)
            |> Seq.toList

        SSTableWriter.flush finalEntries (ssTablePath (level + 1))

    let rec compact level =
        let tablesToCompact =
            lock ssTablesLock (fun () ->
                if
                    level < ssTables.Length - 1
                    && ssTables.[level].Count > compactLevelLimits.[level]
                then
                    ssTables.[level] |> Seq.toList
                else
                    [])

        if tablesToCompact.Length > 0 then
            let newSSTable = mergeSSTables level tablesToCompact

            lock ssTablesLock (fun () ->
                ssTables.[level + 1].Insert(0, newSSTable)

                let remaining =
                    ssTables.[level]
                    |> Seq.filter (fun t -> not (List.contains t tablesToCompact))
                    |> Seq.toList

                ssTables.[level].Clear()
                ssTables.[level].AddRange remaining)

            tablesToCompact
            |> Seq.iter (fun t ->
                try
                    File.Delete t.Path
                with _ ->
                    ())

            compact (level + 1)

    let mutable isCompacting = false

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
                wal <- WAL walPath
                immutableMemTable <- Some oldMemTable
                Some(oldMemTable, oldWalPath)
            else
                None
        finally
            mainLock.ExitWriteLock()

    let addSSTable (oldMemTable: MemTable) =
        let sst = SSTableWriter.flush oldMemTable.Entries (ssTablePath 0)
        lock ssTablesLock (fun () -> ssTables.[0].Insert(0, sst))

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

    member _.Snapshot() = Interlocked.Read(&globalSeq)

    member this.BeginTransaction() =
        new LsmTransaction(this, this.Snapshot()) :> ITransaction

    member this.Put(key: string, value: string) =
        let tx = this.BeginTransaction()
        tx.Put(key, value)
        tx.Commit()

    member this.Delete(key: string) =
        let tx = this.BeginTransaction()
        tx.Delete key
        tx.Commit()

    member this.Get(key: string, ?snapshot: int64) =
        let snap = defaultArg snapshot (this.Snapshot())

        let rec search tbls =
            match tbls with
            | [] -> None
            | t: SSTable :: rest ->
                match t.Get(key, snap) with
                | Some res -> Some res
                | None -> search rest

        let rec searchLevel level =
            if level >= ssTables.Length then
                None
            else
                let tables = lock ssTablesLock (fun () -> ssTables.[level] |> Seq.toList)

                match search tables with
                | Some res -> Some res
                | None -> searchLevel (level + 1)

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
                match searchLevel 0 with
                | Some(Some v) -> Some v
                | _ -> None

    member _.Flush() = flushMemTable ()

    member _.WaitForCompaction() =
        let rec wait () =
            let active = lock ssTablesLock (fun () -> isCompacting)

            if active then
                Thread.Sleep 50
                wait ()

        wait ()

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

                    wal.Commit commitSeq

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
            failwith "Transaction already finished"

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

        member _.Commit() =
            checkFinished ()
            lsm.CommitTransaction(ops |> Seq.rev |> Seq.toList)
            finished <- true

        member _.Rollback() =
            checkFinished ()
            ops <- []
            finished <- true
